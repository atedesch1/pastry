use std::{sync::Arc, time::Duration};

use log::{info, warn};
use tokio::sync::{mpsc::Sender, Notify, RwLock};
use tonic::transport::{Channel, Server};

use crate::{
    error::Result,
    hring::hasher::Sha256Hasher,
    pastry::{leaf::LeafSet, table::RoutingTable},
    rpc::node::{
        node_service_client::NodeServiceClient, node_service_server::NodeServiceServer,
        JoinRequest, UpdateNeighborsRequest,
    },
};

#[derive(Debug, Clone)]
pub struct NodeInfo {
    pub id: u64,
    pub pub_addr: String,
}

const MAX_CONNECT_RETRIES: usize = 5;
const CONNECT_TIMEOUT_SECONDS: u64 = 1;

#[derive(Debug, Clone)]
pub struct NodeConnection {
    pub info: NodeInfo,
    pub client: Option<NodeServiceClient<Channel>>,
}

impl NodeConnection {
    fn new(id: u64, pub_addr: &str, client: Option<NodeServiceClient<Channel>>) -> Self {
        NodeConnection {
            info: NodeInfo {
                id,
                pub_addr: pub_addr.to_owned(),
            },
            client,
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum NodeState {
    Unitialized,
    Initializing,
    UpdatingConnections,
    RoutingRequests,
}

#[derive(Debug)]
pub struct State {
    pub state: RwLock<NodeState>,
    pub changed: Notify,
}

#[derive(Debug)]
pub struct Node {
    pub id: u64,
    pub addr: String,
    pub pub_addr: String,

    pub state: Arc<State>,

    pub leaf: Arc<RwLock<LeafSet<NodeConnection>>>,
    pub table: Arc<RwLock<RoutingTable<NodeInfo>>>,
}

/// Configuration for the Pastry network
#[derive(Debug, Clone)]
pub struct PastryConfig {
    pub leaf_set_k: usize,
}

impl Node {
    /// Registers a new DHT node which will be available publicly on
    /// http://hostname:port
    ///
    /// # Arguments
    ///
    /// * `config` - The Pastry network configuration.
    /// * `hostname` - The Hostname to serve this node on.
    /// * `port` - The port to serve this node on.
    ///
    /// # Returns
    ///
    /// A Result containing the newly registered node.
    ///
    pub fn new(config: PastryConfig, hostname: &str, port: &str) -> Result<Self> {
        let pub_addr = format!("http://{}:{}", hostname, port);
        let addr = format!("0.0.0.0:{}", port);

        let id = Sha256Hasher::hash_once(pub_addr.as_bytes());
        info!("#{:x}: Registered node", id);

        Ok(Node {
            id,
            addr,
            pub_addr: pub_addr.clone(),

            state: Arc::new(State {
                state: RwLock::new(NodeState::Unitialized),
                changed: Notify::new(),
            }),

            leaf: Arc::new(RwLock::new(LeafSet::new(
                config.leaf_set_k,
                id,
                NodeConnection::new(id, &pub_addr, None),
            )?)),
            table: Arc::new(RwLock::new(RoutingTable::new(id))),
        })
    }

    /// Connects to network via bootstrap node and serves node server.
    /// Consumes node.
    ///
    /// # Arguments
    ///
    /// * `bootstrap_addr` - A bootstrap node address.
    ///
    /// # Returns
    ///
    /// An empty Result.
    ///
    pub async fn bootstrap_and_serve(
        self,
        bootstrap_addr: Option<&str>,
        connected_tx: Option<Sender<bool>>,
    ) -> Result<()> {
        info!("#{:x}: Initializing node on {}", self.id, self.pub_addr);

        if let Some(bootstrap_addr) = bootstrap_addr {
            tokio::spawn(Self::connect_to_network(
                self.id,
                self.pub_addr.clone(),
                self.state.clone(),
                self.leaf.clone(),
                self.table.clone(),
                bootstrap_addr.to_string(),
                connected_tx,
            ));
        } else {
            info!("#{:x}: Initializing network", self.id);
            {
                let mut state = self.state.state.write().await;
                *state = NodeState::RoutingRequests;
                self.state.changed.notify_waiters();
            }
            info!("#{:x}: Connected to network", self.id);
            if let Some(tx) = connected_tx {
                tx.send(true).await?;
                tx.closed().await;
            }
        }

        let addr = self.addr.clone();
        Ok(Server::builder()
            .add_service(NodeServiceServer::new(self))
            .serve(addr.parse()?)
            .await?)
    }

    /// Connects to bootstrap node.
    async fn connect_to_network(
        id: u64,
        pub_addr: String,
        state: Arc<State>,
        leaf: Arc<RwLock<LeafSet<NodeConnection>>>,
        table: Arc<RwLock<RoutingTable<NodeInfo>>>,
        bootstrap_addr: String,
        connected_tx: Option<Sender<bool>>,
    ) -> Result<()> {
        let mut st = state.state.write().await;
        *st = NodeState::Initializing;

        info!("#{:x}: Connecting to network", id);

        let mut client = Node::connect_with_retry(&bootstrap_addr).await?;
        let join_response = client
            .join(JoinRequest {
                id,
                pub_addr: pub_addr.clone(),
            })
            .await?
            .into_inner();

        // Update leaf set
        info!("#{:x}: Updating leaf set", id);
        let mut leaf = leaf.write().await;
        for entry in join_response.leaf_set {
            let mut client = Node::connect_with_retry(&entry.pub_addr).await?;
            client
                .update_neighbors(UpdateNeighborsRequest {
                    id,
                    pub_addr: pub_addr.clone(),
                })
                .await?;
            leaf.insert(
                entry.id,
                NodeConnection::new(entry.id, &entry.pub_addr, Some(client)),
            )?;
        }
        info!("#{:x}: Updated leaf set", id);
        *st = NodeState::RoutingRequests;
        state.changed.notify_waiters();
        info!("#{:x}: Connected to network", id);

        if let Some(tx) = connected_tx {
            tx.send(true).await?;
            tx.closed().await;
        }

        Ok(())
    }

    /// Changes the Node state and notifies waiters
    pub async fn change_state(&self, next_state: NodeState) {
        let mut state = self.state.state.write().await;
        *state = next_state;
        self.state.changed.notify_waiters();
    }

    /// Blocks thread and yields back execution until state is RoutingRequests
    pub async fn block_until_routing_requests(&self) -> () {
        while *self.state.state.read().await != NodeState::RoutingRequests {
            self.state.changed.notified().await;
        }
    }

    /// Attempts to repeatedly connect to a node and returns a Result containing the client
    pub async fn connect_with_retry(addr: &str) -> Result<NodeServiceClient<Channel>> {
        let mut retries = 0;

        loop {
            match NodeServiceClient::connect(addr.to_owned()).await {
                Ok(client) => return Ok(client),
                Err(err) => {
                    retries += 1;

                    if retries >= MAX_CONNECT_RETRIES {
                        return Err(err.into());
                    }

                    warn!(
                        "Connection failed. Retrying in {} seconds...",
                        CONNECT_TIMEOUT_SECONDS
                    );
                    tokio::time::sleep(Duration::from_secs(CONNECT_TIMEOUT_SECONDS)).await;
                }
            }
        }
    }
}
