use std::sync::Arc;

use log::info;
use tokio::sync::{Notify, RwLock};
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
pub struct Node {
    pub id: u64,
    pub addr: String,
    pub pub_addr: String,

    pub state: Arc<RwLock<NodeState>>,
    pub state_changed: Arc<Notify>,

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

            state: Arc::new(RwLock::new(NodeState::Unitialized)),
            state_changed: Arc::new(Notify::new()),

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
    pub async fn bootstrap_and_serve(self, bootstrap_addr: Option<&str>) -> Result<()> {
        info!("#{:x}: Initializing node on {}", self.id, self.pub_addr);

        if let Some(bootstrap_addr) = bootstrap_addr {
            tokio::spawn(Self::connect_to_network(
                self.id,
                self.pub_addr.clone(),
                self.state.clone(),
                self.state_changed.clone(),
                self.leaf.clone(),
                self.table.clone(),
                bootstrap_addr.to_string(),
            ));
        } else {
            info!("#{:x}: Initializing network", self.id);
            {
                let mut state = self.state.write().await;
                *state = NodeState::RoutingRequests;
                self.state_changed.notify_waiters();
            }
            info!("#{:x}: Connected to network", self.id);
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
        state: Arc<RwLock<NodeState>>,
        state_changed: Arc<Notify>,
        leaf: Arc<RwLock<LeafSet<NodeConnection>>>,
        table: Arc<RwLock<RoutingTable<NodeInfo>>>,
        bootstrap_addr: String,
    ) -> Result<()> {
        let mut state = state.write().await;
        *state = NodeState::Initializing;

        info!("#{:x}: Connecting to network", id);

        let mut client = NodeServiceClient::connect(bootstrap_addr.to_owned()).await?;
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
            let mut client = NodeServiceClient::connect(entry.pub_addr.clone()).await?;
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
        *state = NodeState::RoutingRequests;
        state_changed.notify_waiters();
        info!("#{:x}: Connected to network", id);

        Ok(())
    }

    // Changes the Node state and notifies waiters
    pub async fn change_state(&self, next_state: NodeState) {
        let mut state = self.state.write().await;
        *state = next_state;
        self.state_changed.notify_waiters();
    }

    // Blocks thread and yields back execution until state is RoutingRequests
    pub async fn block_until_routing_requests(&self) -> () {
        while *self.state.read().await != NodeState::RoutingRequests {
            self.state_changed.notified().await;
        }
    }
}
