use std::{net::SocketAddr, sync::Arc, time::Duration};

use log::{debug, info, warn};
use tokio::{
    sync::{Notify, RwLock, RwLockWriteGuard},
    task::JoinHandle,
};
use tonic::transport::{Channel, Server};

use crate::{
    error::{Error, Result},
    hring::hasher::Sha256Hasher,
    pastry::{leaf::LeafSet, table::RoutingTable},
    rpc::node::{
        node_service_client::NodeServiceClient, node_service_server::NodeServiceServer,
        AnnounceArrivalRequest, JoinRequest, NodeEntry,
    },
};

#[derive(Debug, Clone)]
pub struct NodeInfo {
    pub id: u64,
    pub pub_addr: String,
}

impl NodeInfo {
    pub fn new(id: u64, pub_addr: &str) -> Self {
        NodeInfo {
            id,
            pub_addr: pub_addr.to_owned(),
        }
    }

    pub fn from_node_entry(entry: &NodeEntry) -> Self {
        Self::new(entry.id, &entry.pub_addr)
    }

    pub fn to_node_entry(self) -> NodeEntry {
        NodeEntry {
            id: self.id,
            pub_addr: self.pub_addr,
        }
    }
}

const MAX_CONNECT_RETRIES: usize = 10;
const CONNECT_TIMEOUT_SECONDS: u64 = 1;

#[derive(Debug, PartialEq)]
pub enum NodeState {
    Unitialized,
    Initializing,
    UpdatingConnections,
    RoutingRequests,
}

#[derive(Debug)]
pub struct State {
    pub name: RwLock<NodeState>,
    pub notify: Notify,
    pub data: RwLock<StateData>,
}

#[derive(Debug)]
pub struct StateData {
    pub leaf: LeafSet<NodeInfo>,
    pub table: RoutingTable<NodeInfo>,
}

#[derive(Debug, Clone)]
pub struct Node {
    pub id: u64,
    pub addr: String,
    pub pub_addr: String,

    pub state: Arc<State>,
}

/// Configuration for the Pastry network
#[derive(Debug, Clone)]
pub struct PastryConfig {
    pub leaf_set_k: usize,
}

impl Node {
    /// Gets node's id and public address and returns a NodeInfo
    ///
    /// # Returns
    ///
    /// A NodeInfo.
    ///
    pub fn get_info(&self) -> NodeInfo {
        NodeInfo::new(self.id, &self.pub_addr)
    }

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
        info!("#{:X}: Registered node", id);

        Ok(Node {
            id,
            addr,
            pub_addr: pub_addr.clone(),

            state: Arc::new(State {
                name: RwLock::new(NodeState::Unitialized),
                notify: Notify::new(),
                data: RwLock::new(StateData {
                    leaf: LeafSet::new(config.leaf_set_k, id, NodeInfo::new(id, &pub_addr))?,
                    table: RoutingTable::new(id),
                }),
            }),
        })
    }

    /// Registers a new DHT node which will be available publicly on
    /// http://hostname:port
    ///
    /// # Arguments
    ///
    /// * `config` - The Pastry network configuration.
    /// * `hostname` - The Hostname to serve this node on.
    /// * `port` - The port to serve this node on.
    /// * `id` - The node's id.
    ///
    /// # Returns
    ///
    /// A Result containing the newly registered node.
    ///
    pub fn from_id(config: PastryConfig, hostname: &str, port: &str, id: u64) -> Result<Self> {
        let pub_addr = format!("http://{}:{}", hostname, port);
        let addr = format!("0.0.0.0:{}", port);

        info!("#{:X}: Registered node", id);

        Ok(Node {
            id,
            addr,
            pub_addr: pub_addr.clone(),

            state: Arc::new(State {
                name: RwLock::new(NodeState::Unitialized),
                notify: Notify::new(),
                data: RwLock::new(StateData {
                    leaf: LeafSet::new(config.leaf_set_k, id, NodeInfo::new(id, &pub_addr))?,
                    table: RoutingTable::new(id),
                }),
            }),
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
    /// A Result containing the JoinHandle for the server.
    ///
    pub async fn bootstrap_and_serve(
        self,
        bootstrap_addr: Option<&str>,
    ) -> Result<JoinHandle<Result<()>>> {
        info!("#{:X}: Initializing node on {}", self.id, self.pub_addr);
        self.change_state(NodeState::Initializing).await;
        let server_handle = self.initialize_server().await?;

        if let Some(bootstrap_addr) = bootstrap_addr {
            self.connect_to_network(bootstrap_addr).await?;
        } else {
            info!("#{:X}: Initializing network", self.id);
        }

        self.change_state(NodeState::RoutingRequests).await;
        info!("#{:X}: Connected to network", self.id);

        Ok(server_handle)
    }

    /// Serves node server.
    ///
    /// # Returns
    ///
    /// A Result containing the JoinHandle for the server.
    ///
    pub async fn serve(&self) -> Result<JoinHandle<Result<()>>> {
        info!("#{:X}: Initializing node on {}", self.id, self.pub_addr);
        self.change_state(NodeState::Initializing).await;
        let server_handle = self.initialize_server().await?;

        Ok(server_handle)
    }

    /// Updates state and consumes node.
    ///
    /// # Arguments
    ///
    /// * `leaf` - A vector containing the nodes of its leaf set.
    /// * `table` - A vector containing the nodes of its routing table.
    ///
    /// # Returns
    ///
    /// An empty Result.
    ///
    pub async fn route_with_state(self, leaf: Vec<NodeInfo>, table: Vec<NodeInfo>) -> Result<()> {
        info!("#{:X}: Updating connections", self.id);
        self.change_state(NodeState::UpdatingConnections).await;

        let mut state_data = self.state.data.write().await;
        let table_entries: Vec<NodeEntry> = table.into_iter().map(|e| e.to_node_entry()).collect();
        self.update_routing_table(&mut state_data, &table_entries)
            .await?;

        let leaf_entries: Vec<NodeEntry> = leaf.into_iter().map(|e| e.to_node_entry()).collect();
        self.update_leaf_set(&mut state_data, &leaf_entries).await?;

        self.change_state(NodeState::RoutingRequests).await;
        info!("#{:X}: Connected to network", self.id);
        Ok(())
    }

    /// Initializes gRPC server
    async fn initialize_server(&self) -> Result<JoinHandle<Result<()>>> {
        let addr: SocketAddr = self.addr.clone().parse()?;
        let incoming = tonic::transport::server::TcpIncoming::new(addr, true, None)?;

        let node = self.clone();
        Ok(tokio::spawn(async move {
            Server::builder()
                .add_service(NodeServiceServer::new(node))
                .serve_with_incoming(incoming)
                .await
                .map_err(|e| Error::from(e))
        }))
    }

    /// Connects to bootstrap node.
    async fn connect_to_network(&self, bootstrap_addr: &str) -> Result<()> {
        info!("#{:X}: Connecting to network", self.id);

        let mut state_data = self.state.data.write().await;

        let mut client = Node::connect_with_retry(bootstrap_addr).await?;
        let join_response = client
            .join(JoinRequest {
                id: self.id,
                pub_addr: self.pub_addr.clone(),
                matched_digits: 0,
                routing_table: Vec::new(),
            })
            .await?
            .into_inner();

        self.update_routing_table(&mut state_data, &join_response.routing_table)
            .await?;

        self.update_leaf_set(&mut state_data, &join_response.leaf_set)
            .await?;

        self.announce_arrival(&mut state_data).await?;

        Ok(())
    }

    pub async fn update_leaf_set<'a, T>(
        &self,
        state_data: &mut RwLockWriteGuard<'_, StateData>,
        entries: T,
    ) -> Result<()>
    where
        T: IntoIterator<Item = &'a NodeEntry>,
    {
        info!("#{:X}: Updating leaf set", self.id);
        for entry in entries.into_iter() {
            state_data
                .leaf
                .insert(entry.id, NodeInfo::from_node_entry(entry))?;
        }
        info!("#{:X}: Updated leaf set", self.id);
        debug!("#{:X}: Leaf set updated: \n{}", self.id, state_data.leaf);

        Ok(())
    }

    pub async fn update_routing_table<'a, T>(
        &self,
        state_data: &mut RwLockWriteGuard<'_, StateData>,
        entries: T,
    ) -> Result<()>
    where
        T: IntoIterator<Item = &'a NodeEntry>,
    {
        info!("#{:X}: Updating routing table", self.id);
        for entry in entries.into_iter() {
            state_data
                .table
                .insert(entry.id, NodeInfo::from_node_entry(entry))?;
        }
        info!("#{:X}: Updated routing table", self.id);
        debug!(
            "#{:X}: Routing table updated: \n{}",
            self.id, state_data.table
        );
        Ok(())
    }

    async fn announce_arrival(
        &self,
        state_data: &mut RwLockWriteGuard<'_, StateData>,
    ) -> Result<()> {
        info!("#{:X}: Announcing arrival to all neighbors", self.id);
        let announce_arrival_request = AnnounceArrivalRequest {
            id: self.id,
            pub_addr: self.pub_addr.clone(),
        };

        for entry in state_data.leaf.get_entries() {
            if entry.value.id != self.id {
                let mut client = Node::connect_with_retry(&entry.value.pub_addr).await?;
                client
                    .announce_arrival(announce_arrival_request.clone())
                    .await?;
            }
        }

        for entry in state_data.table.get_entries() {
            if let Some(entry) = entry {
                let mut client = Node::connect_with_retry(&entry.value.pub_addr).await?;
                client
                    .announce_arrival(announce_arrival_request.clone())
                    .await?;
            }
        }
        info!("#{:X}: Announced arrival to all neighbors", self.id);

        Ok(())
    }

    /// Changes the Node state and notifies waiters
    pub async fn change_state(&self, next_state: NodeState) {
        let mut state = self.state.name.write().await;
        *state = next_state;
        self.state.notify.notify_waiters();
    }

    /// Blocks thread and yields back execution until state is RoutingRequests
    pub async fn block_until_routing_requests(&self) -> () {
        while *self.state.name.read().await != NodeState::RoutingRequests {
            self.state.notify.notified().await;
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
