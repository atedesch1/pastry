use log::{debug, info, warn};
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::{
    sync::{Notify, RwLock, RwLockWriteGuard},
    task::JoinHandle,
};
use tonic::transport::{Channel, Server};

use super::service::grpc::*;
use super::store::Store;

use crate::{
    error::*,
    internal::{
        hring::hasher::Sha256Hasher,
        pastry::{leaf::LeafSet, shared::Config, table::RoutingTable},
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

#[derive(Debug, PartialEq)]
pub enum NodeState {
    Uninitialized,
    Initializing,
    UpdatingConnections,
    RoutingRequests,
}

#[derive(Debug)]
pub struct State {
    pub name: RwLock<NodeState>,
    pub notify: Notify,
    pub data: RwLock<StateData>,
    pub store: RwLock<Store>,
}

#[derive(Debug)]
pub struct StateData {
    pub leaf: LeafSet<NodeInfo>,
    pub table: RoutingTable<NodeInfo>,
}

#[derive(Debug, Clone)]
pub struct Node {
    pub id: u64,
    pub addr: SocketAddr,
    pub pub_addr: String,

    pub state: Arc<State>,
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
    /// * `addr` - The address of the socket to listen on.
    /// * `pub_addr` - The address the node will be exposed on.
    ///
    /// # Returns
    ///
    /// A Result containing the newly registered node.
    ///
    pub fn new(config: Config, addr: SocketAddr, pub_addr: SocketAddr) -> Result<Self> {
        let pub_addr = format!("http://{}:{}", pub_addr.ip(), pub_addr.port());
        let id = Sha256Hasher::hash_once(pub_addr.as_bytes());

        info!("#{:016X}: Registered node", id);

        let info = NodeInfo::new(id, &pub_addr);

        Ok(Node {
            id,
            addr,
            pub_addr: pub_addr.clone(),

            state: Arc::new(State {
                name: RwLock::new(NodeState::Uninitialized),
                notify: Notify::new(),
                data: RwLock::new(StateData {
                    leaf: LeafSet::new(config.k, id, info.clone())?,
                    table: RoutingTable::new(id, info),
                }),
                store: RwLock::new(Store::new()),
            }),
        })
    }

    /// Registers a new DHT node which will be available publicly on
    /// http://hostname:port
    ///
    /// # Arguments
    ///
    /// * `config` - The Pastry network configuration.
    /// * `addr` - The address of the socket to listen on.
    /// * `pub_addr` - The address the node will be exposed on.
    /// * `id` - The node's id.
    ///
    /// # Returns
    ///
    /// A Result containing the newly registered node.
    ///
    pub fn from_id(
        config: Config,
        addr: SocketAddr,
        pub_addr: SocketAddr,
        id: u64,
    ) -> Result<Self> {
        let pub_addr = format!("http://{}:{}", pub_addr.ip(), pub_addr.port());

        info!("#{:016X}: Registered node", id);

        let info = NodeInfo::new(id, &pub_addr);

        Ok(Node {
            id,
            addr,
            pub_addr: pub_addr.clone(),

            state: Arc::new(State {
                name: RwLock::new(NodeState::Uninitialized),
                notify: Notify::new(),
                data: RwLock::new(StateData {
                    leaf: LeafSet::new(config.k, id, info.clone())?,
                    table: RoutingTable::new(id, info),
                }),
                store: RwLock::new(Store::new()),
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
        info!("#{:016X}: Initializing node on {}", self.id, self.pub_addr);
        self.change_state(NodeState::Initializing).await;
        let server_handle = self.initialize_server().await?;

        if let Some(bootstrap_addr) = bootstrap_addr {
            self.connect_to_network(bootstrap_addr).await?;
        } else {
            info!("#{:016X}: Initializing network", self.id);
        }

        self.change_state(NodeState::RoutingRequests).await;
        info!("#{:016X}: Connected to network", self.id);

        Ok(server_handle)
    }

    /// Serves node server.
    ///
    /// # Returns
    ///
    /// A Result containing the JoinHandle for the server.
    ///
    pub async fn serve(&self) -> Result<JoinHandle<Result<()>>> {
        info!("#{:016X}: Initializing node on {}", self.id, self.pub_addr);
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
        info!("#{:016X}: Updating connections", self.id);
        self.change_state(NodeState::UpdatingConnections).await;

        let mut state_data = self.state.data.write().await;
        let table_entries: Vec<NodeEntry> = table.into_iter().map(|e| e.to_node_entry()).collect();
        self.update_routing_table(&mut state_data, &table_entries)
            .await?;

        let leaf_entries: Vec<NodeEntry> = leaf.into_iter().map(|e| e.to_node_entry()).collect();
        self.update_leaf_set(&mut state_data, &leaf_entries).await?;

        self.change_state(NodeState::RoutingRequests).await;
        info!("#{:016X}: Connected to network", self.id);
        Ok(())
    }

    /// Initializes gRPC server
    async fn initialize_server(&self) -> Result<JoinHandle<Result<()>>> {
        let incoming = tonic::transport::server::TcpIncoming::new(self.addr, true, None)?;

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
        info!("#{:016X}: Connecting to network", self.id);

        let mut data = self.state.data.write().await;

        let mut client = Node::connect_with_retry(bootstrap_addr).await?;
        let join_response = client
            .join(JoinRequest {
                id: self.id,
                pub_addr: self.pub_addr.clone(),
                hops: 0,
                matched_digits: 0,
                routing_table: Vec::new(),
            })
            .await?
            .into_inner();

        {
            let mut client = Node::connect_with_retry(&join_response.pub_addr).await?;
            let mut stream = client
                .transfer_keys(TransferKeysRequest { id: self.id })
                .await?
                .into_inner();
            let mut store = self.state.store.write().await;

            while let Some(entry) = stream.message().await? {
                store.set(&entry.key, &entry.value);
            }
        }

        self.update_routing_table(&mut data, &join_response.routing_table)
            .await?;

        self.update_leaf_set(&mut data, &join_response.leaf_set)
            .await?;

        self.announce_arrival_to_neighbors(&mut data).await?;

        Ok(())
    }

    pub async fn route_with_leaf_set(&self, key: u64) -> Option<NodeInfo> {
        self.state.data.read().await.leaf.get(key).cloned()
    }

    pub async fn get_closest_from_leaf_set(&self, key: u64) -> (NodeInfo, usize) {
        self.state
            .data
            .read()
            .await
            .leaf
            .get_closest(key)
            .map(|e| (e.0.clone(), e.1))
            .unwrap()
    }

    pub async fn route_with_routing_table(
        &self,
        key: u64,
        min_matched_digits: usize,
    ) -> Option<(NodeInfo, usize)> {
        self.state
            .data
            .read()
            .await
            .table
            .route(key, min_matched_digits)
            .map(|e| e.map(|f| (f.0.clone(), f.1)))
            .unwrap()
    }

    pub async fn update_leaf_set<'a, T>(
        &self,
        state_data: &mut RwLockWriteGuard<'_, StateData>,
        entries: T,
    ) -> Result<()>
    where
        T: IntoIterator<Item = &'a NodeEntry>,
    {
        info!("#{:016X}: Updating leaf set", self.id);
        for entry in entries.into_iter() {
            state_data
                .leaf
                .insert(entry.id, NodeInfo::from_node_entry(entry))?;
        }
        info!("#{:016X}: Updated leaf set", self.id);
        debug!("#{:016X}: Leaf set updated: \n{}", self.id, state_data.leaf);

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
        info!("#{:016X}: Updating routing table", self.id);
        for entry in entries.into_iter() {
            state_data
                .table
                .insert(entry.id, NodeInfo::from_node_entry(entry))?;
        }
        info!("#{:016X}: Updated routing table", self.id);
        debug!(
            "#{:016X}: Routing table updated: \n{}",
            self.id, state_data.table
        );
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

    const MAX_CONNECT_RETRIES: usize = 10;
    const CONNECT_TIMEOUT_SECONDS: u64 = 1;

    /// Attempts to repeatedly connect to a node and returns a Result containing the client
    pub async fn connect_with_retry(addr: &str) -> Result<NodeServiceClient<Channel>> {
        let mut retries = 0;

        loop {
            match NodeServiceClient::connect(addr.to_owned()).await {
                Ok(client) => return Ok(client),
                Err(err) => {
                    retries += 1;

                    if retries >= Self::MAX_CONNECT_RETRIES {
                        return Err(err.into());
                    }

                    warn!(
                        "Connection failed. Retrying in {} seconds...",
                        Self::CONNECT_TIMEOUT_SECONDS
                    );
                    tokio::time::sleep(Duration::from_secs(Self::CONNECT_TIMEOUT_SECONDS)).await;
                }
            }
        }
    }
}
