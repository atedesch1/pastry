use log::{debug, info, warn};
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::{
    sync::{Notify, RwLock, RwLockWriteGuard},
    task::JoinHandle,
};
use tonic::transport::{Channel, Server};

use super::{grpc::*, store::Store};

use crate::{
    error::*,
    internal::{
        hring::hasher::Sha256Hasher,
        pastry::{leaf::LeafSet, shared::Config, table::RoutingTable},
        util::{self, U64_HEX_NUM_OF_DIGITS},
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
    pub addr: String,
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
    /// * `hostname` - The Hostname to serve this node on.
    /// * `port` - The port to serve this node on.
    ///
    /// # Returns
    ///
    /// A Result containing the newly registered node.
    ///
    pub fn new(config: Config, hostname: &str, port: &str) -> Result<Self> {
        let pub_addr = format!("http://{}:{}", hostname, port);
        let addr = format!("0.0.0.0:{}", port);

        let id = Sha256Hasher::hash_once(pub_addr.as_bytes());
        info!("#{:016X}: Registered node", id);

        let info = NodeInfo::new(id, &pub_addr);

        Ok(Node {
            id,
            addr,
            pub_addr: pub_addr.clone(),

            state: Arc::new(State {
                name: RwLock::new(NodeState::Unitialized),
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
    /// * `hostname` - The Hostname to serve this node on.
    /// * `port` - The port to serve this node on.
    /// * `id` - The node's id.
    ///
    /// # Returns
    ///
    /// A Result containing the newly registered node.
    ///
    pub fn from_id(config: Config, hostname: &str, port: &str, id: u64) -> Result<Self> {
        let pub_addr = format!("http://{}:{}", hostname, port);
        let addr = format!("0.0.0.0:{}", port);

        info!("#{:016X}: Registered node", id);

        let info = NodeInfo::new(id, &pub_addr);

        Ok(Node {
            id,
            addr,
            pub_addr: pub_addr.clone(),

            state: Arc::new(State {
                name: RwLock::new(NodeState::Unitialized),
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
        info!("#{:016X}: Connecting to network", self.id);

        let mut data = self.state.data.write().await;

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

        self.announce_arrival(&mut data).await?;

        Ok(())
    }

    /// Executes query request.
    pub async fn execute_query(&self, query: &QueryRequest) -> Result<Option<Vec<u8>>> {
        let key = &query.key;
        let value = &query.value;
        let query_type = query.query_type;

        info!(
            "#{:016X}: Executing query for key {:016X}",
            self.id, query.key
        );

        match QueryType::from_i32(query_type).unwrap() {
            QueryType::Set => match value {
                None => Err(Error::Value("Value not provided".into())),
                Some(value) => Ok(self.state.store.write().await.set(key, value)),
            },
            QueryType::Get => match self.state.store.read().await.get(key) {
                None => Err(Error::Value("Key not present in database".into())),
                Some(value) => Ok(Some(value.clone())),
            },
            QueryType::Delete => match self.state.store.write().await.delete(key) {
                None => Err(Error::Value("Key not present in database.".into())),
                Some(value) => Ok(Some(value)),
            },
        }
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

    async fn announce_arrival(
        &self,
        state_data: &mut RwLockWriteGuard<'_, StateData>,
    ) -> Result<()> {
        info!("#{:016X}: Announcing arrival to all neighbors", self.id);
        let announce_arrival_request = AnnounceArrivalRequest {
            id: self.id,
            pub_addr: self.pub_addr.clone(),
        };

        for entry in state_data.leaf.get_entries() {
            if entry.id == self.id {
                continue;
            }

            let mut client = Node::connect_with_retry(&entry.pub_addr).await?;
            client
                .announce_arrival(announce_arrival_request.clone())
                .await?;
        }

        for entry in state_data.table.get_entries() {
            if let Some(entry) = entry {
                if entry.id == self.id {
                    continue;
                }

                let mut client = Node::connect_with_retry(&entry.pub_addr).await?;
                client
                    .announce_arrival(announce_arrival_request.clone())
                    .await?;
            }
        }
        info!("#{:016X}: Announced arrival to all neighbors", self.id);

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

    /// Attempts to fix a leaf set entry
    pub async fn fix_leaf_entry(&self, node: &NodeInfo) -> Result<()> {
        info!("#{:016X}: Fixing leaf set", self.id);
        self.change_state(NodeState::UpdatingConnections).await;

        let mut data = self.state.data.write().await;

        if !data.leaf.is_full() {
            // remove failed entry
            data.leaf.remove(node.id).unwrap();

            // there are not enough nodes to replace entry

            self.change_state(NodeState::RoutingRequests).await;
            return Ok(());
        }

        match data.leaf.is_clockwise_neighbor(node.id) {
            Err(_) => {}
            Ok(is_clockwise_neighbor) => {
                // iterator without failed node
                let forward_iterator = data.leaf.clone().into_iter().filter(|e| e.id != node.id);

                // remove failed leaf entry
                data.leaf.remove(node.id).unwrap();

                // yield only the ones on the same side as the failed node
                let nodes_on_the_same_side: Vec<NodeInfo> = if !is_clockwise_neighbor {
                    forward_iterator.take_while(|e| e.id != self.id).collect()
                } else {
                    forward_iterator
                        .rev()
                        .take_while(|e| e.id != self.id)
                        .collect()
                };

                for neighbor in &nodes_on_the_same_side {
                    // check if node is alive
                    let mut client =
                        match NodeServiceClient::connect(neighbor.pub_addr.to_owned()).await {
                            Ok(client) => client,
                            Err(err) => {
                                warn!(
                                    "#{:016X}: Connection to #{:016X} failed: {}",
                                    self.id, neighbor.id, err
                                );
                                continue;
                            }
                        };
                    let state = client.get_node_state(()).await?.into_inner();

                    // replace entry
                    for entry in state.leaf_set {
                        if entry.id == neighbor.id || entry.id == node.id {
                            continue;
                        }

                        // check if entry is alive
                        if let Err(err) =
                            NodeServiceClient::connect(entry.pub_addr.to_owned()).await
                        {
                            warn!(
                                "#{:016X}: Connection to #{:016X} failed: {}",
                                self.id, entry.id, err
                            );
                            continue;
                        }

                        data.leaf
                            .insert(entry.id, NodeInfo::from_node_entry(&entry))?;
                    }

                    // break if already fixed leaf set
                    if data.leaf.is_full() {
                        break;
                    }
                }

                if !data.leaf.is_full() {
                    // unable to fix leaf set
                    panic!(
                        "#{:016X}: Could not fix leaf set. Too many failed nodes.",
                        self.id,
                    );
                }

                debug!("#{:016X}: Fixed leaf set: \n{}", self.id, data.leaf);
            }
        }

        self.change_state(NodeState::RoutingRequests).await;
        Ok(())
    }

    pub async fn fix_table_entry(&self, node: &NodeInfo) -> Result<()> {
        info!("#{:016X}: Fixing routing table", self.id);
        self.change_state(NodeState::UpdatingConnections).await;

        let mut data = self.state.data.write().await;

        // remove node from table
        data.table.remove(node.id)?;

        let matched_digits = util::get_num_matched_digits(self.id, node.id)?;
        let mut matched = matched_digits;
        let replacement = 'outer: loop {
            let row = data.table.get_row(matched as usize);
            if row.is_none() {
                break None;
            }

            for entry in row.unwrap() {
                if entry.is_none() || entry.unwrap().id == node.id {
                    continue;
                }

                let entry = entry.unwrap();

                let mut client = match NodeServiceClient::connect(entry.pub_addr.to_owned()).await {
                    Ok(client) => client,
                    Err(err) => {
                        warn!(
                            "#{:016X}: Connection to #{:016X} failed: {}",
                            self.id, entry.id, err
                        );
                        continue;
                    }
                };

                let table_entry = client
                    .get_node_table_entry(GetNodeTableEntryRequest {
                        row: matched_digits,
                        column: util::get_nth_digit_in_u64_hex(
                            node.id,
                            matched_digits as usize + 1,
                        )?,
                    })
                    .await?
                    .into_inner()
                    .node;

                if table_entry.is_some() && table_entry.clone().unwrap().id != node.id {
                    break 'outer table_entry;
                }
            }

            matched += 1;
            if matched == U64_HEX_NUM_OF_DIGITS {
                break None;
            }
        };

        if let Some(replacement) = replacement {
            data.table
                .insert(replacement.id, NodeInfo::from_node_entry(&replacement))?;
            debug!("#{:016X}: Fixed routing table: \n{}", self.id, data.table);
        }

        self.change_state(NodeState::RoutingRequests).await;
        Ok(())
    }

    pub async fn warn_and_fix_leaf_entry(&self, node: &NodeInfo, err: &str) {
        warn!(
            "#{:016X}: Connection to #{:016X} failed: {}",
            self.id, node.id, err
        );
        let _ = self.fix_leaf_entry(&node).await;

        // notify neighbors of failed leaf entry
        for leaf_entry in self.state.data.read().await.leaf.get_entries() {
            let mut client = match NodeServiceClient::connect(leaf_entry.pub_addr.to_owned()).await
            {
                Ok(client) => client,
                Err(err) => {
                    warn!(
                        "#{:016X}: Connection to #{:016X} failed: {}",
                        self.id, node.id, err
                    );
                    continue;
                }
            };

            let _ = client
                .fix_leaf_set(FixLeafSetRequest {
                    id: node.id,
                    pub_addr: node.pub_addr.clone(),
                })
                .await;
        }
    }

    pub async fn warn_and_fix_table_entry(&self, node: &NodeInfo, err: &str) {
        warn!(
            "#{:016X}: Connection to #{:016X} failed: {}",
            self.id, node.id, err
        );

        let curr_node = self.clone();
        let failed_node = node.clone();
        tokio::spawn(async move { curr_node.fix_table_entry(&failed_node).await });
    }
}
