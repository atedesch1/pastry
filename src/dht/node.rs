use std::sync::Arc;

use log::info;
use tokio::sync::{Mutex, RwLock};
use tonic::{
    transport::{Channel, Server},
    Request,
};

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

#[derive(Debug)]
pub struct Node {
    pub id: u64,
    pub addr: String,
    pub pub_addr: String,

    pub leaf: Arc<RwLock<LeafSet<NodeConnection>>>,
    pub table: Arc<RwLock<RoutingTable<NodeInfo>>>,

    is_connected: Arc<Mutex<()>>,
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
            leaf: Arc::new(RwLock::new(LeafSet::new(
                config.leaf_set_k,
                id,
                NodeConnection::new(id, &pub_addr, None),
            )?)),
            table: Arc::new(RwLock::new(RoutingTable::new(id))),
            is_connected: Arc::new(Mutex::new(())),
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
                self.leaf.clone(),
                self.table.clone(),
                self.is_connected.clone(),
                bootstrap_addr.to_string(),
            ));
        } else {
            info!("#{:x}: Initializing network", self.id);
            let _ = self.is_connected.lock().await;
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
        leaf: Arc<RwLock<LeafSet<NodeConnection>>>,
        table: Arc<RwLock<RoutingTable<NodeInfo>>>,
        is_connected: Arc<Mutex<()>>,
        bootstrap_addr: String,
    ) -> Result<()> {
        let _ = is_connected.lock().await;

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

        info!("#{:x}: Connected to network", id);

        Ok(())
    }

    /// Blocks execution until node is connected to network.
    pub async fn block_until_connected(&self) -> () {
        let _ = self.is_connected.lock().await;
    }
}
