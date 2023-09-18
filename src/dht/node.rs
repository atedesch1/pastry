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
        node_service_client::NodeServiceClient, node_service_server::NodeServiceServer, JoinRequest,
    },
    util::HasID,
};

#[derive(Debug, Clone)]
pub struct NodeInfo {
    pub id: u64,
    pub pub_addr: String,
}
impl HasID<u64> for NodeInfo {
    fn get_id(&self) -> u64 {
        self.id
    }
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

    pub is_connected: Arc<Mutex<bool>>,
}

/// Configuration for the Pastry network
#[derive(Debug, Clone)]
pub struct PastryConfig {
    pub leaf_set_k: usize,
}

impl Node {
    /// Creates a new DHT node which will be available publicly on
    /// http://hostname:port
    pub fn new(config: PastryConfig, hostname: &str, port: &str) -> Result<Self> {
        let pub_addr = format!("http://{}:{}", hostname, port);
        let addr = format!("0.0.0.0:{}", port);
        let id = Sha256Hasher::hash_once(pub_addr.as_bytes());

        info!("Registering node as {} = #{:x}", id, id);

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
            is_connected: Arc::new(Mutex::new(false)),
        })
    }

    /// Serves server and connects to bootstrap node if exists
    /// else creates own network.
    pub async fn bootstrap_and_serve(self, bootstrap_addr: Option<&str>) -> Result<()> {
        info!("Initializing #{:x} on {}", self.id, self.pub_addr);

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
            info!("Initializing network");
            let mut guard = self.is_connected.lock().await;
            *guard = true;
            info!("Connected to network");
        }

        let addr = self.addr.clone();
        Ok(Server::builder()
            .add_service(NodeServiceServer::new(self))
            .serve(addr.parse()?)
            .await?)
    }

    /// Connects to bootstrap node if exists and joins network, else
    /// creates own network.
    async fn connect_to_network(
        id: u64,
        pub_addr: String,
        leaf: Arc<RwLock<LeafSet<NodeConnection>>>,
        table: Arc<RwLock<RoutingTable<NodeInfo>>>,
        is_connected: Arc<Mutex<bool>>,
        bootstrap_addr: String,
    ) -> Result<()> {
        let mut guard = is_connected.lock().await;

        info!("Connecting to network through #{:x}", id);

        let mut client = NodeServiceClient::connect(bootstrap_addr.to_owned()).await?;
        let join_response = client
            .join(JoinRequest { id, pub_addr })
            .await?
            .into_inner();

        let mut leaf = leaf.write().await;
        for entry in join_response.leaf_set {
            let client = NodeServiceClient::connect(entry.pub_addr.clone()).await?;
            leaf.insert(
                entry.id,
                NodeConnection::new(entry.id, &entry.pub_addr, Some(client)),
            )?;
        }
        // Send join updates to leaf set

        *guard = true;

        info!("Connected to network");

        Ok(())
    }
}
