use core::fmt;

use log::warn;
use pastry_dht::{
    dht::node::{Node, NodeInfo},
    error::Result,
    pastry::shared::Config,
    rpc::node::node_service_client::NodeServiceClient,
    util::get_neighbors,
};
use rand::Rng;
use tokio::task::JoinHandle;
use tonic::transport::Channel;

#[derive(Clone)]
pub struct NetworkConfiguration {
    pub num_nodes: u32,
    pub pastry_conf: Config,
}

impl fmt::Display for NetworkConfiguration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Network Configuration: num_nodes={}, config=(k={})\n",
            self.num_nodes, self.pastry_conf.k,
        )
    }
}

#[derive(Debug)]
pub struct NetworkNode {
    pub info: NodeInfo,
    pub handle: JoinHandle<Result<()>>,
}

pub struct Network {
    pub conf: NetworkConfiguration,
    pub nodes: Vec<NetworkNode>,
    pub ids: Vec<u64>,
}

impl Network {
    /// Creates a Pastry Network with the provided configuration.
    ///
    /// # Arguments
    ///
    /// * `conf` - Pastry network parameters.
    ///
    /// # Returns
    ///
    /// An empty network.
    ///
    pub fn new(conf: NetworkConfiguration) -> Self {
        Network {
            conf,
            nodes: Vec::new(),
            ids: Vec::new(),
        }
    }

    /// Specifies the IDs for the nodes.
    ///
    /// # Arguments
    ///
    /// * `ids` - Vector of Node IDs.
    ///
    /// # Returns
    ///
    /// The same network with IDs specified.
    ///
    /// # Panics
    ///
    /// Panics if number of nodes is different then number of IDs.
    ///
    pub fn with_ids(mut self, ids: Vec<u64>) -> Self {
        if ids.len() != self.conf.num_nodes as usize {
            panic!("number of ids must match number of nodes");
        }

        self.ids = ids;
        self
    }

    /// Adds logging to the network.
    ///
    /// # Arguments
    ///
    /// * `filter` - Log filter.
    ///
    /// # Returns
    ///
    /// The same network with IDs specified.
    ///
    pub fn with_logging(self, filter: log::LevelFilter) -> Self {
        env_logger::Builder::from_default_env()
            .filter_level(filter)
            .init();
        self
    }

    /// Initializes the Pastry Network.
    /// Creates and serves all nodes and updates their state based on network.
    ///
    /// This function just calls init_by_state().
    ///
    pub async fn init(self) -> Result<Self> {
        self.init_by_state().await
    }

    /// Initializes the Pastry Network.
    /// Creates each node by bootstraping to the previously created nodes
    /// and serving them.
    ///
    /// This will make each node request to join the network.
    ///
    pub async fn init_by_join(mut self) -> Result<Self> {
        let mut num_deployed = 0;
        let mut port = 30000;
        while num_deployed < self.conf.num_nodes {
            let (info, handle) = loop {
                let node = if let Some(&id) = self.ids.get(num_deployed as usize) {
                    Node::from_id(
                        self.conf.pastry_conf.clone(),
                        "0.0.0.0",
                        &port.to_string(),
                        id,
                    )?
                } else {
                    Node::new(self.conf.pastry_conf.clone(), "0.0.0.0", &port.to_string())?
                };

                match self.setup_node(node).await {
                    Ok(n) => break n,
                    Err(e) => warn!("error setting up node: {}", e),
                }
                port += 1;
            };

            self.nodes.push(NetworkNode { info, handle });

            num_deployed += 1;
            port += 1;
        }

        self.nodes.sort_by_key(|f| f.info.id);

        println!("Created network: {}", self);

        Ok(self)
    }

    /// Initializes the Pastry Network.
    /// Creates and serves all nodes and updates their state based on network.
    ///
    /// This will only update each node's state
    ///
    pub async fn init_by_state(mut self) -> Result<Self> {
        let mut num_deployed = 0;
        let mut port = 30000;

        let mut nodes: Vec<Node> = Vec::new();
        while num_deployed < self.conf.num_nodes {
            let (node, handle) = loop {
                let node = if let Some(&id) = self.ids.get(num_deployed as usize) {
                    Node::from_id(
                        self.conf.pastry_conf.clone(),
                        "0.0.0.0",
                        &port.to_string(),
                        id,
                    )?
                } else {
                    Node::new(self.conf.pastry_conf.clone(), "0.0.0.0", &port.to_string())?
                };

                match node.serve().await {
                    Ok(handle) => break (node, handle),
                    Err(e) => warn!("error setting up node: {}", e),
                };
                port += 1;
            };

            let info = NodeInfo {
                id: node.id,
                pub_addr: node.pub_addr.clone(),
            };

            self.nodes.push(NetworkNode { info, handle });
            nodes.push(node);

            num_deployed += 1;
            port += 1;
        }

        self.nodes.sort_by_key(|f| f.info.id);
        nodes.sort_by_key(|f| f.id);

        let infos: Vec<NodeInfo> = self.nodes.iter().map(|f| f.info.clone()).collect();

        loop {
            if let Some(node) = nodes.pop() {
                let idx = nodes.len();
                let leaf_set: Vec<NodeInfo> =
                    get_neighbors(&self.nodes, idx, self.conf.pastry_conf.k)
                        .iter()
                        .map(|f| f.info.clone())
                        .filter(|f| f.id != node.id)
                        .collect();
                node.route_with_state(leaf_set, infos.clone()).await?;
            } else {
                break;
            }
        }

        println!("Created network: {}", self);

        Ok(self)
    }

    async fn setup_node(&self, node: Node) -> Result<(NodeInfo, JoinHandle<Result<()>>)> {
        let bootstrap_addr = if self.nodes.is_empty() {
            None
        } else {
            let random_index = rand::thread_rng().gen_range(0..self.nodes.len());
            Some(self.nodes[random_index].info.pub_addr.clone())
        };

        let info = NodeInfo {
            id: node.id,
            pub_addr: node.pub_addr.clone(),
        };

        let handle = node.bootstrap_and_serve(bootstrap_addr.as_deref()).await?;

        Ok((info, handle))
    }

    /// Gets a connection to a random node in the network.
    ///
    /// # Returns
    ///
    /// A Result containing a client connection to a random node in the network and the node
    /// information.
    ///
    pub async fn get_random_node_connection(
        &self,
    ) -> Result<(NodeInfo, NodeServiceClient<Channel>)> {
        let random_index = rand::thread_rng().gen_range(0..self.nodes.len());
        let node = &self.nodes[random_index];
        let client = Node::connect_with_retry(&node.info.pub_addr).await?;

        Ok((node.info.clone(), client))
    }

    /// Shuts down the network, consuming the network.
    ///
    pub fn shutdown(self) -> () {
        for node in &self.nodes {
            node.handle.abort();
        }
    }
}

impl fmt::Display for Network {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.conf)?;
        write!(f, "Nodes: [\n")?;
        for node in &self.nodes {
            write!(
                f,
                "(#{:016X}: address={})\n",
                node.info.id, node.info.pub_addr
            )?;
        }
        write!(f, "]")?;
        Ok(())
    }
}
