use core::fmt;
use log::warn;
use rand::Rng;
use std::net::SocketAddr;
use tokio::task::JoinHandle;
use tonic::transport::Channel;

use crate::{
    error::*,
    internal::{pastry::shared::Config, util::get_neighbors},
};

use super::super::{node::*, service::grpc::*};

const INITIAL_PORT: i32 = 30000;

#[derive(Clone)]
pub struct NetworkConfiguration {
    pub num_nodes: u32,
    pub pastry_conf: Config,
}

impl NetworkConfiguration {
    pub fn new(num_nodes: u32, pastry_conf: Config) -> Self {
        NetworkConfiguration {
            num_nodes,
            pastry_conf,
        }
    }
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
    pub num_deployed: u32,
    pub available_port: i32,
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
            num_deployed: 0,
            available_port: INITIAL_PORT,
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
        while self.num_deployed < self.conf.num_nodes {
            let (info, handle) = loop {
                let addr: SocketAddr = format!("0.0.0.0:{}", self.available_port).parse()?;
                let node = if let Some(&id) = self.ids.get(self.num_deployed as usize) {
                    Node::from_id(self.conf.pastry_conf.clone(), addr, addr, id)?
                } else {
                    Node::new(self.conf.pastry_conf.clone(), addr, addr)?
                };

                match self.setup_node(node).await {
                    Ok(n) => break n,
                    Err(e) => warn!("error setting up node: {}", e),
                }
                self.available_port += 1;
            };

            self.nodes.push(NetworkNode { info, handle });

            self.num_deployed += 1;
            self.available_port += 1;
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
        let mut nodes: Vec<Node> = Vec::new();
        while self.num_deployed < self.conf.num_nodes {
            let (node, handle) = loop {
                let addr: SocketAddr = format!("0.0.0.0:{}", self.available_port).parse()?;
                let node = if let Some(&id) = self.ids.get(self.num_deployed as usize) {
                    Node::from_id(self.conf.pastry_conf.clone(), addr, addr, id)?
                } else {
                    Node::new(self.conf.pastry_conf.clone(), addr, addr)?
                };

                match node.serve().await {
                    Ok(handle) => break (node, handle),
                    Err(e) => warn!("error setting up node: {}", e),
                };
                self.available_port += 1;
            };

            let info = NodeInfo {
                id: node.id,
                pub_addr: node.pub_addr.clone(),
            };

            self.nodes.push(NetworkNode { info, handle });
            nodes.push(node);

            self.num_deployed += 1;
            self.available_port += 1;
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

    pub async fn add_node(&mut self) -> Result<NodeInfo> {
        let (info, handle) = loop {
            let addr: SocketAddr = format!("0.0.0.0:{}", self.available_port).parse()?;
            let node = if let Some(&id) = self.ids.get(self.num_deployed as usize) {
                Node::from_id(self.conf.pastry_conf.clone(), addr, addr, id)?
            } else {
                Node::new(self.conf.pastry_conf.clone(), addr, addr)?
            };

            match self.setup_node(node).await {
                Ok(n) => break n,
                Err(e) => {
                    warn!("error setting up node: {}", e);
                    self.available_port += 1;
                }
            }
        };

        self.nodes.push(NetworkNode {
            info: info.clone(),
            handle,
        });

        self.conf.num_nodes += 1;
        self.num_deployed += 1;

        self.nodes.sort_by_key(|f| f.info.id);

        println!(
            "Added node #{:016X} with address {}",
            info.id, info.pub_addr
        );

        Ok(info)
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
