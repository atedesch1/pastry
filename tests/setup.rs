use core::fmt;

use pastry::{
    dht::node::{Node, NodeInfo, PastryConfig},
    error::Result,
    rpc::node::node_service_client::NodeServiceClient,
};
use rand::Rng;
use tokio::task::JoinHandle;
use tonic::transport::Channel;

#[derive(Clone)]
pub struct NetworkConfiguration {
    pub num_nodes: u32,
    pub pastry_conf: PastryConfig,
}

impl fmt::Display for NetworkConfiguration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Network Configuration: num_nodes={}, config=(k={})\n",
            self.num_nodes, self.pastry_conf.leaf_set_k,
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
        }
    }

    /// Initializes the Pastry Network, filling the vector of nodes with connections to the network
    /// nodes.
    ///
    pub async fn init(mut self) -> Result<Self> {
        let start_port = 50000;

        for i in 0..self.conf.num_nodes {
            let port = start_port + i;
            let node = Node::new(self.conf.pastry_conf.clone(), "0.0.0.0", &port.to_string())?;

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

            let handle =
                tokio::spawn(
                    async move { node.bootstrap_and_serve(bootstrap_addr.as_deref()).await },
                );

            self.nodes.push(NetworkNode { info, handle });

            tokio::time::sleep(std::time::Duration::from_millis(
                2 * self.conf.pastry_conf.leaf_set_k as u64 * 100,
            ))
            .await;
        }

        self.nodes.sort_by_key(|f| f.info.id);

        println!("Created network: {}", self);

        Ok(self)
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
        let client = NodeServiceClient::connect(node.info.pub_addr.clone()).await?;

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
            write!(f, "(#{:x}: address={})\n", node.info.id, node.info.pub_addr)?;
        }
        write!(f, "]")?;
        Ok(())
    }
}
