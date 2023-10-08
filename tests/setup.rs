use core::fmt;
use std::net::TcpListener;

use log::error;
use pastry::{
    dht::node::{Node, NodeInfo, PastryConfig},
    error::Result,
    rpc::node::node_service_client::NodeServiceClient,
};
use rand::Rng;
use tokio::{sync::mpsc, task::JoinHandle};
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
        let mut num_deployed = 0;
        let mut port = 30000;
        while num_deployed < self.conf.num_nodes {
            // while Self::is_port_in_use(port) {
            //     port += 1;
            // }

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

            let (tx, mut rx) = mpsc::channel::<bool>(1);

            let handle = tokio::spawn(async move {
                node.bootstrap_and_serve(bootstrap_addr.as_deref(), Some(tx))
                    .await
            });

            self.nodes.push(NetworkNode { info, handle });

            rx.recv().await.unwrap();
            num_deployed += 1;
            port += 1;
        }

        self.nodes.sort_by_key(|f| f.info.id);

        println!("Created network: {}", self);

        Ok(self)
    }

    pub fn is_port_in_use(port: i32) -> bool {
        TcpListener::bind(format!("0.0.0.0:{}", port)).is_err()
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
