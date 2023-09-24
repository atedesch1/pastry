use core::fmt;

use pastry::{
    dht::node::{Node, NodeInfo, PastryConfig},
    error::Result,
    rpc::node::node_service_client::NodeServiceClient,
};
use rand::Rng;
use tonic::transport::Channel;

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

pub struct Network {
    pub conf: NetworkConfiguration,
    pub nodes: Vec<NodeInfo>,
}

impl fmt::Display for Network {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.conf)?;
        write!(f, "Nodes: [\n")?;
        for node in &self.nodes {
            write!(f, "(#{:x}: address={})\n", node.id, node.pub_addr)?;
        }
        write!(f, "]")?;
        Ok(())
    }
}

/// Setups a Pastry Network with the provided configuration.
///
/// # Arguments
///
/// * `conf` - Pastry network parameters.
///
/// # Returns
///
/// A Result containing the network configuration and all node address sorted by ID.
///
pub async fn setup_network(conf: NetworkConfiguration) -> Result<Network> {
    let mut nodes: Vec<NodeInfo> = Vec::new();
    let start_port = 50000;

    for i in 0..conf.num_nodes {
        let port = start_port + i;
        let node = Node::new(conf.pastry_conf.clone(), "0.0.0.0", &port.to_string())?;

        let bootstrap_addr = if nodes.is_empty() {
            None
        } else {
            let random_index = rand::thread_rng().gen_range(0..nodes.len());
            Some(nodes[random_index].pub_addr.clone())
        };

        nodes.push(NodeInfo {
            id: node.id,
            pub_addr: node.pub_addr.clone(),
        });

        tokio::spawn(async move { node.bootstrap_and_serve(bootstrap_addr.as_deref()).await });

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    nodes.sort_by_key(|f| f.id);

    let network = Network { conf, nodes };

    println!("{}", network);

    Ok(network)
}

/// Connects to a random node in the network.
///
/// # Arguments
///
/// * `network` - A Pastry network.
///
/// # Returns
///
/// A Result containing a client connection to a random node in the network.
///
pub async fn get_random_client(network: &Network) -> Result<NodeServiceClient<Channel>> {
    let random_index = rand::thread_rng().gen_range(0..network.nodes.len());
    let node = &network.nodes[random_index];
    let client = NodeServiceClient::connect(node.pub_addr.clone()).await?;

    Ok(client)
}
