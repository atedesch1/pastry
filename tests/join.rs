use pastry::{dht::node::PastryConfig, rpc::node::node_service_client::NodeServiceClient};

mod setup;
mod util;

use setup::*;
use tonic::Request;

fn get_neighbors<T>(vector: &[T], index: usize, k: usize) -> Vec<&T> {
    let len = vector.len();
    if len == 0 || index >= len || k == 0 {
        return Vec::new(); // Return an empty vector for invalid inputs.
    }

    let mut neighbors = Vec::with_capacity(2 * k + 1);

    for i in (0..=k).rev() {
        let prev_index = (index + len - i) % len;
        neighbors.push(&vector[prev_index]);
    }

    for i in 1..=k {
        let next_index = (index + i) % len;
        neighbors.push(&vector[next_index]);
    }

    neighbors
}

#[tokio::test]
async fn test_join() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::from_default_env()
        .filter_level(log::LevelFilter::Debug)
        .init();

    let config = NetworkConfiguration {
        pastry_conf: PastryConfig { leaf_set_k: 2 },
        num_nodes: 16,
    };

    let network = setup_network(config.clone()).await?;

    for (idx, node) in network.nodes.iter().enumerate() {
        let mut client = NodeServiceClient::connect(node.info.pub_addr.clone()).await?;
        let state = client.get_node_state(Request::new(())).await?.into_inner();
        let leaf_set = state
            .leaf_set
            .clone()
            .iter()
            .map(|f| f.id)
            .collect::<Vec<u64>>();
        let mut neighbors = get_neighbors(&network.nodes, idx, config.pastry_conf.leaf_set_k)
            .iter()
            .map(|f| f.info.id)
            .collect::<Vec<u64>>();
        neighbors.sort();

        assert_eq!(
            leaf_set.clone(),
            neighbors.clone(),
            "\nExpected left == right\n left: {}\n right: {}\n",
            util::format(leaf_set),
            util::format(neighbors)
        );
    }

    Ok(())
}
