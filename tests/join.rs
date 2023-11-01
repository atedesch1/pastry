use pastry::{
    dht::node::{Node, PastryConfig},
    error::Result,
    util::get_neighbors,
};

mod setup;
mod util;

use setup::*;
use tonic::Request;

#[tokio::test(flavor = "multi_thread")]
async fn test_join() -> Result<()> {
    // env_logger::Builder::from_default_env()
    //     .filter_level(log::LevelFilter::Debug)
    //     .init();

    for k in vec![1, 4, 16] {
        for num_of_nodes in vec![1, 4, 16, 64] {
            let network = Network::new(NetworkConfiguration {
                pastry_conf: PastryConfig { leaf_set_k: k },
                num_nodes: num_of_nodes,
            })
            .init_by_join()
            .await?;

            for (idx, node) in network.nodes.iter().enumerate() {
                let mut client = Node::connect_with_retry(&node.info.pub_addr).await?;
                let state = client.get_node_state(Request::new(())).await?.into_inner();
                let mut leaf_set = state
                    .leaf_set
                    .clone()
                    .iter()
                    .map(|f| f.id)
                    .collect::<Vec<u64>>();
                leaf_set.sort();
                let mut neighbors =
                    get_neighbors(&network.nodes, idx, network.conf.pastry_conf.leaf_set_k)
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

            network.shutdown();
        }
    }

    Ok(())
}
