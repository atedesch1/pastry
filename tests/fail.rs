mod setup;
mod util;

use log::info;
use pastry::{
    dht::node::{Node, NodeInfo, PastryConfig},
    error::Result,
    rpc::node::QueryRequest,
    util::get_neighbors,
};
use rand::Rng;
use setup::*;

#[tokio::test(flavor = "multi_thread")]
async fn test_fail() -> Result<()> {
    // env_logger::Builder::from_default_env()
    //     .filter_level(log::LevelFilter::Debug)
    //     .init();

    let mut network = Network::new(NetworkConfiguration {
        pastry_conf: PastryConfig { leaf_set_k: 2 },
        // num_nodes: 8,
        num_nodes: 100,
    })
    // .with_ids(vec![0, 1, 2, 3, 4, 5, 6, 7])
    .init()
    .await?;

    for _ in 0..50 {
        let random_index = rand::thread_rng().gen_range(0..network.nodes.len());
        let node = &network.nodes[random_index];
        let node_info = node.info.clone();

        // get node neighbors without itself
        let prev_neighbors = get_neighbors(
            &network.nodes,
            random_index,
            network.conf.pastry_conf.leaf_set_k,
        )
        .iter()
        .map(|&f| f.info.clone())
        .filter(|f| f.id != node.info.id)
        .collect::<Vec<NodeInfo>>();

        // remove node from network
        info!("TEST: Removing Node #{:016X}: ", node.info.id);
        node.handle.abort();
        network.nodes.remove(random_index);

        // query its previous neighbors in order for them to fix their
        // leaf set and get their state
        for neighbor in prev_neighbors {
            let mut client = Node::connect_with_retry(&neighbor.pub_addr).await?;

            // let state = client.get_node_state(()).await?.into_inner();
            // let leaf_set = state
            //     .leaf_set
            //     .clone()
            //     .iter()
            //     .map(|f| f.id)
            //     .collect::<Vec<u64>>();
            // info!("NODE leaf set: {}", util::format(leaf_set));

            // query neighbor for node
            client
                .query(QueryRequest {
                    from_id: 0,
                    matched_digits: 0,
                    key: node_info.id,
                })
                .await?;

            // get neighbor state
            let state = client.get_node_state(()).await?.into_inner();
            let mut leaf_set = state
                .leaf_set
                .clone()
                .iter()
                .map(|f| f.id)
                .collect::<Vec<u64>>();
            leaf_set.sort();
            let neighbor_index = network
                .nodes
                .iter()
                .position(|e| e.info.id == neighbor.id)
                .unwrap();
            let mut neighbors = get_neighbors(
                &network.nodes,
                neighbor_index,
                network.conf.pastry_conf.leaf_set_k,
            )
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
    }

    network.shutdown();

    Ok(())
}
