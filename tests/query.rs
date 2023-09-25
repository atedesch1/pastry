mod setup;
mod util;

use std::time::{SystemTime, UNIX_EPOCH};

use pastry::{dht::node::PastryConfig, hring::hasher::Sha256Hasher, rpc::node::QueryRequest};
use setup::*;
use tonic::Request;

fn find_responsible(nodes: &Vec<NetworkNode>, key: u64) -> usize {
    let mut position = match nodes.binary_search_by_key(&key, |f| f.info.id) {
        Ok(position) => position,
        Err(position) => position,
    };

    if position == nodes.len() {
        position -= 1;
    }

    if key < nodes[position].info.id {
        position = if position == 0 {
            nodes.len() - 1
        } else {
            position - 1
        };
    }

    position
}

#[tokio::test]
async fn test_query() -> Result<(), Box<dyn std::error::Error>> {
    // env_logger::Builder::from_default_env()
    //     .filter_level(log::LevelFilter::Debug)
    //     .init();

    let network = Network::new(NetworkConfiguration {
        pastry_conf: PastryConfig { leaf_set_k: 2 },
        num_nodes: 20,
    })
    .init()
    .await?;

    for i in 0..5 {
        let (_, mut client) = network.get_random_node_connection().await?;

        let key = Sha256Hasher::hash_once(
            format!(
                "{}_{}",
                i,
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)?
                    .as_nanos()
                    .to_string()
            )
            .as_bytes(),
        );

        let res = client
            .query(Request::new(QueryRequest {
                from_id: 0,
                matched_digits: 0,
                key,
            }))
            .await?
            .into_inner();

        let idx = find_responsible(&network.nodes, key);

        assert_eq!(res.id, network.nodes[idx].info.id);
    }

    network.shutdown();

    Ok(())
}
