use log::info;
use std::time::{SystemTime, UNIX_EPOCH};
use tonic::Request;

use super::{super::service::grpc::*, setup::*};
use crate::{
    error::*,
    internal::{hring::hasher::Sha256Hasher, pastry::shared::Config},
};

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

fn get_random_key(i: i32) -> Result<u64> {
    Ok(Sha256Hasher::hash_once(
        format!(
            "{}_{}",
            i,
            SystemTime::now()
                .duration_since(UNIX_EPOCH)?
                .as_nanos()
                .to_string()
        )
        .as_bytes(),
    ))
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial]
async fn test_query() -> Result<()> {
    let network = Network::new(NetworkConfiguration {
        pastry_conf: Config::new(8),
        num_nodes: 512,
    })
    .init()
    .await?;

    let num_queries: u32 = 256;
    let mut hops = 0;
    for i in 0..num_queries {
        let (_, mut client) = network.get_random_node_connection().await?;

        let key = get_random_key(i as i32)?;

        let res = client
            .query(Request::new(QueryRequest {
                from_id: 0,
                matched_digits: 0,
                hops: 0,
                query_type: QueryType::Get.into(),
                key,
                value: None,
            }))
            .await?
            .into_inner();

        hops += res.hops;
        let idx = find_responsible(&network.nodes, key);

        assert_eq!(res.from_id, network.nodes[idx].info.id);
    }

    let mean_hops = hops / num_queries;
    let complexity = ((network.nodes.len() as f64).log2() / 4.0) as u32;

    info!("Mean number of hops: {}", mean_hops);
    info!("Complexity: {}", complexity);
    assert_eq!(mean_hops <= complexity, true);

    network.shutdown();

    Ok(())
}
