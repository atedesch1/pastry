use tonic::Request;

use crate::{
    error::*,
    internal::{
        pastry::shared::Config,
        util::{self, get_neighbors},
    },
};

use super::{
    super::{node::*, service::grpc::*},
    setup::*,
    util::*,
};

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial]
async fn test_join() -> Result<()> {
    for k in vec![1, 4, 16] {
        for num_of_nodes in vec![1, 4, 16, 64] {
            let network = Network::new(NetworkConfiguration {
                pastry_conf: Config::new(k),
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
                let mut neighbors = get_neighbors(&network.nodes, idx, network.conf.pastry_conf.k)
                    .iter()
                    .map(|f| f.info.id)
                    .collect::<Vec<u64>>();
                neighbors.sort();

                assert_eq!(
                    leaf_set.clone(),
                    neighbors.clone(),
                    "\nExpected left == right\n left: {}\n right: {}\n",
                    format_ids(leaf_set),
                    format_ids(neighbors)
                );
            }

            network.shutdown();
        }
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial]
async fn test_transfer_keys() -> Result<()> {
    let initial_ids: Vec<u64> = (0..6)
        .map(|i| (i * (u64::MAX as u128) / 6) as u64)
        .collect();
    let keys: Vec<u64> = initial_ids
        .iter()
        .map(|id| vec![id + u64::MAX / 18, id + u64::MAX / 9])
        .flatten()
        .collect();

    let mut network = Network::new(NetworkConfiguration {
        pastry_conf: Config::new(4),
        num_nodes: 6,
    })
    .with_ids(initial_ids)
    .init()
    .await?;

    for key in &keys {
        let (_, mut client) = network.get_random_node_connection().await?;
        client
            .query(QueryRequest {
                from_id: 0,
                matched_digits: 0,
                hops: 0,
                query_type: QueryType::Set.into(),
                key: *key,
                value: Some(key.to_be_bytes().to_vec()),
            })
            .await?;
    }

    let more_ids: Vec<u64> = (0..6)
        .map(|i| u64::MAX / 12 + (i * (u64::MAX as u128) / 6) as u64)
        .collect();
    for id in more_ids {
        network.add_node_with_id(id).await?;
    }

    for key in &keys {
        let (_, mut client) = network.get_random_node_connection().await?;
        let res = client
            .query(QueryRequest {
                from_id: 0,
                matched_digits: 0,
                hops: 0,
                query_type: QueryType::Get.into(),
                key: *key,
                value: None,
            })
            .await?
            .into_inner();
        let idx = find_responsible(&network.nodes, *key);

        assert_eq!(res.from_id, network.nodes[idx].info.id);
        assert_eq!(res.value, Some(key.to_be_bytes().to_vec()));
    }
    network.shutdown();

    Ok(())
}
