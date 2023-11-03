use tonic::Request;

use crate::{
    error::*,
    internal::{pastry::shared::Config, util::get_neighbors},
};

use super::{
    super::{grpc::*, node::*},
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
    let mut network = Network::new(NetworkConfiguration {
        pastry_conf: Config::new(4),
        num_nodes: 0,
    })
    .with_ids(vec![u64::MAX - 10, 0, 10])
    .init()
    .await?;

    network.add_node().await?;

    let (_, mut client) = network.get_random_node_connection().await?;
    client
        .query(QueryRequest {
            from_id: 0,
            matched_digits: 0,
            hops: 0,
            query_type: QueryType::Set.into(),
            key: u64::MAX - 5,
            value: Some("first".as_bytes().to_vec()),
        })
        .await?;
    client
        .query(QueryRequest {
            from_id: 0,
            matched_digits: 0,
            hops: 0,
            query_type: QueryType::Set.into(),
            key: 5,
            value: Some("second".as_bytes().to_vec()),
        })
        .await?;
    client
        .query(QueryRequest {
            from_id: 0,
            matched_digits: 0,
            hops: 0,
            query_type: QueryType::Set.into(),
            key: 15,
            value: Some("third".as_bytes().to_vec()),
        })
        .await?;

    network.add_node().await?;
    network.add_node().await?;

    let res = client
        .query(QueryRequest {
            from_id: 0,
            matched_digits: 0,
            hops: 0,
            query_type: QueryType::Get.into(),
            key: u64::MAX - 5,
            value: None,
        })
        .await?
        .into_inner();
    assert_eq!(res.from_id, u64::MAX - 10);
    assert_eq!(String::from_utf8(res.value.unwrap())?, "first".to_owned());

    let res = client
        .query(QueryRequest {
            from_id: 0,
            matched_digits: 0,
            hops: 0,
            query_type: QueryType::Get.into(),
            key: 5,
            value: None,
        })
        .await?
        .into_inner();
    assert_eq!(res.from_id, 0);
    assert_eq!(String::from_utf8(res.value.unwrap())?, "second".to_owned());

    let res = client
        .query(QueryRequest {
            from_id: 0,
            matched_digits: 0,
            hops: 0,
            query_type: QueryType::Get.into(),
            key: 15,
            value: None,
        })
        .await?
        .into_inner();
    assert_eq!(res.from_id, 10);
    assert_eq!(String::from_utf8(res.value.unwrap())?, "third".to_owned());

    network.shutdown();

    Ok(())
}
