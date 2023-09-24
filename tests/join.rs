use pastry::dht::node::PastryConfig;

mod setup;

use setup::*;

#[tokio::test]
async fn test_join() -> Result<(), Box<dyn std::error::Error>> {
    let config = NetworkConfiguration {
        pastry_conf: PastryConfig { leaf_set_k: 1 },
        num_nodes: 3,
    };

    let network = setup_network(config).await?;

    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    Ok(())
}
