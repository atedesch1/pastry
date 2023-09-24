extern crate pastry;

use log::info;
use pastry::{
    dht::node::PastryConfig,
    error::{Error, Result},
};
use std::env;

use pastry::dht::node::Node;

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    let k = args
        .get(1)
        .ok_or(Error::Parse("missing leaf set size argument".into()))?
        .parse::<usize>()?;
    let port = args
        .get(2)
        .ok_or(Error::Parse("missing port argument".into()))?;

    let bootstrap_addr = args.get(3).map(|s| s.as_str());

    env_logger::init();

    let hostname = std::env::var("NODE_HOSTNAME").unwrap_or("0.0.0.0".to_owned());
    let public_addr = format!("http://{}:{}", hostname, port);

    info!("Initializing node on {}", public_addr);

    let config = PastryConfig { leaf_set_k: k };

    let node = Node::new(config, &hostname, &port)?;

    node.bootstrap_and_serve(bootstrap_addr).await
}
