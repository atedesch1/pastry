extern crate pastry_dht;

use log::info;
use pastry_dht::{error::*, node::PastryNode, Config};
use std::env;

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

    let node = PastryNode::new(Config::new(k), &hostname, &port)?;

    node.bootstrap_and_serve(bootstrap_addr).await?.await?
}
