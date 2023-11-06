use std::env;

use log::info;
use pastry_dht::error::*;
use pastry_dht::{node::PastryNode, Config};

struct KVStoreNode {
    pastry_node: PastryNode,
}

impl KVStoreNode {
    pub fn new(hostname: &str, port: &str) -> Result<Self> {
        let config = Config::new(16);
        Ok(KVStoreNode {
            pastry_node: PastryNode::new(config, hostname, port)?,
        })
    }

    pub async fn serve(&mut self, bootstrap_addr: Option<&str>) -> Result<()> {
        self.pastry_node
            .clone()
            .bootstrap_and_serve(bootstrap_addr)
            .await
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    let port = args
        .get(1)
        .ok_or(Error::Parse("missing port argument".into()))?;

    let bootstrap_addr = args.get(2).map(|s| s.as_str());

    env_logger::init();

    let hostname = std::env::var("NODE_HOSTNAME").unwrap_or("0.0.0.0".to_owned());
    let public_addr = format!("http://{}:{}", hostname, port);

    info!("Initializing node on {}", public_addr);

    KVStoreNode::new(&hostname, &port)?
        .serve(bootstrap_addr)
        .await
}
