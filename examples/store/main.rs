use std::env;
use std::net::SocketAddr;

use log::info;
use pastry_dht::error::*;
use pastry_dht::{node::PastryNode, Config};

struct KVStoreNode {
    pastry_node: PastryNode,
}

impl KVStoreNode {
    pub fn new(addr: SocketAddr, pub_addr: SocketAddr) -> Result<Self> {
        let config = Config::new(16);
        Ok(KVStoreNode {
            pastry_node: PastryNode::new(config, addr, pub_addr)?,
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

    let addr: SocketAddr = format!("0.0.0.0:{}", port).parse()?;
    info!("Initializing node on {}", addr);

    KVStoreNode::new(addr, addr)?.serve(bootstrap_addr).await
}
