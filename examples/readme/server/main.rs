use pastry_dht::node::PastryNode;
use pastry_dht::Config;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    let hostname = args.get(1).unwrap();
    let port = args.get(2).unwrap();
    let bootstrap_addr = args.get(3).map(|s| s.as_str());

    env_logger::Builder::from_default_env()
        .filter_level(log::LevelFilter::Info)
        .init();

    Ok(PastryNode::new(Config::new(8), &hostname, &port)?
        .bootstrap_and_serve(bootstrap_addr)
        .await?)
}
