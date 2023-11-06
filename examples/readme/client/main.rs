use pastry_dht::client::PastryClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    let node_addr = args.get(1).unwrap();

    let mut client = PastryClient::connect(&node_addr).await?;

    client.set_kv("pastry".as_bytes(), "rocks!".as_bytes()).await?;
    let value = client.get_kv("pastry".as_bytes()).await?;
    println!("Value read from Pastry network: {}", String::from_utf8(value.unwrap())?);
    Ok(())
}