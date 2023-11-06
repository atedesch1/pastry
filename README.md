# Pastry

A reusable Pastry DHT protocol implementation using gRPC for node communication.

Refer to the [Pastry paper](https://rowstron.azurewebsites.net/PAST/pastry.pdf) for the algorithm.

## Usage

Install with cargo:
```
cargo install pastry-dht
```
or include in Cargo.toml `pastry-dht = "*"`.

## Example
Running a simple PastryNode with parameter **k=8**:

> Create a binary for the node server

```rust
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
```

> Run in three different terminals
  1. `cargo run 0.0.0.0 50000`
  2. `cargo run 0.0.0.0 50001 http://0.0.0.0:50000`
  3. `cargo run 0.0.0.0 50002 http://0.0.0.0:50001`

This will spin up a 3-node network.

> Now, create a binary for the client: using *PastryClient* we connect to a node and send a request
```rust
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
```
> Finally, in another terminal execute `cargo run http://0.0.0.0:50001`. 

This should print 'Value read from Pastry network: rocks!'

### TODO
- [x] Create Leaf set/Routing table structures
- [x] Handle node arrivals (Join)
- [x] Handle get/set/delete queries (Query)
- [x] Handle node failures
- [ ] Handle range queries
- [ ] Handle concurrent node arrivals
- [ ] Handle concurrent node failures
- [ ] Handle data replication