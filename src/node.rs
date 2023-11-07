use std::net::SocketAddr;

use tonic::Request;

use crate::{
    error::*,
    internal::{
        dht::{node::Node, service::grpc::*},
        hring::hasher::Sha256Hasher,
        pastry::shared::Config,
    },
};

/// An instance of a Pastry node.
///
#[derive(Clone)]
pub struct PastryNode {
    node: Node,
}

// Basic Node methods
impl PastryNode {
    /// Registers a new Pastry node which will be available publicly on
    /// http://hostname:port
    ///
    /// # Arguments
    ///
    /// * `config` - The Pastry network configuration.
    /// * `addr` - The address of the socket to listen on.
    /// * `pub_addr` - The address the node will be exposed on.
    ///
    /// # Returns
    ///
    /// A Result containing the newly registered node.
    ///
    pub fn new(config: Config, addr: SocketAddr, pub_addr: SocketAddr) -> Result<Self> {
        Ok(PastryNode {
            node: Node::new(config, addr, pub_addr)?,
        })
    }

    /// Connects to Pastry network via bootstrap node and serves node server.
    /// Consumes node.
    ///
    /// # Arguments
    ///
    /// * `bootstrap_addr` - A bootstrap node address.
    ///
    /// # Returns
    ///
    /// An empty Result
    ///
    pub async fn bootstrap_and_serve(self, bootstrap_addr: Option<&str>) -> Result<()> {
        self.node.bootstrap_and_serve(bootstrap_addr).await?.await?
    }

    /// Gets the internal Pastry node ID.
    ///
    pub fn get_id(&self) -> u64 {
        self.node.id
    }

    /// Gets the public Pastry node address.
    ///
    pub fn get_public_address(&self) -> String {
        self.node.pub_addr.clone()
    }
}

// gRPC methods
impl PastryNode {
    /// Retrieves a value associated with the given key stored in the Pastry
    /// network.
    ///
    /// # Arguments
    ///
    /// * `key` - A slice of bytes representing the key for which the value is
    /// requested.
    ///
    /// # Returns
    ///
    /// Returns a `Result` which is:
    ///
    /// - `Ok(Some(Vec<u8>))` if the key exists, containing the associated
    /// value.
    /// - `Ok(None)` if the key does not exist.
    /// - `Err(e)` where `e` encapsulates any error encountered during the
    /// operation.
    ///
    pub async fn get_kv(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let response = self
            .node
            .query(Request::new(QueryRequest {
                from_id: 0,
                matched_digits: 0,
                hops: 0,
                query_type: QueryType::Get.into(),
                key: Sha256Hasher::hash_once(key),
                value: None,
            }))
            .await?
            .into_inner();

        Ok(response.value)
    }

    /// Sets a value for a given key in the Pastry network.
    ///
    /// # Arguments
    ///
    /// * `key` - A slice of bytes representing the key to which the value is
    /// to be associated.
    /// * `value` - A slice of bytes representing the value to be set.
    ///
    /// # Returns
    ///
    /// Returns a `Result` which is:
    ///
    /// - `Ok(Some(Vec<u8>))` if the key existed and the value was replaced,
    /// containing the old value.
    /// - `Ok(None)` if the key did not exist and a new entry was created.
    /// - `Err(e)` where `e` encapsulates any error encountered during the
    /// operation.
    ///
    pub async fn set_kv(&self, key: &[u8], value: &[u8]) -> Result<Option<Vec<u8>>> {
        let response = self
            .node
            .query(Request::new(QueryRequest {
                from_id: 0,
                matched_digits: 0,
                hops: 0,
                query_type: QueryType::Set.into(),
                key: Sha256Hasher::hash_once(key),
                value: Some(value.to_vec()),
            }))
            .await?
            .into_inner();

        Ok(response.value)
    }

    /// Deletes the value associated with the given key in the Pastry network.
    ///
    /// # Arguments
    ///
    /// * `key` - A slice of bytes representing the key whose associated value
    /// is to be deleted.
    ///
    /// # Returns
    ///
    /// Returns a `Result` which is:
    ///
    /// - `Ok(Some(Vec<u8>))` if the key existed and the value was successfully deleted, containing the deleted value.
    /// - `Ok(None)` if the key did not exist.
    /// - `Err(e)` where `e` encapsulates any error encountered during the operation.
    ///
    pub async fn delete_kv(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let response = self
            .node
            .query(Request::new(QueryRequest {
                from_id: 0,
                matched_digits: 0,
                hops: 0,
                query_type: QueryType::Delete.into(),
                key: Sha256Hasher::hash_once(key),
                value: None,
            }))
            .await?
            .into_inner();

        Ok(response.value)
    }
}
