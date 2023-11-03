use tonic::transport::Channel;

use crate::{
    error::*,
    internal::{
        dht::grpc::{NodeServiceClient, QueryRequest, QueryType},
        hring::hasher::Sha256Hasher,
    },
};

/// A client for Pastry nodes.
///
#[derive(Clone)]
pub struct PastryClient {
    client: NodeServiceClient<Channel>,
}

impl PastryClient {
    /// Connects to a node in the Pastry network.
    ///
    /// # Arguments
    ///
    /// * `address` - The public address of the node.
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing the client.
    ///
    pub async fn connect(address: &str) -> Result<Self> {
        Ok(PastryClient {
            client: NodeServiceClient::connect(address.to_owned()).await?,
        })
    }

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
    pub async fn get_kv(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let response = self
            .client
            .query(QueryRequest {
                from_id: 0,
                matched_digits: 0,
                query_type: QueryType::Get.into(),
                key: Sha256Hasher::hash_once(key),
                value: None,
            })
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
    pub async fn set_kv(&mut self, key: &[u8], value: &[u8]) -> Result<Option<Vec<u8>>> {
        let response = self
            .client
            .query(QueryRequest {
                from_id: 0,
                matched_digits: 0,
                query_type: QueryType::Set.into(),
                key: Sha256Hasher::hash_once(key),
                value: Some(value.to_vec()),
            })
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
    pub async fn delete_kv(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let response = self
            .client
            .query(QueryRequest {
                from_id: 0,
                matched_digits: 0,
                query_type: QueryType::Delete.into(),
                key: Sha256Hasher::hash_once(key),
                value: None,
            })
            .await?
            .into_inner();

        Ok(response.value)
    }
}
