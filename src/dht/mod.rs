mod node;
mod service;
mod store;
mod tests;

use self::node::Node;
use crate::{
    error::Result,
    hring::hasher::Sha256Hasher,
    pastry::shared::Config,
    rpc::node::{node_service_server::NodeService, QueryRequest, QueryType},
};
use tokio::task::JoinHandle;
use tonic::Request;

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
    /// * `hostname` - The Hostname to serve this node on.
    /// * `port` - The port to serve this node on.
    ///
    /// # Returns
    ///
    /// A Result containing the newly registered node.
    ///
    pub fn new(config: Config, hostname: &str, port: &str) -> Result<Self> {
        Ok(PastryNode {
            node: Node::new(config, hostname, port)?,
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
    /// A Result containing the JoinHandle for the server.
    ///
    pub async fn bootstrap_and_serve(
        self,
        bootstrap_addr: Option<&str>,
    ) -> Result<JoinHandle<Result<()>>> {
        self.node.bootstrap_and_serve(bootstrap_addr).await
    }

    pub fn get_id(&self) -> u64 {
        self.node.id
    }

    pub fn get_public_address(&self) -> String {
        self.node.pub_addr.clone()
    }
}

// gRPC methods
impl PastryNode {
    // pub async fn route(&self, key: &[u8]) -> Result<u64> {
    //     let response = self
    //         .node
    //         .query(Request::new(QueryRequest {
    //             from_id: 0,
    //             matched_digits: 0,
    //             query_type: QueryType::Get.into(),
    //             key: Sha256Hasher::hash_once(key),
    //             value: None,
    //         }))
    //         .await?
    //         .into_inner();
    //
    //     Ok(response.from_id)
    // }

    pub async fn get_kv(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let response = self
            .node
            .query(Request::new(QueryRequest {
                from_id: 0,
                matched_digits: 0,
                query_type: QueryType::Get.into(),
                key: Sha256Hasher::hash_once(key),
                value: None,
            }))
            .await?
            .into_inner();

        Ok(response.value)
    }

    pub async fn set_kv(&self, key: &[u8], value: &[u8]) -> Result<Option<Vec<u8>>> {
        let response = self
            .node
            .query(Request::new(QueryRequest {
                from_id: 0,
                matched_digits: 0,
                query_type: QueryType::Set.into(),
                key: Sha256Hasher::hash_once(key),
                value: Some(value.to_vec()),
            }))
            .await?
            .into_inner();

        Ok(response.value)
    }

    pub async fn delete_kv(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let response = self
            .node
            .query(Request::new(QueryRequest {
                from_id: 0,
                matched_digits: 0,
                query_type: QueryType::Delete.into(),
                key: Sha256Hasher::hash_once(key),
                value: None,
            }))
            .await?
            .into_inner();

        Ok(response.value)
    }
}
