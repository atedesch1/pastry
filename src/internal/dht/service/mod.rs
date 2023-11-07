pub mod join;
pub mod query;

use log::{info, warn};
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::{Request, Response, Status};

use super::{grpc::*, node::Node};

use crate::{
    error::*,
    internal::{
        dht::node::{NodeInfo, NodeState},
        hring::ring::*,
        util::{self, U64_HEX_NUM_OF_DIGITS},
    },
};

#[tonic::async_trait]
impl NodeService for Node {
    async fn get_node_state(
        &self,
        _request: Request<()>,
    ) -> std::result::Result<Response<GetNodeStateResponse>, Status> {
        info!("#{:016X}: Got request for get_node_state", self.id);
        self.block_until_routing_requests().await;

        Ok(Response::new(GetNodeStateResponse {
            id: self.id,
            leaf_set: self
                .state
                .data
                .read()
                .await
                .leaf
                .get_set()
                .iter()
                .map(|&e| e.clone().to_node_entry())
                .collect(),
        }))
    }

    async fn join(
        &self,
        request: Request<JoinRequest>,
    ) -> std::result::Result<Response<JoinResponse>, Status> {
        info!("#{:016X}: Got request for join", self.id);
        self.block_until_routing_requests().await;
        self.join_service(request.get_ref()).await
    }

    async fn query(
        &self,
        request: Request<QueryRequest>,
    ) -> std::result::Result<Response<QueryResponse>, Status> {
        info!("#{:016X}: Got request for query", self.id);
        self.block_until_routing_requests().await;
        self.query_service(request.get_ref()).await
    }

    type TransferKeysStream = UnboundedReceiverStream<std::result::Result<KeyValueEntry, Status>>;

    async fn transfer_keys(
        &self,
        request: Request<TransferKeysRequest>,
    ) -> std::result::Result<Response<Self::TransferKeysStream>, Status> {
        info!("#{:016X}: Got request for transfer_keys", self.id);
        self.block_until_routing_requests().await;
        self.transfer_keys_service(request.get_ref()).await
    }

    async fn announce_arrival(
        &self,
        request: Request<AnnounceArrivalRequest>,
    ) -> std::result::Result<Response<()>, Status> {
        info!("#{:016X}: Got request for announce_arrival", self.id);
        self.block_until_routing_requests().await;
        self.announce_arrival_service(request.get_ref()).await
    }

    async fn fix_leaf_set(
        &self,
        request: Request<FixLeafSetRequest>,
    ) -> std::result::Result<Response<()>, Status> {
        info!("#{:016X}: Got request for fix_leaf_set", self.id);
        self.block_until_routing_requests().await;

        let req = request.get_ref();

        if let None = self.state.data.read().await.leaf.get(req.id) {
            return Ok(Response::new(()));
        }

        let node = NodeInfo::new(req.id, &req.pub_addr);

        self.fix_leaf_entry(&node).await?;

        Ok(Response::new(()))
    }

    async fn get_node_table_entry(
        &self,
        request: Request<GetNodeTableEntryRequest>,
    ) -> std::result::Result<Response<GetNodeTableEntryResponse>, Status> {
        info!("#{:016X}: Got request for get_node_table_entry", self.id);
        self.block_until_routing_requests().await;

        let req = request.get_ref();

        let node = match self.state.data.read().await.table.get_row(req.row as usize) {
            Some(row) => row[req.column as usize].map(|node| node.clone().to_node_entry()),
            None => None,
        };

        Ok(Response::new(GetNodeTableEntryResponse { node }))
    }
}
