pub mod fail;
pub mod grpc;
pub mod join;
pub mod query;
pub mod state;

use log::info;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::{Request, Response, Status};

use super::node::Node;
use grpc::*;

#[tonic::async_trait]
impl NodeService for Node {
    // INFO
    async fn get_node_state(
        &self,
        _request: Request<()>,
    ) -> std::result::Result<Response<GetNodeStateResponse>, Status> {
        info!("#{:016X}: Got request for get_node_state", self.id);
        self.block_until_routing_requests().await;
        self.get_node_state_service().await
    }

    async fn get_node_table_entry(
        &self,
        request: Request<GetNodeTableEntryRequest>,
    ) -> std::result::Result<Response<GetNodeTableEntryResponse>, Status> {
        info!("#{:016X}: Got request for get_node_table_entry", self.id);
        self.block_until_routing_requests().await;
        self.get_node_table_entry_service(request.get_ref()).await
    }

    // MAIN
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

    // UPDATE
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
        self.fix_leaf_set_service(request.get_ref()).await
    }
}
