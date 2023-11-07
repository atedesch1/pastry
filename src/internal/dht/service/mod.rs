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
    async fn get_node_id(
        &self,
        _request: Request<()>,
    ) -> std::result::Result<Response<GetNodeIdResponse>, Status> {
        info!("#{:016X}: Got request for get_node_id", self.id);

        Ok(Response::new(GetNodeIdResponse { id: self.id }))
    }

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

    async fn leave(
        &self,
        request: Request<LeaveRequest>,
    ) -> std::result::Result<Response<()>, Status> {
        info!("#{:016X}: Got request for leave", self.id);
        self.block_until_routing_requests().await;

        todo!()
    }

    type TransferKeysStream = UnboundedReceiverStream<std::result::Result<KeyValueEntry, Status>>;

    async fn transfer_keys(
        &self,
        request: Request<TransferKeysRequest>,
    ) -> std::result::Result<Response<Self::TransferKeysStream>, Status> {
        let prev_id = self.id;
        let node_id = request.get_ref().id;

        let (tx, rx) = mpsc::unbounded_channel();

        let state = self.state.clone();

        tokio::spawn(async move {
            let mut store = state.store.write().await;
            let entries = store
                .get_entries(|key| !Ring64::is_in_range(prev_id, node_id, key))
                .await
                .iter()
                .map(|e| (e.0.clone(), e.1.clone()))
                .collect::<Vec<(u64, Vec<u8>)>>();

            info!("#{:016X}: Transferring keys to #{:016X}", prev_id, node_id);

            for (key, value) in &entries {
                // TODO: implement retry logic
                match tx.send(Ok(KeyValueEntry {
                    key: key.clone(),
                    value: value.clone(),
                })) {
                    Ok(_) => {
                        store.delete(key);
                    }
                    Err(err) => {
                        warn!(
                            "#{:016X}: Could not transfer key {:016X} to #{:016X}: {}",
                            prev_id, key, node_id, err
                        );
                    }
                };
            }
        });

        Ok(Response::new(UnboundedReceiverStream::new(rx)))
    }

    async fn announce_arrival(
        &self,
        request: Request<AnnounceArrivalRequest>,
    ) -> std::result::Result<Response<()>, Status> {
        info!("#{:016X}: Got request for announce_arrival", self.id);
        self.block_until_routing_requests().await;
        self.change_state(NodeState::UpdatingConnections).await;

        let req = request.get_ref();

        let mut data = self.state.data.write().await;

        let node_entry = NodeEntry {
            id: req.id,
            pub_addr: req.pub_addr.clone(),
        };

        if let Some(entry) = data.leaf.get(req.id) {
            if entry.id != req.id {
                self.update_leaf_set(&mut data, &node_entry).await?;
            }
        }

        self.update_routing_table(&mut data, &node_entry).await?;

        self.change_state(NodeState::RoutingRequests).await;
        Ok(Response::new(()))
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

// Helper functions
impl Node {
    async fn connect_and_join(
        &self,
        node: &NodeInfo,
        request: JoinRequest,
    ) -> Result<Response<JoinResponse>> {
        match NodeServiceClient::connect(node.pub_addr.to_owned()).await {
            Ok(mut client) => Ok(client.join(request.clone()).await?),
            Err(err) => Err(err.into()),
        }
    }

    async fn connect_and_query(
        &self,
        node: &NodeInfo,
        request: QueryRequest,
    ) -> Result<Response<QueryResponse>> {
        match NodeServiceClient::connect(node.pub_addr.to_owned()).await {
            Ok(mut client) => Ok(client.query(request.clone()).await?),
            Err(err) => Err(err.into()),
        }
    }
}
