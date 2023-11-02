use log::{info, warn};
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::{Request, Response, Status};

use crate::{
    dht::node::{Node, NodeState},
    error::Error,
    hring::ring::{Ring, Ring64},
    rpc::node::{
        node_service_client::NodeServiceClient, node_service_server::NodeService,
        AnnounceArrivalRequest, FixLeafSetRequest, GetNodeIdResponse, GetNodeStateResponse,
        GetNodeTableEntryRequest, GetNodeTableEntryResponse, JoinRequest, JoinResponse,
        KeyValueEntry, LeaveRequest, NodeEntry, QueryError, QueryRequest, QueryResponse, QueryType,
        TransferKeysRequest,
    },
    util::{self, U64_HEX_NUM_OF_DIGITS},
};

use super::node::NodeInfo;

#[tonic::async_trait]
impl NodeService for super::node::Node {
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

        let req = request.get_ref();

        let mut routing_table = req.routing_table.clone();

        // Append routing table entries from this node
        {
            let data = self.state.data.read().await;
            for i in req.matched_digits..U64_HEX_NUM_OF_DIGITS {
                match data.table.get_row(i as usize) {
                    Some(row) => {
                        for entry in row {
                            if let Some(entry) = entry {
                                routing_table.push(entry.clone().to_node_entry());
                            }
                        }
                    }
                    None => break,
                }
            }
            routing_table.push(self.get_info().to_node_entry());
        }

        if let Some(node) = self.route_with_leaf_set(req.id).await {
            if node.id == self.id {
                // Current node is closest previous to joining node
                let data = self.state.data.read().await;
                let leaf_set = {
                    let mut leaf = data.leaf.clone();

                    // Remove left most neighbor if leaf set is full
                    if leaf.is_full() {
                        leaf.remove(
                            data.leaf
                                .get_furthest_counter_clockwise_neighbor()
                                .unwrap()
                                .id,
                        )?;
                    }

                    leaf.get_set()
                        .iter()
                        .map(|&e| e.clone().to_node_entry())
                        .collect()
                };

                return Ok(Response::new(JoinResponse {
                    id: self.id,
                    pub_addr: self.pub_addr.to_string(),
                    leaf_set,
                    routing_table,
                }));
            }
        }

        let mut request = req.clone();
        request.routing_table = routing_table;
        request.matched_digits = util::get_num_matched_digits(self.id, req.id)?;

        if let Some(res) = self.join_with_leaf_set(&request).await? {
            return Ok(res);
        }

        if let Some(res) = self.join_with_routing_table(&request).await? {
            return Ok(res);
        }

        self.join_with_closest_from_leaf_set(&request).await
    }

    async fn leave(
        &self,
        request: Request<LeaveRequest>,
    ) -> std::result::Result<Response<()>, Status> {
        info!("#{:016X}: Got request for leave", self.id);
        self.block_until_routing_requests().await;

        todo!()
    }

    async fn query(
        &self,
        request: Request<QueryRequest>,
    ) -> std::result::Result<Response<QueryResponse>, Status> {
        info!("#{:016X}: Got request for query", self.id);
        self.block_until_routing_requests().await;

        let req = request.get_ref();

        if let Some(node) = self.route_with_leaf_set(req.key).await {
            if node.id == self.id {
                // Node is the owner of key
                return match self.execute_query(req).await {
                    Ok(value) => Ok(Response::new(QueryResponse {
                        from_id: self.id,
                        key: req.key,
                        value,
                        error: None,
                    })),
                    Err(err) => {
                        warn!("#{:016X}: Query error: {}", self.id, err);

                        Ok(Response::new(QueryResponse {
                            from_id: self.id,
                            key: req.key,
                            value: None,
                            error: Some(
                                match QueryType::from_i32(req.query_type).unwrap() {
                                    QueryType::Set => QueryError::ValueNotProvided,
                                    QueryType::Get | QueryType::Delete => QueryError::KeyNotFound,
                                }
                                .into(),
                            ),
                        }))
                    }
                };
            }
        }

        let mut request = req.clone();
        request.from_id = self.id;
        request.matched_digits = util::get_num_matched_digits(self.id, req.key)?;

        if let Some(res) = self.query_with_leaf_set(&request).await? {
            return Ok(res);
        }

        if let Some(res) = self.query_with_routing_table(&request).await? {
            return Ok(res);
        }

        self.query_with_closest_from_leaf_set(&request).await
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

        if let Some(entry) = data.leaf.get(req.id) {
            if entry.id != req.id {
                self.update_leaf_set(
                    &mut data,
                    &NodeEntry {
                        id: req.id,
                        pub_addr: req.pub_addr.clone(),
                    },
                )
                .await?;
            }
        }

        self.update_routing_table(
            &mut data,
            &NodeEntry {
                id: req.id,
                pub_addr: req.pub_addr.clone(),
            },
        )
        .await?;

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

        let node = NodeInfo {
            id: req.id,
            pub_addr: req.pub_addr.clone(),
        };

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
    // JOIN
    async fn join_with_leaf_set(
        &self,
        request: &JoinRequest,
    ) -> std::result::Result<Option<Response<JoinResponse>>, Status> {
        loop {
            let node = match self.route_with_leaf_set(request.id).await {
                Some(node) => node,
                None => return Ok(None),
            };

            // Forward to neighbor in leaf set
            let err: Error = match NodeServiceClient::connect(node.pub_addr.to_owned()).await {
                Ok(mut client) => match client.join(request.clone()).await {
                    Ok(r) => return Ok(Some(r)),
                    Err(err) => err.into(),
                },
                Err(err) => err.into(),
            };

            self.warn_and_fix_leaf_entry(&node, &err.to_string()).await;
        }
    }

    async fn join_with_routing_table(
        &self,
        request: &JoinRequest,
    ) -> std::result::Result<Option<Response<JoinResponse>>, Status> {
        let (node, _) = match self
            .route_with_routing_table(request.id, request.matched_digits as usize)
            .await
        {
            Some(res) => res,
            None => return Ok(None),
        };

        if node.id == self.id {
            return Ok(None);
        }

        let err: Error = match NodeServiceClient::connect(node.pub_addr.to_owned()).await {
            Ok(mut client) => match client.join(request.clone()).await {
                Ok(r) => return Ok(Some(r)),
                Err(err) => err.into(),
            },
            Err(err) => err.into(),
        };

        self.warn_and_fix_table_entry(&node, &err.to_string()).await;

        Ok(None)
    }

    async fn join_with_closest_from_leaf_set(
        &self,
        request: &JoinRequest,
    ) -> std::result::Result<Response<JoinResponse>, Status> {
        loop {
            let (node, _) = self.get_closest_from_leaf_set(request.id).await;

            if node.id == self.id {
                panic!("#{:016X}: Could not route join request", self.id);
            }

            let err: Error = match NodeServiceClient::connect(node.pub_addr.to_owned()).await {
                Ok(mut client) => match client.join(request.clone()).await {
                    Ok(r) => return Ok(r),
                    Err(err) => err.into(),
                },
                Err(err) => err.into(),
            };

            self.warn_and_fix_leaf_entry(&node, &err.to_string()).await;
        }
    }

    // QUERY
    async fn query_with_leaf_set(
        &self,
        request: &QueryRequest,
    ) -> std::result::Result<Option<Response<QueryResponse>>, Status> {
        loop {
            let node = match self.route_with_leaf_set(request.key).await {
                Some(node) => node,
                None => return Ok(None),
            };

            // Forward query to neighbor in leaf set
            let err: Error = match NodeServiceClient::connect(node.pub_addr.to_owned()).await {
                Ok(mut client) => match client.query(request.clone()).await {
                    Ok(r) => return Ok(Some(r)),
                    Err(err) => err.into(),
                },
                Err(err) => err.into(),
            };

            self.warn_and_fix_leaf_entry(&node, &err.to_string()).await;
        }
    }

    async fn query_with_routing_table(
        &self,
        request: &QueryRequest,
    ) -> std::result::Result<Option<Response<QueryResponse>>, Status> {
        let (node, _) = match self
            .route_with_routing_table(request.key, request.matched_digits as usize + 1)
            .await
        {
            Some(res) => res,
            None => return Ok(None),
        };

        let err: Error = match NodeServiceClient::connect(node.pub_addr.to_owned()).await {
            Ok(mut client) => match client.query(request.clone()).await {
                Ok(r) => return Ok(Some(r)),
                Err(err) => err.into(),
            },
            Err(err) => err.into(),
        };

        self.warn_and_fix_table_entry(&node, &err.to_string()).await;

        Ok(None)
    }

    async fn query_with_closest_from_leaf_set(
        &self,
        request: &QueryRequest,
    ) -> std::result::Result<Response<QueryResponse>, Status> {
        loop {
            let (node, _) = self.get_closest_from_leaf_set(request.key).await;

            let err: Error = match NodeServiceClient::connect(node.pub_addr.to_owned()).await {
                Ok(mut client) => match client.query(request.clone()).await {
                    Ok(r) => return Ok(r),
                    Err(err) => err.into(),
                },
                Err(err) => err.into(),
            };

            self.warn_and_fix_leaf_entry(&node, &err.to_string()).await;
        }
    }
}
