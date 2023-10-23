use log::info;
use tonic::{Request, Response, Status};

use crate::{
    dht::node::{Node, NodeState},
    rpc::node::{
        node_service_server::NodeService, AnnounceArrivalRequest, GetNodeIdResponse,
        GetNodeStateResponse, JoinRequest, JoinResponse, LeaveRequest, NodeEntry, QueryRequest,
        QueryResponse,
    },
    util::{self, U64_HEX_NUM_OF_DIGITS},
};

#[tonic::async_trait]
impl NodeService for super::node::Node {
    async fn get_node_id(
        &self,
        _request: Request<()>,
    ) -> std::result::Result<Response<GetNodeIdResponse>, Status> {
        info!("#{:X}: Got request for get_node_id", self.id);

        Ok(Response::new(GetNodeIdResponse { id: self.id }))
    }

    async fn get_node_state(
        &self,
        _request: Request<()>,
    ) -> std::result::Result<Response<GetNodeStateResponse>, Status> {
        info!("#{:X}: Got request for get_node_state", self.id);
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
                .map(|e| e.value.clone().to_node_entry())
                .collect(),
        }))
    }

    async fn join(
        &self,
        request: Request<JoinRequest>,
    ) -> std::result::Result<Response<JoinResponse>, Status> {
        info!("#{:X}: Got request for join", self.id);
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
                                routing_table.push(entry.value.clone().to_node_entry());
                            }
                        }
                    }
                    None => break,
                }
            }
        }
        routing_table.push(self.get_info().to_node_entry());

        let node = self.state.data.read().await.leaf.get(req.id).clone();

        match node {
            Some(node) => {
                // Route using leaf set

                if node.id != self.id {
                    // Forward to neighbor in leaf set
                    let mut client = Node::connect_with_retry(&node.pub_addr).await?;
                    return client
                        .join(Request::new(JoinRequest {
                            id: req.id,
                            pub_addr: req.pub_addr.clone(),
                            matched_digits: util::get_num_matched_digits(node.id, req.id)?,
                            routing_table,
                        }))
                        .await;
                }
                // Current node is closest previous to joining node

                let data = self.state.data.read().await;
                let leaf_set = {
                    let mut set = data.leaf.get_set().clone();

                    // Remove left most neighbor if leaf set is full
                    if data.leaf.is_full() {
                        set.remove(data.leaf.get_first_index().unwrap());
                    }

                    set.iter()
                        .map(|e| e.value.clone().to_node_entry())
                        .collect()
                };

                Ok(Response::new(JoinResponse {
                    id: self.id,
                    pub_addr: self.pub_addr.clone(),
                    leaf_set,
                    routing_table,
                }))
            }
            None => {
                let (mut client, matched_digits) = {
                    let data = self.state.data.read().await;
                    match data.table.route(req.id, req.matched_digits as usize + 1)? {
                        // Forward request using routing table
                        Some((info, matched)) => {
                            (Node::connect_with_retry(&info.pub_addr).await?, matched)
                        }
                        // Forward request using closest leaf node
                        None => {
                            let (closest, matched) = data.leaf.get_closest(req.id)?;
                            (Node::connect_with_retry(&closest.pub_addr).await?, matched)
                        }
                    }
                };

                client
                    .join(Request::new(JoinRequest {
                        id: req.id,
                        pub_addr: req.pub_addr.clone(),
                        matched_digits: matched_digits as u32,
                        routing_table,
                    }))
                    .await
            }
        }
    }

    async fn leave(
        &self,
        request: Request<LeaveRequest>,
    ) -> std::result::Result<Response<()>, Status> {
        info!("#{:X}: Got request for leave", self.id);
        self.block_until_routing_requests().await;

        todo!()
    }

    async fn query(
        &self,
        request: Request<QueryRequest>,
    ) -> std::result::Result<Response<QueryResponse>, Status> {
        info!("#{:X}: Got request for query", self.id);
        self.block_until_routing_requests().await;

        let req = request.get_ref();

        let node = self.state.data.read().await.leaf.get(req.key).clone();

        if let Some(node) = node {
            if node.id != self.id {
                // Forward request using leaf set
                let mut client = Node::connect_with_retry(&node.pub_addr).await?;
                return client
                    .query(QueryRequest {
                        from_id: self.id,
                        key: req.key,
                        matched_digits: util::get_num_matched_digits(node.id, req.key)?,
                    })
                    .await;
            }

            // Node is the owner of key
            return Ok(Response::new(QueryResponse { id: self.id }));
        }

        let (mut client, matched_digits) = {
            let data = self.state.data.read().await;
            match data.table.route(req.key, req.matched_digits as usize + 1)? {
                // Forward request using routing table
                Some((info, matched)) => (Node::connect_with_retry(&info.pub_addr).await?, matched),
                // Forward request using closest leaf node
                None => {
                    let (closest, matched) = data.leaf.get_closest(req.key)?;
                    (Node::connect_with_retry(&closest.pub_addr).await?, matched)
                }
            }
        };

        client
            .query(Request::new(QueryRequest {
                from_id: self.id,
                matched_digits: matched_digits as u32,
                key: req.key,
            }))
            .await
    }

    async fn announce_arrival(
        &self,
        request: Request<AnnounceArrivalRequest>,
    ) -> std::result::Result<Response<()>, Status> {
        info!("#{:X}: Got request for announce_arrival", self.id);
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
}
