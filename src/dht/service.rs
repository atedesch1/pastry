use log::{debug, info};
use tonic::{Request, Response, Status};

use crate::{
    dht::node::{Node, NodeConnection, NodeInfo, NodeState},
    rpc::node::{
        node_service_server::NodeService, GetNodeIdResponse, GetNodeStateResponse, JoinRequest,
        JoinResponse, LeafSetEntry, LeaveRequest, QueryRequest, QueryResponse, RoutingTableEntry,
        UpdateNeighborsRequest,
    },
    util::U64_HEX_NUM_OF_DIGITS,
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
                .leaf
                .read()
                .await
                .get_set()
                .iter()
                .map(|e| LeafSetEntry {
                    id: e.value.info.id.clone(),
                    pub_addr: e.value.info.pub_addr.clone(),
                })
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

        let mut table_entries = req.table_entries.clone();

        // Append routing table entries from this node
        let table = self.state.table.read().await;
        // for i in req.matched_digits..U64_HEX_NUM_OF_DIGITS {
        for i in 0..U64_HEX_NUM_OF_DIGITS {
            match table.get_row(i as usize) {
                Some(row) => {
                    for entry in row {
                        if let Some(entry) = entry {
                            table_entries.push(RoutingTableEntry {
                                id: entry.value.id,
                                pub_addr: entry.value.pub_addr.clone(),
                            });
                        }
                    }
                }
                None => break,
            }
        }
        table_entries.push(RoutingTableEntry {
            id: self.id,
            pub_addr: self.pub_addr.clone(),
        });

        let conn = {
            let leaf = self.state.leaf.read().await;
            leaf.get(req.id).clone()
        };

        match conn {
            Some(conn) => {
                // Route using leaf set

                if conn.info.id != self.id {
                    // Forward to neighbor in leaf set
                    return conn
                        .client
                        .unwrap()
                        .join(Request::new(JoinRequest {
                            id: req.id,
                            pub_addr: req.pub_addr.clone(),
                            matched_digits: req.matched_digits,
                            table_entries,
                        }))
                        .await;
                }
                // Current node is closest previous to joining node

                let leaf = self.state.leaf.read().await;
                let leaf_set = {
                    let mut set = leaf.get_set().clone();

                    // Remove left most neighbor if leaf set is full
                    if leaf.is_full() {
                        set.remove(leaf.get_first_index().unwrap());
                    }

                    set.iter()
                        .map(|e| LeafSetEntry {
                            id: e.value.info.id.clone(),
                            pub_addr: e.value.info.pub_addr.clone(),
                        })
                        .collect()
                };

                Ok(Response::new(JoinResponse {
                    id: self.id,
                    pub_addr: self.pub_addr.clone(),
                    leaf_set,
                    routing_table: table_entries,
                }))
            }
            None => {
                let (mut client, matched_digits) = {
                    match self
                        .state
                        .table
                        .read()
                        .await
                        .route(req.id, req.matched_digits as usize + 1)?
                    {
                        // Forward request using routing table
                        Some((info, matched)) => {
                            (Node::connect_with_retry(&info.pub_addr).await?, matched)
                        }
                        // Forward request using closest leaf node
                        None => {
                            let (closest, matched) =
                                self.state.leaf.read().await.get_closest(req.id)?;
                            (closest.client.unwrap(), matched)
                        }
                    }
                };

                client
                    .join(Request::new(JoinRequest {
                        id: req.id,
                        pub_addr: req.pub_addr.clone(),
                        matched_digits: matched_digits as u32,
                        table_entries,
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

        let conn = {
            let leaf = self.state.leaf.read().await;
            leaf.get(req.key).clone()
        };

        if let Some(conn) = conn {
            if conn.info.id != self.id {
                // Forward request using leaf set
                return conn.client.unwrap().query(request).await;
            }

            // Node is the owner of key
            return Ok(Response::new(QueryResponse { id: self.id }));
        }

        let table = self.state.table.read().await;
        let route_result = table.route(req.key, req.matched_digits as usize + 1)?;
        let (mut client, matched_digits) = {
            match route_result {
                // Forward request using routing table
                Some((info, matched)) => (Node::connect_with_retry(&info.pub_addr).await?, matched),
                // Forward request using closest leaf node
                None => {
                    let (closest, matched) = self.state.leaf.read().await.get_closest(req.key)?;
                    (closest.client.unwrap(), matched)
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

    async fn update_neighbors(
        &self,
        request: Request<UpdateNeighborsRequest>,
    ) -> std::result::Result<Response<()>, Status> {
        info!("#{:X}: Got request for update_neighbors", self.id);
        self.block_until_routing_requests().await;
        self.change_state(NodeState::UpdatingConnections).await;

        let req = request.get_ref();

        let mut leaf = self.state.leaf.write().await;
        let mut table = self.state.table.write().await;

        if let Some(entry) = leaf.get(req.id) {
            if entry.info.id != req.id {
                // debug!("#{:X}: Attempting to connect to #{:X}", self.id, req.id);
                // debug!("#{:X}: leaf - \n{}", self.id, leaf);
                // debug!("#{:X}: table - \n{}", self.id, table);
                let client = Node::connect_with_retry(&req.pub_addr).await?;
                leaf.insert(
                    req.id,
                    NodeConnection {
                        info: NodeInfo {
                            id: req.id,
                            pub_addr: req.pub_addr.clone(),
                        },
                        client: Some(client),
                    },
                )?;
            }
        }

        table.insert(
            req.id,
            NodeInfo {
                id: req.id,
                pub_addr: req.pub_addr.clone(),
            },
        )?;

        for entry in &req.leaf_set {
            table.insert(
                entry.id,
                NodeInfo {
                    id: entry.id,
                    pub_addr: entry.pub_addr.clone(),
                },
            )?;
        }

        for entry in &req.routing_table {
            table.insert(
                entry.id,
                NodeInfo {
                    id: entry.id,
                    pub_addr: entry.pub_addr.clone(),
                },
            )?;
        }

        debug!("#{:X}: Leaf set updated: \n{}", self.id, leaf);
        debug!("#{:X}: Routing table updated: \n{}", self.id, table);

        self.change_state(NodeState::RoutingRequests).await;
        Ok(Response::new(()))
    }
}
