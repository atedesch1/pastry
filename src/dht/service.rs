use tonic::{Request, Response, Status};

use crate::{
    dht::node::{NodeConnection, NodeInfo},
    rpc::node::{
        node_service_client::NodeServiceClient, node_service_server::NodeService,
        GetNodeIdResponse, GetNodeStateResponse, JoinRequest, JoinResponse, LeafSetEntry,
        LeaveRequest, QueryRequest, QueryResponse, UpdateNeighborsRequest,
    },
};

#[tonic::async_trait]
impl NodeService for super::node::Node {
    async fn get_node_id(
        &self,
        _request: Request<()>,
    ) -> std::result::Result<Response<GetNodeIdResponse>, Status> {
        self.block_until_connected().await;

        Ok(Response::new(GetNodeIdResponse { id: self.id }))
    }

    async fn get_node_state(
        &self,
        _request: Request<()>,
    ) -> std::result::Result<Response<GetNodeStateResponse>, Status> {
        self.block_until_connected().await;

        let leaf = self.leaf.read().await;

        Ok(Response::new(GetNodeStateResponse {
            id: self.id,
            leaf_set: leaf
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
        self.block_until_connected().await;

        let req = request.into_inner();

        let conn = {
            let leaf = self.leaf.read().await;
            leaf.get(req.id).clone()
        };

        if let Some(conn) = conn {
            if conn.info.id != self.id {
                // Forward to node in leaf set
                return conn.client.unwrap().join(Request::new(req.clone())).await;
            }
            // Current node is previous to joining node
            let mut leaf = self.leaf.write().await;

            // Construct joining node's leaf set
            let mut leaf_set: Vec<LeafSetEntry> = leaf
                .get_set()
                .iter()
                .map(|e| LeafSetEntry {
                    id: e.value.info.id.clone(),
                    pub_addr: e.value.info.pub_addr.clone(),
                })
                .collect();

            // Remove the first node connection
            if leaf.is_full() {
                leaf_set.remove(leaf.get_first_index().unwrap());
            }

            // Insert as neighbor
            leaf.insert(
                req.id,
                NodeConnection {
                    info: NodeInfo {
                        id: req.id,
                        pub_addr: req.pub_addr.clone(),
                    },
                    client: Some(
                        NodeServiceClient::connect(req.pub_addr.clone())
                            .await
                            .unwrap(),
                    ),
                },
            )?;

            return Ok(Response::new(JoinResponse {
                id: self.id,
                pub_addr: self.pub_addr.clone(),
                leaf_set,
            }));
        }

        // Forward request using routing table
        let mut client = {
            let table = self.table.read().await;
            let info = table.route(req.id, 0)?;
            NodeServiceClient::connect(info.pub_addr.clone())
                .await
                .unwrap()
        };
        client.join(Request::new(req.clone())).await
    }

    async fn leave(
        &self,
        request: Request<LeaveRequest>,
    ) -> std::result::Result<Response<()>, Status> {
        self.block_until_connected().await;

        todo!()
    }

    async fn query(
        &self,
        request: Request<QueryRequest>,
    ) -> std::result::Result<Response<QueryResponse>, Status> {
        self.block_until_connected().await;

        let req = request.into_inner();

        let conn = {
            let leaf = self.leaf.read().await;
            leaf.get(req.key).clone()
        };

        if let Some(conn) = conn {
            if conn.info.id != self.id {
                // Forward request using leaf set
                return conn.client.unwrap().query(Request::new(req.clone())).await;
            }

            // Node is the owner of key
            return Ok(Response::new(QueryResponse { id: self.id }));
        }

        // Forward request using routing table
        let mut client = {
            let table = self.table.read().await;
            let info = table.route(req.key, 0)?;
            NodeServiceClient::connect(info.pub_addr.clone())
                .await
                .unwrap()
        };
        client.query(Request::new(req.clone())).await
    }

    async fn update_neighbors(
        &self,
        request: Request<UpdateNeighborsRequest>,
    ) -> std::result::Result<Response<()>, Status> {
        self.block_until_connected().await;

        let req = request.into_inner();

        let conn = {
            let leaf = self.leaf.read().await;
            leaf.get(req.id).clone()
        };

        if let Some(conn) = conn {
            if conn.info.id != req.id {
                let mut leaf = self.leaf.write().await;

                // Insert as neighbor
                leaf.insert(
                    req.id,
                    NodeConnection {
                        info: NodeInfo {
                            id: req.id,
                            pub_addr: req.pub_addr.clone(),
                        },
                        client: Some(
                            NodeServiceClient::connect(req.pub_addr.clone())
                                .await
                                .unwrap(),
                        ),
                    },
                )?;

                println!(
                    "Node #{} updated leaf set: {:?}",
                    self.id,
                    leaf.get_set()
                        .iter()
                        .map(|e| e.key.clone())
                        .collect::<Vec<u64>>()
                );
            }
        }

        Ok(Response::new(()))
    }
}
