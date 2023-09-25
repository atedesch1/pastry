use log::info;
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
        info!("#{:x}: Got request for get_node_id", self.id);
        self.block_until_connected().await;

        Ok(Response::new(GetNodeIdResponse { id: self.id }))
    }

    async fn get_node_state(
        &self,
        _request: Request<()>,
    ) -> std::result::Result<Response<GetNodeStateResponse>, Status> {
        info!("#{:x}: Got request for get_node_state", self.id);
        self.block_until_connected().await;

        Ok(Response::new(GetNodeStateResponse {
            id: self.id,
            leaf_set: self
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
        info!("#{:x}: Got request for join", self.id);
        self.block_until_connected().await;

        let req = request.into_inner();

        let conn = {
            let leaf = self.leaf.read().await;
            leaf.get(req.id).clone()
        };

        match conn {
            Some(conn) => {
                // Route using leaf set

                if conn.info.id != self.id {
                    // Forward to neighbor in leaf set
                    return conn.client.unwrap().join(Request::new(req)).await;
                }
                // Current node is closest previous to joining node

                let leaf = self.leaf.read().await;
                let leaf_set = {
                    let mut set = leaf.get_set();

                    // Remove first element if leaf set is full
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
                }))
            }
            None => {
                // Route using routing table
                // let mut client = {
                //     let table = self.table.read().await;
                //     let info = table.route(req.id, 0)?;
                //     NodeServiceClient::connect(info.pub_addr.clone())
                //         .await
                //         .unwrap()
                // };
                // client.join(Request::new(req)).await

                // Remove after routing table is implemented
                let conn = {
                    let leaf = self.leaf.read().await;
                    leaf.get_right_neighbor().clone()
                };
                conn.unwrap().client.unwrap().join(Request::new(req)).await
            }
        }
    }

    async fn leave(
        &self,
        request: Request<LeaveRequest>,
    ) -> std::result::Result<Response<()>, Status> {
        info!("#{:x}: Got request for leave", self.id);
        self.block_until_connected().await;

        todo!()
    }

    async fn query(
        &self,
        request: Request<QueryRequest>,
    ) -> std::result::Result<Response<QueryResponse>, Status> {
        info!("#{:x}: Got request for query", self.id);
        self.block_until_connected().await;

        let req = request.into_inner();

        let conn = {
            let leaf = self.leaf.read().await;
            leaf.get(req.key).clone()
        };

        if let Some(conn) = conn {
            if conn.info.id != self.id {
                // Forward request using leaf set
                return conn.client.unwrap().query(Request::new(req)).await;
            }

            // Node is the owner of key
            return Ok(Response::new(QueryResponse { id: self.id }));
        }

        // Forward request using routing table
        // let mut client = {
        //     let table = self.table.read().await;
        //     let info = table.route(req.key, 0)?;
        //     NodeServiceClient::connect(info.pub_addr.clone())
        //         .await
        //         .unwrap()
        // };
        // client.query(Request::new(req)).await

        // Remove after routing table is implemented
        let conn = {
            let leaf = self.leaf.read().await;
            leaf.get_right_neighbor().clone()
        };
        conn.unwrap().client.unwrap().query(Request::new(req)).await
    }

    async fn update_neighbors(
        &self,
        request: Request<UpdateNeighborsRequest>,
    ) -> std::result::Result<Response<()>, Status> {
        info!("#{:x}: Got request for update_neighbors", self.id);
        self.block_until_connected().await;

        let req = request.into_inner();

        let mut leaf = self.leaf.write().await;

        if let Some(conn) = leaf.get(req.id) {
            if conn.info.id == self.id {
                // Is closest previous neighbor
            }

            // Register as neighbor
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
        }

        Ok(Response::new(()))
    }
}
