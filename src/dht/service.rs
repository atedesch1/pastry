use log::info;
use tonic::{Request, Response, Status};

use crate::{
    dht::node::{NodeConnection, NodeInfo},
    rpc::node::{
        node_service_client::NodeServiceClient, node_service_server::NodeService,
        GetNodeIdResponse, JoinRequest, JoinResponse, LeafSetEntry, LeaveRequest, QueryRequest,
        QueryResponse,
    },
};

#[tonic::async_trait]
impl NodeService for super::node::Node {
    async fn get_node_id(
        &self,
        _request: Request<()>,
    ) -> std::result::Result<Response<GetNodeIdResponse>, Status> {
        {
            let _ = self.is_connected.lock().await;
        }
        Ok(Response::new(GetNodeIdResponse { id: self.id }))
    }

    async fn join(
        &self,
        request: Request<JoinRequest>,
    ) -> std::result::Result<Response<JoinResponse>, Status> {
        {
            let _ = self.is_connected.lock().await;
        }

        let req = request.into_inner();
        // Check if joining node would be next neighbor (falls in between itself and next node)
        // If found -> add to leaf set, return cloned leaf set and routing table

        info!("Node #{:x} got request {:?}", self.id, req);

        let conn = {
            let leaf = self.leaf.read().await;
            leaf.get(req.id).clone()
        };

        info!(
            "Node #{:x} found {:?} responsible for #{:x}",
            self.id, conn, req.id
        );

        if let Some(conn) = conn {
            if conn.info.id != self.id {
                // If not neighbor -> forward request to responsible node
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

            info!("Node #{:x} Leafset after join {:?}", self.id, leaf);

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
        {
            let _ = self.is_connected.lock().await;
        }
        todo!()
    }

    async fn query(
        &self,
        request: Request<QueryRequest>,
    ) -> std::result::Result<Response<QueryResponse>, Status> {
        {
            let _ = self.is_connected.lock().await;
        }
        // Check if key belongs to node
        // If found -> execute query and return

        // If not -> check leaf set for key owners
        // If found -> forward request

        // If not -> forward request using routing table
        todo!()
    }
}
