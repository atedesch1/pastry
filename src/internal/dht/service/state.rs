use tonic::{Response, Status};

use super::super::{grpc::*, node::Node};

use crate::internal::dht::node::NodeInfo;

impl Node {
    pub async fn get_node_state_service(
        &self,
    ) -> std::result::Result<Response<GetNodeStateResponse>, Status> {
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

    pub async fn get_node_table_entry_service(
        &self,
        req: &GetNodeTableEntryRequest,
    ) -> std::result::Result<Response<GetNodeTableEntryResponse>, Status> {
        let node = match self.state.data.read().await.table.get_row(req.row as usize) {
            Some(row) => row[req.column as usize].map(|node| node.clone().to_node_entry()),
            None => None,
        };

        Ok(Response::new(GetNodeTableEntryResponse { node }))
    }

    pub async fn fix_leaf_set_service(
        &self,
        req: &FixLeafSetRequest,
    ) -> std::result::Result<Response<()>, Status> {
        if let None = self.state.data.read().await.leaf.get(req.id) {
            return Ok(Response::new(()));
        }

        let node = NodeInfo::new(req.id, &req.pub_addr);

        self.fix_leaf_entry(&node).await?;

        Ok(Response::new(()))
    }
}
