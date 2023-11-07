use tonic::{Response, Status};

use super::super::{grpc::*, node::Node};

use crate::{
    error::*,
    internal::{
        dht::node::NodeInfo,
        util::{self, U64_HEX_NUM_OF_DIGITS},
    },
};

impl Node {
    pub async fn join_service(
        &self,
        req: &JoinRequest,
    ) -> std::result::Result<Response<JoinResponse>, Status> {
        let mut routing_table = req.routing_table.clone();

        // Append routing table entries from this node
        {
            let data = self.state.data.read().await;
            for i in req.matched_digits..U64_HEX_NUM_OF_DIGITS {
                match data.table.get_row(i as usize) {
                    Some(row) => {
                        for entry in row {
                            if let Some(entry) = entry {
                                if let Err(position) =
                                    routing_table.binary_search_by(|e| e.id.cmp(&entry.id))
                                {
                                    routing_table.insert(position, entry.clone().to_node_entry());
                                };
                            }
                        }
                    }
                    None => break,
                }
            }
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
                    hops: req.hops,
                    leaf_set,
                    routing_table,
                }));
            }
        }

        let mut request = req.clone();
        request.routing_table = routing_table;
        request.matched_digits = util::get_num_matched_digits(self.id, req.id)?;
        request.hops += 1;

        if let Some(res) = self.join_with_leaf_set(&request).await? {
            return Ok(res);
        }

        if let Some(res) = self.join_with_routing_table(&request).await? {
            return Ok(res);
        }

        self.join_with_closest_from_leaf_set(&request).await
    }

    // JOIN
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

    async fn join_with_leaf_set(
        &self,
        request: &JoinRequest,
    ) -> std::result::Result<Option<Response<JoinResponse>>, Status> {
        loop {
            let node = match self.route_with_leaf_set(request.id).await {
                Some(node) => node,
                None => break Ok(None),
            };

            match self.connect_and_join(&node, request.clone()).await {
                Ok(r) => break Ok(Some(r)),
                Err(err) => self.warn_and_fix_leaf_entry(&node, &err.to_string()).await,
            }
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

        match self.connect_and_join(&node, request.clone()).await {
            Ok(r) => return Ok(Some(r)),
            Err(err) => self.warn_and_fix_table_entry(&node, &err.to_string()).await,
        }

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

            match self.connect_and_join(&node, request.clone()).await {
                Ok(r) => break Ok(r),
                Err(err) => self.warn_and_fix_leaf_entry(&node, &err.to_string()).await,
            }
        }
    }
}
