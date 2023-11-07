use log::{info, warn};
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::{Response, Status};

use super::super::node::Node;
use super::grpc::*;

use crate::{
    error::*,
    internal::{
        dht::node::{NodeInfo, NodeState},
        hring::ring::*,
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

    pub async fn announce_arrival_service(
        &self,
        req: &AnnounceArrivalRequest,
    ) -> std::result::Result<Response<()>, Status> {
        self.change_state(NodeState::UpdatingConnections).await;

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

    pub async fn transfer_keys_service(
        &self,
        req: &TransferKeysRequest,
    ) -> std::result::Result<Response<<Node as NodeService>::TransferKeysStream>, Status> {
        let prev_id = self.id;
        let node_id = req.id;

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
}
