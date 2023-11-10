use log::{debug, info, warn};

use super::grpc::*;

use crate::{
    error::*,
    internal::{
        dht::node::{Node, NodeInfo, NodeState},
        util::{self, U64_HEX_NUM_OF_DIGITS},
    },
};

impl Node {
    /// Attempts to fix a leaf set entry
    pub async fn fix_leaf_entry(&self, node: &NodeInfo) -> Result<()> {
        info!("#{:016X}: Fixing leaf set", self.id);
        self.change_state(NodeState::UpdatingConnections).await;

        let mut data = self.state.data.write().await;

        if !data.leaf.is_full() {
            // remove failed entry
            data.leaf.remove(node.id).unwrap();

            // there are not enough nodes to replace entry

            self.change_state(NodeState::RoutingRequests).await;
            return Ok(());
        }

        match data.leaf.is_clockwise_neighbor(node.id) {
            Err(_) => {}
            Ok(is_clockwise_neighbor) => {
                // iterator without failed node
                let forward_iterator = data.leaf.clone().into_iter().filter(|e| e.id != node.id);

                // remove failed leaf entry
                data.leaf.remove(node.id).unwrap();

                // yield only the ones on the same side as the failed node
                let nodes_on_the_same_side: Vec<NodeInfo> = if !is_clockwise_neighbor {
                    forward_iterator.take_while(|e| e.id != self.id).collect()
                } else {
                    forward_iterator
                        .rev()
                        .take_while(|e| e.id != self.id)
                        .collect()
                };

                for neighbor in &nodes_on_the_same_side {
                    // check if node is alive
                    let mut client =
                        match NodeServiceClient::connect(neighbor.pub_addr.to_owned()).await {
                            Ok(client) => client,
                            Err(err) => {
                                warn!(
                                    "#{:016X}: Connection to #{:016X} failed: {}",
                                    self.id, neighbor.id, err
                                );
                                continue;
                            }
                        };
                    let state = client.get_node_state(()).await?.into_inner();

                    // replace entry
                    for entry in state.leaf_set {
                        if entry.id == neighbor.id || entry.id == node.id {
                            continue;
                        }

                        // check if entry is alive
                        if let Err(err) =
                            NodeServiceClient::connect(entry.pub_addr.to_owned()).await
                        {
                            warn!(
                                "#{:016X}: Connection to #{:016X} failed: {}",
                                self.id, entry.id, err
                            );
                            continue;
                        }

                        data.leaf
                            .insert(entry.id, NodeInfo::from_node_entry(&entry))?;
                    }

                    // break if already fixed leaf set
                    if data.leaf.is_full() {
                        break;
                    }
                }

                if !data.leaf.is_full() {
                    // unable to fix leaf set
                    panic!(
                        "#{:016X}: Could not fix leaf set. Too many failed nodes.",
                        self.id,
                    );
                }

                debug!("#{:016X}: Fixed leaf set: \n{}", self.id, data.leaf);
            }
        }

        self.change_state(NodeState::RoutingRequests).await;
        Ok(())
    }

    pub async fn fix_table_entry(&self, node: &NodeInfo) -> Result<()> {
        info!("#{:016X}: Fixing routing table", self.id);
        self.change_state(NodeState::UpdatingConnections).await;

        let mut data = self.state.data.write().await;

        // remove node from table
        data.table.remove(node.id)?;

        let matched_digits = util::get_num_matched_digits(self.id, node.id)?;
        let row_index = matched_digits;
        let column_index = util::get_nth_digit_in_u64_hex(node.id, matched_digits as usize + 1)?;

        let mut matched = matched_digits;
        while let Some(row) = data.table.get_row(matched as usize) {
            for entry in row.iter().filter_map(|&opt| opt) {
                if entry.id == node.id || entry.id == self.id {
                    continue;
                }

                let mut client = match NodeServiceClient::connect(entry.pub_addr.to_owned()).await {
                    Ok(client) => client,
                    Err(err) => {
                        warn!(
                            "#{:016X}: Connection to #{:016X} failed: {}",
                            self.id, entry.id, err
                        );
                        continue;
                    }
                };

                let table_entry = client
                    .get_node_table_entry(GetNodeTableEntryRequest {
                        row: row_index,
                        column: column_index,
                    })
                    .await?
                    .into_inner()
                    .node;

                if let Some(replacement) = table_entry {
                    if replacement.id != node.id {
                        data.table
                            .insert(replacement.id, NodeInfo::from_node_entry(&replacement))?;
                        debug!("#{:016X}: Fixed routing table: \n{}", self.id, data.table);
                        break;
                    }
                }
            }

            matched += 1;
            if matched == U64_HEX_NUM_OF_DIGITS {
                break;
            }
        }

        self.change_state(NodeState::RoutingRequests).await;
        Ok(())
    }

    pub async fn warn_and_fix_leaf_entry(&self, node: &NodeInfo, err: &str) {
        warn!(
            "#{:016X}: Connection to #{:016X} failed: {}",
            self.id, node.id, err
        );
        let _ = self.fix_leaf_entry(&node).await;

        // notify neighbors of failed leaf entry
        for leaf_entry in self.state.data.read().await.leaf.get_entries() {
            let mut client = match NodeServiceClient::connect(leaf_entry.pub_addr.to_owned()).await
            {
                Ok(client) => client,
                Err(err) => {
                    warn!(
                        "#{:016X}: Connection to #{:016X} failed: {}",
                        self.id, node.id, err
                    );
                    continue;
                }
            };

            let _ = client
                .fix_leaf_set(FixLeafSetRequest {
                    id: node.id,
                    pub_addr: node.pub_addr.clone(),
                })
                .await;
        }
    }

    pub async fn warn_and_fix_table_entry(&self, node: &NodeInfo, err: &str) {
        warn!(
            "#{:016X}: Connection to #{:016X} failed: {}",
            self.id, node.id, err
        );

        let curr_node = self.clone();
        let failed_node = node.clone();
        tokio::spawn(async move { curr_node.fix_table_entry(&failed_node).await });
    }
}
