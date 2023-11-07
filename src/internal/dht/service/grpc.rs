mod proto {
    tonic::include_proto!("dht.node");
}

pub use proto::node_service_client::*;
pub use proto::node_service_server::*;
pub use proto::*;

pub struct NodeEntryIterator<'a> {
    node_entry: &'a NodeEntry,
    index: usize,
}

impl<'a> IntoIterator for &'a NodeEntry {
    type Item = &'a NodeEntry;
    type IntoIter = NodeEntryIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        NodeEntryIterator {
            node_entry: self,
            index: 0,
        }
    }
}

impl<'a> Iterator for NodeEntryIterator<'a> {
    type Item = &'a NodeEntry;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index > 0 {
            None
        } else {
            self.index += 1;
            Some(self.node_entry)
        }
    }
}
