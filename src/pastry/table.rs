use crate::{
    error::Result,
    util::{get_distance_between_usigned, get_nth_digit_in_u64_hex, HasID, U64_HEX_NUM_OF_DIGITS},
};

/// A struct for constructing the Pastry's Routing Table data structure.
/// It keeps a 16x16 table (using u64 ids with hexadecimal digits) to store node structures in
/// order to route requests to the apropriate node.
#[derive(Debug, Clone)]
pub struct RoutingTable<U>
where
    U: HasID<u64>,
{
    id: u64,
    table: Vec<Vec<Option<U>>>,
}

impl<U> RoutingTable<U>
where
    U: Clone + HasID<u64>,
{
    /// Creates a new instance of the RoutingTable struct.
    pub fn new(id: u64) -> Self {
        let table = vec![vec![None; 0xF as usize]; U64_HEX_NUM_OF_DIGITS as usize];
        Self { id, table }
    }

    /// Inserts a value into the table, overwriting the previous if not empty.
    pub fn insert(&mut self, id: u64, val: U) -> Result<()> {
        for i in 0..U64_HEX_NUM_OF_DIGITS {
            let table_digit = get_nth_digit_in_u64_hex(self.id, i)?;
            let id_digit = get_nth_digit_in_u64_hex(id, i)?;
            if table_digit != id_digit {
                self.table[i as usize][id_digit as usize] = Some(val.clone());
                break;
            }
        }

        Ok(())
    }

    /// Returns the next node to route the request to in the Pastry algorithm.
    pub fn route(&self, key: u64, min_matched_digits: u32) -> Result<U> {
        for i in min_matched_digits..U64_HEX_NUM_OF_DIGITS {
            let table_digit = get_nth_digit_in_u64_hex(self.id, i)?;
            let key_digit = get_nth_digit_in_u64_hex(key, i)?;

            if table_digit != key_digit {
                let row = &self.table[i as usize];
                if let Some(entry) = row[key_digit as usize].as_ref() {
                    return Ok(entry.clone());
                } else {
                    // Should start from the "no entry" digit and expand outwards
                    let mut closest: &Option<U> = &None;
                    for entry in row {
                        if let Some(e) = entry {
                            if closest.is_none()
                                || (closest.is_some()
                                    && get_distance_between_usigned(e.get_id(), self.id)
                                        < get_distance_between_usigned(
                                            closest.as_ref().unwrap().get_id(),
                                            self.id,
                                        ))
                            {
                                closest = entry;
                            }
                        }
                    }
                    return closest.clone().ok_or(crate::error::Error::Internal(
                        "Couldn't find a node to route to.".into(),
                    ));
                }
            }
        }

        Err(crate::error::Error::Internal(
            "Couldn't find a node to route to.".into(),
        ))
    }
}

mod tests {
    use super::HasID;
    use super::Result;
    use super::RoutingTable;

    #[derive(Clone, Debug, PartialEq)]
    struct NodePackage {
        id: u64,
    }

    impl HasID<u64> for NodePackage {
        fn get_id(&self) -> u64 {
            self.id
        }
    }

    fn setup() -> RoutingTable<NodePackage> {
        let id: u64 = 0xFEDCBA9876543210;

        RoutingTable::new(id)
    }

    #[test]
    fn test_insert() -> Result<()> {
        let mut t = setup();
        let node = NodePackage {
            id: 0xFEDCBA0000000000,
        };
        t.insert(node.id, node.clone())?;
        assert_eq!(t.table[6][0], Some(node));

        let node = NodePackage {
            id: 0xFEDCBA9400000000,
        };
        t.insert(node.id, node.clone())?;

        assert_eq!(t.table[7][4], Some(node));

        Ok(())
    }

    #[test]
    fn test_route() -> Result<()> {
        let mut t = setup();
        let node = NodePackage {
            id: 0xFEDCBA0000000000,
        };
        t.insert(node.id, node.clone())?;
        let node = NodePackage {
            id: 0xFEDCBA1111111111,
        };
        t.insert(node.id, node.clone())?;
        let node = NodePackage {
            id: 0xFEDCBA2111111111,
        };
        t.insert(node.id, node.clone())?;
        let node = NodePackage {
            id: 0xFEDCBA4000000000,
        };
        t.insert(node.id, node.clone())?;

        let key = 0xFEDCBA0111111111;
        assert_eq!(
            t.route(key, 0)?,
            NodePackage {
                id: 0xFEDCBA0000000000,
            }
        );
        assert_eq!(
            t.route(key, 6)?,
            NodePackage {
                id: 0xFEDCBA0000000000,
            }
        );

        let key = 0xFEDCBA3333333333;
        assert_eq!(
            t.route(key, 0)?,
            NodePackage {
                id: 0xFEDCBA4000000000,
            }
        );

        Ok(())
    }
}
