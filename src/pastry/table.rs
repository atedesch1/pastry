use crate::{
    error::Result,
    hring::ring::{Ring, Ring64},
    util::{get_nth_digit_in_u64_hex, U64_HEX_NUM_OF_DIGITS},
};

use super::shared::KeyValuePair;

/// A struct for constructing the Pastry's Routing Table data structure.
/// It keeps a 16x16 table (using u64 ids with hexadecimal digits) to store node structures in
/// order to route requests to the apropriate node.
#[derive(Debug, Clone)]
pub struct RoutingTable<T> {
    id: u64,
    table: Vec<Vec<Option<KeyValuePair<u64, T>>>>,
}

impl<T: Clone> RoutingTable<T> {
    /// Creates a new instance of the RoutingTable struct.
    pub fn new(id: u64) -> Self {
        let table = vec![vec![None; 0xF as usize]; U64_HEX_NUM_OF_DIGITS as usize];
        Self { id, table }
    }

    /// Inserts a value into the table, overwriting the previous if not empty.
    pub fn insert(&mut self, id: u64, val: T) -> Result<()> {
        let new_pair = KeyValuePair::new(id, val);

        for i in 0..U64_HEX_NUM_OF_DIGITS {
            let table_digit = get_nth_digit_in_u64_hex(self.id, i)?;
            let id_digit = get_nth_digit_in_u64_hex(id, i)?;
            if table_digit != id_digit {
                self.table[i as usize][id_digit as usize] = Some(new_pair);
                break;
            }
        }

        Ok(())
    }

    /// Removes a value from the table if it exists.
    pub fn remove(&mut self, id: u64) -> Result<()> {
        for i in 0..U64_HEX_NUM_OF_DIGITS {
            let table_digit = get_nth_digit_in_u64_hex(self.id, i)?;
            let id_digit = get_nth_digit_in_u64_hex(id, i)?;
            if table_digit != id_digit {
                self.table[i as usize][id_digit as usize] = None;
                break;
            }
        }

        Ok(())
    }

    /// Returns the next node to route the request to in the Pastry algorithm.
    pub fn route(&self, key: u64, min_matched_digits: u32) -> Result<T> {
        for i in min_matched_digits..U64_HEX_NUM_OF_DIGITS {
            let table_digit = get_nth_digit_in_u64_hex(self.id, i)?;
            let key_digit = get_nth_digit_in_u64_hex(key, i)?;

            if table_digit != key_digit {
                let row = &self.table[i as usize];
                if let Some(entry) = row[key_digit as usize].as_ref() {
                    return Ok(entry.value.clone());
                } else {
                    // Should start from the "no entry" digit and expand outwards
                    let mut closest: &Option<KeyValuePair<u64, T>> = &None;
                    for entry in row {
                        if let Some(e) = entry {
                            if closest.is_none()
                                || (Ring64::distance(e.key, self.id)
                                    < Ring64::distance(closest.as_ref().unwrap().key, self.id))
                            {
                                closest = entry;
                            }
                        }
                    }

                    return closest.clone().map(|kv| kv.value).ok_or(
                        crate::error::Error::Internal("could not find a node to route to.".into()),
                    );
                }
            }
        }

        Err(crate::error::Error::Internal(
            "could not find a node to route to.".into(),
        ))
    }
}

mod tests {
    use super::KeyValuePair;
    use super::Result;
    use super::RoutingTable;

    fn setup() -> RoutingTable<u64> {
        let id: u64 = 0xFEDCBA9876543210;

        RoutingTable::new(id)
    }

    #[test]
    fn test_insert() -> Result<()> {
        let mut t = setup();
        let kv = KeyValuePair::new(0xFEDCBA0000000000, 0xFEDCBA0000000000);
        t.insert(kv.key, kv.value)?;
        assert_eq!(t.table[6][0], Some(kv));

        let kv = KeyValuePair::new(0xFEDCBA9400000000, 0xFEDCBA9400000000);
        t.insert(kv.key, kv.value)?;
        assert_eq!(t.table[7][4], Some(kv));

        Ok(())
    }

    #[test]
    fn test_remove() -> Result<()> {
        let mut t = setup();
        let kv1 = KeyValuePair::new(0xFEDCBA0000000000, 0xFEDCBA0000000000);
        t.insert(kv1.key, kv1.value)?;
        let kv2 = KeyValuePair::new(0xFEDCBA9400000000, 0xFEDCBA9400000000);
        t.insert(kv2.key, kv2.value)?;

        t.remove(kv1.key)?;
        assert_eq!(t.table[6][0], None);

        Ok(())
    }

    #[test]
    fn test_route() -> Result<()> {
        let mut t = setup();
        let kv1 = KeyValuePair::new(0xFEDCBA0000000000, 0xFEDCBA0000000000);
        t.insert(kv1.key, kv1.value)?;
        let kv2 = KeyValuePair::new(0xFEDCBA1111111111, 0xFEDCBA1111111111);
        t.insert(kv2.key, kv2.value)?;
        let kv3 = KeyValuePair::new(0xFEDCBA2111111111, 0xFEDCBA2111111111);
        t.insert(kv3.key, kv3.value)?;
        let kv4 = KeyValuePair::new(0xFEDCBA4000000000, 0xFEDCBA4000000000);
        t.insert(kv4.key, kv4.value)?;

        let key = 0xFEDCBA0111111111;
        assert_eq!(t.route(key, 0)?, kv1.value);
        assert_eq!(t.route(key, 6)?, kv1.value);

        let key = 0xFEDCBA3333333333;
        assert_eq!(t.route(key, 0)?, kv4.value);

        Ok(())
    }
}
