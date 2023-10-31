use core::fmt;

use crate::{
    error::Result,
    hring::ring::{Ring, Ring64},
    util::{self, get_nth_digit_in_u64_hex, HEX_BASE, U64_HEX_NUM_OF_DIGITS},
};

use super::shared::KeyValuePair;

/// A struct for constructing the Pastry's Routing Table data structure.
/// It keeps a Nx16 table where N <= 16 (using u64 ids with hexadecimal digits)
/// to store node structures in order to route requests to the apropriate node.
#[derive(Debug, Clone)]
pub struct RoutingTable<T> {
    id: u64,
    table: Vec<Vec<Option<KeyValuePair<u64, T>>>>,
}

impl<T: Clone> RoutingTable<T> {
    /// Creates a new instance of the RoutingTable struct.
    pub fn new(id: u64) -> Self {
        Self {
            id,
            table: Vec::new(),
        }
    }

    /// Inserts a value into the table, overwriting the previous if not empty.
    pub fn insert(&mut self, key: u64, value: T) -> Result<()> {
        let new_pair = KeyValuePair::new(key, value);

        for i in 0..U64_HEX_NUM_OF_DIGITS as usize {
            let table_digit = get_nth_digit_in_u64_hex(self.id, i)?;
            let key_digit = get_nth_digit_in_u64_hex(key, i)?;

            if table_digit != key_digit {
                while self.table.len() < i + 1 {
                    // Push new rows to allow for new entry
                    self.table.push(vec![None; HEX_BASE as usize]);
                }

                self.table[i][key_digit as usize] = Some(new_pair);
                break;
            }
        }

        Ok(())
    }

    /// Removes a value from the table if it exists.
    pub fn remove(&mut self, key: u64) -> Result<()> {
        for i in 0..U64_HEX_NUM_OF_DIGITS as usize {
            let table_digit = get_nth_digit_in_u64_hex(self.id, i)?;
            let key_digit = get_nth_digit_in_u64_hex(key, i)?;

            if i >= self.table.len() {
                break;
            }

            if table_digit != key_digit {
                self.table[i][key_digit as usize] = None;
                break;
            }
        }

        Ok(())
    }

    /// Returns the next node to route the request to in the Pastry algorithm and the number of
    /// matched digits.
    pub fn route(&self, key: u64, min_matched_digits: usize) -> Result<Option<(&T, usize)>> {
        if min_matched_digits > self.table.len() - 1 {
            return Ok(None);
        }

        let matched_digits = util::get_num_matched_digits(self.id, key)? as usize;
        let row_index = matched_digits.min(self.table.len() - 1);

        if min_matched_digits > row_index {
            return Ok(None);
        }

        let row = &self.table[row_index];
        let key_digit = util::get_nth_digit_in_u64_hex(key, row_index)?;

        let mut closest: &Option<KeyValuePair<u64, T>> = &None;

        if row[key_digit as usize].is_some() {
            closest = &row[key_digit as usize];
        } else {
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
        }

        Ok(closest.as_ref().map(|kv| (&kv.value, row_index)))
    }

    /// Returns an Option containing a row of the routing table if it exists.
    pub fn get_row(&self, index: usize) -> Option<Vec<Option<&T>>> {
        self.table
            .get(index)
            .map(|v| v.iter().map(|e| e.as_ref().map(|kv| &kv.value)).collect())
    }

    /// Returns an Vector containing all entries of the routing table.
    pub fn get_entries(&self) -> Vec<Option<&T>> {
        self.table
            .iter()
            .flat_map(|row| row.iter().map(|e| e.as_ref().map(|kv| &kv.value)))
            .collect()
    }
}

impl<T> fmt::Display for RoutingTable<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Header
        write!(f, "{:016}|", "Matched")?;
        for i in 0..16 {
            write!(f, "{:016}|", format!("{:X}", i))?;
        }
        writeln!(f)?;

        // Content
        for (i, row) in self.table.iter().enumerate() {
            let mut matched = "*".to_string();
            if i > 0 {
                matched = format!(
                    "{:0width$X}",
                    util::get_first_digits_in_u64_hex(self.id, i).unwrap(),
                    width = i
                ) + &matched;
            }

            write!(f, "{:016}|", matched)?;

            for cell in row {
                match cell {
                    Some(kv) => write!(f, "{:016X}|", kv.key)?,
                    None => write!(f, "{:016}|", " ")?,
                }
            }
            writeln!(f)?;
        }
        Ok(())
    }
}

mod tests {
    use super::*;

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
        assert_eq!(
            t.route(key, 0)?.map(|e| (e.0.clone(), e.1)).unwrap(),
            (kv1.value, 6)
        );
        assert_eq!(
            t.route(key, 6)?.map(|e| (e.0.clone(), e.1)).unwrap(),
            (kv1.value, 6)
        );

        let key = 0xFEDCBA3333333333;
        assert_eq!(
            t.route(key, 0)?.map(|e| (e.0.clone(), e.1)).unwrap(),
            (kv4.value, 6)
        );

        Ok(())
    }
}
