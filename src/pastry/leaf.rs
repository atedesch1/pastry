use crate::{
    error::{Error, Result},
    hring::ring::{Ring, Ring64},
    util,
};
use std::{fmt::Display, vec};

use super::shared::KeyValuePair;

/// LeafSet is a data structure used in the Pastry routing algorithm.
/// The leaf set is a data structure that holds connection information
/// of the node's closests neighbors (that have the closest IDs).
#[derive(Debug, Clone)]
pub struct LeafSet<T: Clone> {
    max_size: usize,
    node_idx: usize,
    first_idx: usize,
    last_idx: usize,
    set: Vec<KeyValuePair<u64, T>>,
}

impl<T: Clone> LeafSet<T> {
    /// Creates a new instance of the LeafSet with the specified parameters.
    ///
    /// # Arguments
    ///
    /// * `k` - The size parameter `k` for the LeafSet. Must be greater than or equal to 1.
    /// * `key` - The main key for the LeafSet.
    /// * `value` - The main value for the LeafSet.
    ///
    /// # Returns
    ///
    /// A Result containing the newly created LeafSet if successful, or an Error if `k` is less than 1.
    ///
    pub fn new(k: usize, key: u64, value: T) -> Result<Self> {
        if k < 1 {
            return Err(Error::Config("cannot have leaf set with k < 1".into()));
        }

        Ok(Self {
            max_size: 2 * k + 1,
            node_idx: 0,
            first_idx: 0,
            last_idx: 0,
            set: vec![KeyValuePair::new(key, value)],
        })
    }

    /// Checks if the leaf set is full.
    ///
    pub fn is_full(&self) -> bool {
        self.set.len() == self.max_size
    }

    /// Gets the maximum leaf set size (2k + 1).
    ///
    pub fn get_max_size(&self) -> usize {
        self.max_size
    }

    /// Gets all leaf set entries.
    ///
    pub fn get_set(&self) -> Vec<&T> {
        self.set.iter().map(|e| &e.value).collect()
    }

    /// Gets leaf set entries, filtering out the center node entry.
    ///
    pub fn get_entries(&self) -> Vec<&T> {
        let node_id = self.set[self.node_idx].key;
        self.set
            .iter()
            .filter(|&e| e.key != node_id)
            .map(|e| &e.value)
            .collect()
    }

    /// Gets the node that is the owner of the supplied key.
    ///
    /// # Arguments
    ///
    /// * `key` - The key being routed.
    ///
    /// # Returns
    ///
    /// An Option containing the owner of the supplied key, or None if not found.
    ///
    pub fn get(&self, key: u64) -> Option<&T> {
        self.find_owner(key).map(|idx| &self.set[idx].value)
    }

    /// Gets the node which has the closest ID to the supplied one.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to find the closest node to.
    ///
    /// # Returns
    ///
    /// A Result containing the node that has key closest to the supplied one and the number of
    /// the matched digits between the key supplied and the node ID.
    ///
    pub fn get_closest(&self, key: u64) -> Result<(&T, usize)> {
        let mut closest: Option<&KeyValuePair<u64, T>> = None;

        for kv in &self.set {
            if closest.is_none()
                || Ring64::distance(key, kv.key) < Ring64::distance(key, closest.unwrap().key)
            {
                closest = Some(kv);
            }
        }

        Ok((
            &closest.unwrap().value,
            util::get_num_matched_digits(key, closest.unwrap().key)? as usize,
        ))
    }

    /// Gets first counter clockwise neighbor.
    ///
    /// # Returns
    ///
    /// An Option containing the neighbor of leaf set owner, or None if it has none.
    ///
    pub fn get_first_counter_clockwise_neighbor(&self) -> Option<&T> {
        let idx = (self.set.len() + self.node_idx - 1) % self.set.len();
        match idx == self.node_idx {
            true => None,
            false => Some(&self.set[idx].value),
        }
    }

    /// Gets first clockwise neighbor.
    ///
    /// # Returns
    ///
    /// An Option containing the neighbor of leaf set owner, or None if it has none.
    ///
    pub fn get_first_clockwise_neighbor(&self) -> Option<&T> {
        let idx = (self.node_idx + 1) % self.set.len();
        match idx == self.node_idx {
            true => None,
            false => Some(&self.set[idx].value),
        }
    }

    /// Gets furthest neighbor in the counter clockwise direction.
    ///
    /// # Returns
    ///
    /// An Option containing the neighbor. None if set is not full.
    ///
    pub fn get_furthest_counter_clockwise_neighbor(&self) -> Option<&T> {
        if !self.is_full() {
            return None;
        }

        Some(&self.set[self.first_idx].value)
    }

    /// Gets furthest neighbor in the clockwise direction.
    ///
    /// # Returns
    ///
    /// An Option containing the neighbor. None if set is not full.
    ///
    pub fn get_furthest_clockwise_neighbor(&self) -> Option<&T> {
        if !self.is_full() {
            return None;
        }

        Some(&self.set[self.last_idx].value)
    }

    /// Inserts a key-value pair into the LeafSet, maintaining its size and order.
    ///
    /// If the LeafSet is not full, the new pair is inserted into the appropriate position based on the key,
    /// and the `node_idx` is updated if necessary.
    ///
    /// If the LeafSet is already full, the new pair replaces an existing pair based on the key's position.
    /// The LeafSet is then sorted by key to maintain order.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to insert.
    /// * `value` - The value associated with the key.
    ///
    /// # Returns
    ///
    /// An `Ok(())` result if the insertion was successful.
    ///
    /// # Errors
    ///
    /// An error is returned if:
    ///
    /// * Key is outside the set, indicating an internal error.
    ///
    pub fn insert(&mut self, key: u64, value: T) -> Result<()> {
        let new_pair = KeyValuePair::new(key, value);

        if !self.is_full() {
            let position = match self.set.binary_search_by(|pair| pair.key.cmp(&key)) {
                Ok(position) => position,
                Err(position) => position,
            };

            self.set.insert(position, new_pair);

            if position <= self.node_idx {
                self.node_idx += 1;
            }
        } else {
            let position = self
                .find_owner(key)
                .ok_or(Error::Internal("key cannot be outside set".into()))?;

            let replaced_index = if self.is_clockwise_neighbor(position).unwrap() {
                self.last_idx
            } else {
                self.first_idx
            };

            let id = self.set[self.node_idx].key;

            if key > id && replaced_index < self.node_idx {
                self.node_idx -= 1;
            } else if key < id && replaced_index > self.node_idx {
                self.node_idx += 1;
            }

            self.set[replaced_index] = new_pair;

            self.set.sort_by_key(|e| e.key);
        }

        if self.is_full() {
            self.last_idx = (self.node_idx + self.max_size / 2) % self.max_size;
            self.first_idx = (self.last_idx + 1) % self.max_size;
        }

        Ok(())
    }

    /// Removes a key-value pair from the LeafSet based on the provided key.
    ///
    /// The key-value pair is removed from the set, and if the removed key is on the left side of the node index,
    /// the `node_idx` is decremented accordingly.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to remove from the LeafSet.
    ///
    /// # Returns
    ///
    /// An `Ok(())` result if the removal was successful.
    ///
    /// # Errors
    ///
    /// An error is returned if:
    ///
    /// * The specified key cannot be found in the LeafSet, indicating an internal error.
    /// * The specified key is in the same position as the `node_idx`, indicating it cannot be removed.
    ///
    pub fn remove(&mut self, key: u64) -> Result<()> {
        let position = self
            .set
            .iter()
            .position(|e| e.key == key)
            .ok_or(Error::Internal(format!(
                "cannot find element with key {}",
                key
            )))?;

        if position == self.node_idx {
            return Err(Error::Internal("cannot remove node from leaf set".into()));
        }

        self.set.remove(position);

        if position < self.node_idx {
            self.node_idx -= 1;
        }

        Ok(())
    }

    /// Checks if the index corresponds to a clockwise neighbor
    fn is_clockwise_neighbor(&self, idx: usize) -> Result<bool> {
        if idx >= self.set.len() {
            return Err(Error::Internal("index is out of bounds".into()));
        }

        if !self.is_full() {
            return Ok(true);
        }

        Ok(if self.last_idx > self.node_idx {
            self.node_idx <= idx && idx <= self.last_idx
        } else {
            (self.node_idx <= idx) || (idx <= self.last_idx)
        })
    }

    /// Finds the index of the owner of the key in the set
    fn find_owner(&self, key: u64) -> Option<usize> {
        let mut index = match self.set.binary_search_by(|pair| pair.key.cmp(&key)) {
            Ok(index) => index,
            Err(index) => index,
        };

        if index == self.set.len() {
            index -= 1;
        } else if key < self.set[index].key {
            index = if index == 0 {
                self.set.len() - 1
            } else {
                index - 1
            };
        }

        if self.is_full() && index == self.last_idx {
            return None;
        }

        Some(index)
    }
}

impl<T: Clone> Display for LeafSet<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let set_str = {
            let mut str = String::new();
            if !self.is_full() {
                str += "-> ";
                for kv in &self.set {
                    str += format!("{:016X} -> ", kv.key).as_str();
                }
            } else {
                for i in 0..self.max_size {
                    str += format!(
                        "{:016X}",
                        self.set[(self.first_idx + i) % self.max_size].key
                    )
                    .as_str();
                    if i < self.max_size - 1 {
                        str += " -> ";
                    }
                }
            }
            str
        };
        write!(f, "{}", set_str)
    }
}

pub struct LeafSetIterator<T: Clone> {
    data: LeafSet<T>,
    index: usize,
}

impl<T: Clone> Iterator for LeafSetIterator<T> {
    type Item = KeyValuePair<u64, T>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index == self.data.last_idx {
            None
        } else {
            self.index = (self.index + 1) % self.data.max_size;
            Some(self.data.set[self.index].clone())
        }
    }
}

impl<T: Clone> IntoIterator for LeafSet<T> {
    type Item = KeyValuePair<u64, T>;
    type IntoIter = LeafSetIterator<T>;

    fn into_iter(self) -> Self::IntoIter {
        LeafSetIterator {
            data: self.clone(),
            index: self.first_idx,
        }
    }
}

mod tests {
    use super::*;
    use crate::error::{Error, Result};

    fn leafset_from_vec(k: usize, initial: u64, v: Vec<u64>) -> LeafSet<Option<()>> {
        let mut leaf: LeafSet<Option<()>> = LeafSet::new(k, initial, None).unwrap();
        leaf.set = v.iter().map(|&i| KeyValuePair::new(i, None)).collect();
        leaf.node_idx = v.iter().position(|&i| i == initial).unwrap();
        if leaf.is_full() {
            leaf.last_idx = (leaf.node_idx + leaf.max_size / 2) % leaf.max_size;
            leaf.first_idx = (leaf.last_idx + 1) % leaf.max_size;
        }
        leaf
    }

    fn set_to_vec<T: Clone>(leafset: &LeafSet<T>) -> Vec<u64> {
        leafset.set.iter().map(|val| val.key).collect()
    }

    #[test]
    fn test_find_owner() -> Result<()> {
        let k = 2;
        // Size < MAX_SIZE

        // -> 300 -> 100 -> 200 ->
        let leafset = leafset_from_vec(k, 100, vec![100, 200, 300]);
        assert_eq!(leafset.find_owner(350), Some(2));
        assert_eq!(leafset.find_owner(50), Some(2));
        assert_eq!(leafset.find_owner(150), Some(0));
        assert_eq!(leafset.find_owner(250), Some(1));

        // Size == MAX_SIZE

        // 400 -> 500 -> 100 -> 200 -> 300
        let leafset = leafset_from_vec(k, 100, vec![100, 200, 300, 400, 500]);
        assert_eq!(leafset.find_owner(450), Some(3));
        assert_eq!(leafset.find_owner(550), Some(4));
        assert_eq!(leafset.find_owner(150), Some(0));
        assert_eq!(leafset.find_owner(250), Some(1));
        assert_eq!(leafset.find_owner(350), None);

        // 100 -> 200 -> 300 -> 400 -> 500
        let leafset = leafset_from_vec(k, 300, vec![100, 200, 300, 400, 500]);
        assert_eq!(leafset.find_owner(50), None);
        assert_eq!(leafset.find_owner(150), Some(0));
        assert_eq!(leafset.find_owner(250), Some(1));
        assert_eq!(leafset.find_owner(350), Some(2));
        assert_eq!(leafset.find_owner(450), Some(3));
        assert_eq!(leafset.find_owner(550), None);

        Ok(())
    }

    #[test]
    fn test_insert() -> Result<()> {
        let k = 2;
        let mut leaf: LeafSet<Option<()>> = LeafSet::new(k, 0, None).unwrap();

        leaf.insert(2, None)?;
        assert_eq!(set_to_vec(&leaf), vec![0, 2]);
        leaf.insert(4, None)?;
        assert_eq!(set_to_vec(&leaf), vec![0, 2, 4]);
        leaf.insert(6, None)?;
        assert_eq!(set_to_vec(&leaf), vec![0, 2, 4, 6]);
        leaf.insert(8, None)?;
        assert_eq!(set_to_vec(&leaf), vec![0, 2, 4, 6, 8]);

        assert_eq!(leaf.insert(5, None).is_err(), true);

        leaf.insert(3, None)?;
        assert_eq!(set_to_vec(&leaf), vec![0, 2, 3, 6, 8]);
        leaf.insert(1, None)?;
        assert_eq!(set_to_vec(&leaf), vec![0, 1, 2, 6, 8]);
        leaf.insert(7, None)?;
        assert_eq!(set_to_vec(&leaf), vec![0, 1, 2, 7, 8]);

        let mut leaf = leafset_from_vec(k, 4, vec![0, 1, 2, 3, 4]);
        leaf.insert(5, None)?;
        assert_eq!(set_to_vec(&leaf), vec![0, 2, 3, 4, 5]);

        let mut leaf = leafset_from_vec(k, 3, vec![1, 3, 4, 5, 6]);
        leaf.insert(2, None)?;
        assert_eq!(set_to_vec(&leaf), vec![1, 2, 3, 4, 5]);

        Ok(())
    }

    #[test]
    fn test_remove() -> Result<()> {
        let k = 2;
        let mut leaf = leafset_from_vec(k, 200, vec![100, 200, 300, 400]);

        leaf.remove(100)?;
        assert_eq!(set_to_vec(&leaf), vec![200, 300, 400]);
        assert_eq!(leaf.node_idx, 0);
        leaf.remove(300)?;
        assert_eq!(set_to_vec(&leaf), vec![200, 400]);
        assert_eq!(leaf.node_idx, 0);

        Ok(())
    }
}
