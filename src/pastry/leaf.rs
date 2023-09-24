use crate::error::{Error, Result};
use std::vec;

/// A key-value pair data structure used in LeafSet
#[derive(Debug, Clone, PartialEq)]
pub struct KeyValuePair<T, U> {
    pub key: T,
    pub value: U,
}

impl<T, U> KeyValuePair<T, U> {
    fn new(key: T, value: U) -> Self {
        Self { key, value }
    }
}

/// LeafSet is a data structure used in the Pastry routing algorithm. The leaf set is a data
/// structure that holds connections to the closests neighbors to a node.
#[derive(Debug, Clone)]
pub struct LeafSet<T: Clone> {
    max_size: usize,
    node_idx: usize,
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
            set: vec![KeyValuePair::new(key, value)],
        })
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

        if self.set.len() < self.max_size {
            let mut position = self.find_responsible(key).unwrap();

            if position < self.set.len() && key > self.set[position].key {
                position += 1;
            } else if position == self.set.len() - 1 && key < self.set[position].key {
                position = 0;
            }

            self.set.insert(position, new_pair);

            if position <= self.node_idx {
                self.node_idx += 1;
            }
        } else {
            let position = self
                .find_responsible(key)
                .ok_or(Error::Internal("key cannot be outside set".into()))?;

            let replaced_index = if self.is_right_neighbor(position).unwrap() {
                self.get_last_index().unwrap()
            } else {
                self.get_first_index().unwrap()
            };
            self.set[replaced_index] = new_pair;

            self.set.sort_by_key(|e| e.key);
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

    /// Gets owner value of supplied key.
    ///
    /// # Arguments
    ///
    /// * `key` - The main key for the LeafSet.
    ///
    /// # Returns
    ///
    /// An Option containing the owner value of the supplied key, or None if not found.
    ///
    pub fn get(&self, key: u64) -> Option<T> {
        self.find_responsible(key)
            .map(|idx| self.set[idx].value.clone())
    }

    pub fn get_first_index(&self) -> Option<usize> {
        if self.set.len() < self.max_size {
            return None;
        }

        Some((self.max_size + self.node_idx - self.max_size / 2) % self.max_size)
    }

    pub fn get_last_index(&self) -> Option<usize> {
        if self.set.len() < self.max_size {
            return None;
        }

        Some((self.node_idx + self.max_size / 2) % self.max_size)
    }

    pub fn get_set(&self) -> Vec<KeyValuePair<u64, T>> {
        self.set.clone()
    }

    pub fn get_node_index(&self) -> usize {
        self.node_idx
    }

    pub fn is_full(&self) -> bool {
        self.set.len() == self.max_size
    }

    fn find_responsible(&self, key: u64) -> Option<usize> {
        let mut position = match self.set.binary_search_by(|pair| pair.key.cmp(&key)) {
            Ok(position) => position,
            Err(position) => position,
        };

        if position == self.set.len() {
            position -= 1;
        }

        if key < self.set[position].key {
            position = if position == 0 {
                self.set.len() - 1
            } else {
                position - 1
            };
        }

        if self.set.len() == self.max_size && position == self.get_last_index().unwrap() {
            return None;
        }

        Some(position)
    }

    fn is_right_neighbor(&self, idx: usize) -> Result<bool> {
        if idx >= self.set.len() {
            return Err(Error::Internal("index is out of bounds".into()));
        }

        if self.set.len() < self.max_size {
            return Ok(true);
        }

        let last_index = self.get_last_index().unwrap();

        Ok(if last_index > self.node_idx {
            self.node_idx <= idx && idx <= last_index
        } else {
            (self.node_idx <= idx && idx < self.set.len()) || (idx <= last_index)
        })
    }
}

mod tests {
    use crate::error::{Error, Result};

    use super::{KeyValuePair, LeafSet};

    fn leafset_from_vec(k: usize, initial: u64, v: Vec<u64>) -> LeafSet<Option<()>> {
        let mut leaf: LeafSet<Option<()>> = LeafSet::new(k, initial, None).unwrap();
        leaf.set = v.iter().map(|&i| KeyValuePair::new(i, None)).collect();
        leaf.node_idx = v.iter().position(|&i| i == initial).unwrap();
        leaf
    }

    fn set_to_vec<T: Clone>(leafset: &LeafSet<T>) -> Vec<u64> {
        leafset.set.iter().map(|val| val.key).collect()
    }

    #[test]
    fn test_find_responsible() -> Result<()> {
        let k = 2;
        // Size < MAX_SIZE

        // -> 300 -> 100 -> 200 ->
        let leafset = leafset_from_vec(k, 100, vec![100, 200, 300]);
        assert_eq!(leafset.find_responsible(350), Some(2));
        assert_eq!(leafset.find_responsible(50), Some(2));
        assert_eq!(leafset.find_responsible(150), Some(0));
        assert_eq!(leafset.find_responsible(250), Some(1));

        // Size == MAX_SIZE

        // 400 -> 500 -> 100 -> 200 -> 300
        let leafset = leafset_from_vec(k, 100, vec![100, 200, 300, 400, 500]);
        assert_eq!(leafset.find_responsible(450), Some(3));
        assert_eq!(leafset.find_responsible(550), Some(4));
        assert_eq!(leafset.find_responsible(150), Some(0));
        assert_eq!(leafset.find_responsible(250), Some(1));
        assert_eq!(leafset.find_responsible(350), None);

        // 100 -> 200 -> 300 -> 400 -> 500
        let leafset = leafset_from_vec(k, 300, vec![100, 200, 300, 400, 500]);
        assert_eq!(leafset.find_responsible(50), None);
        assert_eq!(leafset.find_responsible(150), Some(0));
        assert_eq!(leafset.find_responsible(250), Some(1));
        assert_eq!(leafset.find_responsible(350), Some(2));
        assert_eq!(leafset.find_responsible(450), Some(3));
        assert_eq!(leafset.find_responsible(550), None);

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
