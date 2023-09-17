use crate::error::{Error, Result};
use std::vec;

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

#[derive(Debug, Clone)]
pub struct LeafSet<T: Clone> {
    max_size: usize,
    node_idx: usize,
    set: Vec<KeyValuePair<u64, T>>,
}

impl<T: Clone> LeafSet<T> {
    pub fn new(k: usize, initial_key: u64, initial_value: T) -> Result<Self> {
        if k < 1 {
            return Err(Error::Config("cannot have leaf set with k < 1".into()));
        }

        Ok(Self {
            max_size: 2 * k + 1,
            node_idx: 0,
            set: vec![KeyValuePair::new(initial_key, initial_value)],
        })
    }

    pub fn insert(&mut self, key: u64, value: T) -> Result<()> {
        let new_pair = KeyValuePair::new(key, value);

        if self.set.len() < self.max_size {
            let position = self.find_responsible(key).unwrap();

            self.set.insert(position + 1, new_pair);

            if position + 1 <= self.node_idx {
                self.node_idx += 1;
            }
        } else {
            let position = self
                .find_responsible(key)
                .ok_or(Error::Internal("key cannot be outside set".into()))?;

            if self.is_right_neighbor(position).unwrap() {
                let last_index = self.get_last_index().unwrap();
                self.set[last_index] = new_pair;
            } else {
                let first_index = self.get_first_index().unwrap();
                self.set[first_index] = new_pair;
            }

            self.set.sort_by_key(|e| e.key);
        }

        Ok(())
    }

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

    pub fn get(&self, key: u64) -> Option<T> {
        self.find_responsible(key)
            .map(|idx| self.set[idx].value.clone())
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

    fn get_first_index(&self) -> Option<usize> {
        if self.set.len() < self.max_size {
            return None;
        }

        Some((self.max_size + self.node_idx - self.max_size / 2) % self.max_size)
    }

    fn get_last_index(&self) -> Option<usize> {
        if self.set.len() < self.max_size {
            return None;
        }

        Some((self.node_idx + self.max_size / 2) % self.max_size)
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
