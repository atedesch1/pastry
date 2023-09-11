use crate::error::{Error, Result};
use std::{sync::RwLock, vec};

/// A centered range set is a set of key value pairs which maintain an order based on their key.
/// This set has an odd number for size and always has a middle element.
pub trait CenteredRangeSet<T, U>
where
    T: PartialOrd + Clone,
    U: Clone,
{
    /// Creates new instance of a centered range set.
    fn new(size: usize, center_key: T, center_value: U) -> Result<Self>
    where
        Self: Sized;
    /// Inserts a new key value pair into the set by pushing other elements to the edges of the
    /// set.
    fn insert(&mut self, key: T, value: U) -> Result<()>;
    /// Removes a key value pair from the set and pulls other elements to the center of the set.
    fn remove(&mut self, key: T) -> Result<()>;
    /// Returns the key value pair responsible for the key provided. A key value pair is
    /// responsible for a key 'k' if 'k' lies in between the pair's key and the next pair's key.
    fn get(&self, key: T) -> Result<Option<U>>;
}

#[derive(Debug, Clone, PartialEq)]
struct KeyValuePair<T, U> {
    key: T,
    value: U,
}

impl<T, U> KeyValuePair<T, U> {
    fn new(key: T, value: U) -> Self {
        Self { key, value }
    }
}

/// LeafSet is a Pastry's routing structure. It is used as a first set when routing requests.
/// It works by implementing CenteredRangeSet trait, laying out node's keys and addresses as key
/// value pairs.
pub struct LeafSet<T, U>
where
    T: PartialOrd + Clone,
    U: Clone,
{
    size: usize,
    set: RwLock<Vec<Option<KeyValuePair<T, U>>>>,
}

impl<T, U> LeafSet<T, U>
where
    T: PartialOrd + Clone,
    U: Clone,
{
    /// Finds the index of the node to replace.
    /// This is used when inserting a new node or removing a node from the leaf set.
    fn find_replace(&self, key: &T) -> Result<usize> {
        let mut l = 0;
        let mut r = self.size - 1;
        let set = self.set.read()?;

        while l < r {
            let m = (r - l) / 2 + l;

            if let Some(entry) = set[m].as_ref() {
                if entry.key < *key {
                    l = m + 1;
                } else if entry.key > *key {
                    if m == 0 {
                        break;
                    }
                    r = m - 1;
                } else {
                    return Ok(m);
                }
            } else if m > self.size / 2 {
                r = m - 1;
            } else {
                l = m + 1;
            }
        }

        if let Some(value) = set[l].as_ref() {
            if l > 0 && l < self.size / 2 && value.key > *key {
                l -= 1;
            }
        }

        Ok(l)
    }

    /// Finds the index of the node responsible for key.
    /// If not in leaf set returns None.
    fn find_responsible(&self, key: &T) -> Result<Option<usize>> {
        let mut l = 0;
        let mut r = self.size - 1;
        let set = self.set.read()?;

        while l <= r {
            let m = (r - l) / 2 + l;

            if let Some(entry) = set[m].as_ref() {
                if entry.key < *key {
                    l = m + 1;
                } else if entry.key > *key {
                    if m == 0 {
                        break;
                    }
                    r = m - 1;
                } else {
                    r = m;
                    break;
                }
            } else if m > self.size / 2 {
                r = m - 1;
            } else {
                l = m + 1;
            }
        }

        if r == self.size - 1 || set[r].is_none() || set[r].as_ref().unwrap().key > *key {
            return Ok(None);
        }

        Ok(Some(r))
    }

    /// Returns a clone of the underlying set as a vector.
    fn get_set(&self) -> Result<Vec<Option<KeyValuePair<T, U>>>> {
        let set = self.set.read()?;

        Ok(set.clone())
    }
}

impl<T, U> CenteredRangeSet<T, U> for LeafSet<T, U>
where
    T: PartialOrd + Clone,
    U: Clone,
{
    fn new(size: usize, center_key: T, center_value: U) -> Result<Self> {
        if size % 2 == 0 {
            return Err(Error::Config(
                "Leaf set should have an odd number size.".into(),
            ));
        }
        if size < 3 {
            return Err(Error::Config(
                "Leaf set should have at least 3 entries.".into(),
            ));
        }

        let mut v = vec![None; size];
        v[size / 2] = Some(KeyValuePair::new(center_key, center_value));

        Ok(LeafSet {
            size,
            set: RwLock::new(v),
        })
    }

    fn insert(&mut self, key: T, value: U) -> Result<()> {
        let idx = self.find_replace(&key)?;
        let mut set = self.set.write()?;

        let mut prev = set[idx].take();

        if prev.is_some() && idx < self.size - 1 && idx > 0 {
            let range: Box<dyn Iterator<Item = usize>> = if idx > self.size / 2 {
                Box::new(idx + 1..self.size)
            } else {
                Box::new((0..idx).rev())
            };

            for i in range {
                let temp = set[i].take();
                set[i] = prev;
                prev = temp;
            }
        }

        set[idx] = Some(KeyValuePair::new(key, value));

        Ok(())
    }

    fn remove(&mut self, key: T) -> Result<()> {
        let idx = self.find_replace(&key)?;
        let mut set = self.set.write()?;

        if idx == self.size / 2 {
            return Err(Error::Internal("Cannot remove middle element".into()));
        }
        if set[idx].is_none() || set[idx].as_ref().unwrap().key != key {
            return Err(Error::Internal("Cannot find key in leaf set".into()));
        }

        let mut i = idx;
        if idx > self.size / 2 {
            while i < self.size - 1 {
                set[i] = set[i + 1].take();
                i += 1;
            }
            set[self.size - 1] = None;
        } else {
            while i > 0 {
                set[i] = set[i - 1].take();
                i -= 1;
            }
            set[0] = None;
        }

        Ok(())
    }

    fn get(&self, key: T) -> Result<Option<U>> {
        let idx = self.find_responsible(&key)?;
        let set = self.set.read()?;

        Ok(idx
            .map(|i| set[i].as_ref().unwrap())
            .map(|kv| kv.value.to_owned()))
    }
}

#[test]
fn test_find_replace() -> Result<()> {
    let mut leaf_set = LeafSet::new(5, 100, 100)?;
    // [x,x,100,x,x]
    assert_eq!(leaf_set.find_replace(&100)?, 2);
    assert_eq!(leaf_set.find_replace(&150)?, 3);
    assert_eq!(leaf_set.find_replace(&50)?, 1);

    leaf_set.insert(150, 150)?;
    // [x,x,100,150,x]
    assert_eq!(leaf_set.find_replace(&125)?, 3);
    assert_eq!(leaf_set.find_replace(&150)?, 3);
    assert_eq!(leaf_set.find_replace(&200)?, 4);

    leaf_set.insert(200, 200)?;
    // [x,x,100,150,200]
    assert_eq!(leaf_set.find_replace(&175)?, 4);
    assert_eq!(leaf_set.find_replace(&200)?, 4);

    leaf_set.insert(50, 50)?;
    // [x,50,100,150,200]
    assert_eq!(leaf_set.find_replace(&50)?, 1);
    assert_eq!(leaf_set.find_replace(&75)?, 1);
    assert_eq!(leaf_set.find_replace(&0)?, 0);

    leaf_set.insert(0, 0)?;
    // [0,50,100,150,200]
    assert_eq!(leaf_set.find_replace(&0)?, 0);
    assert_eq!(leaf_set.find_replace(&25)?, 0);
    assert_eq!(leaf_set.find_replace(&-25)?, 0);

    Ok(())
}

#[test]
fn test_find_responsible() -> Result<()> {
    let mut leaf_set = LeafSet::new(5, 100, 100)?;
    // [x,x,100,x,x]
    assert_eq!(leaf_set.find_responsible(&100)?, Some(2));
    assert_eq!(leaf_set.find_responsible(&150)?, Some(2));
    assert_eq!(leaf_set.find_responsible(&50)?, None);

    leaf_set.insert(150, 150)?;
    // [x,x,100,150,x]
    assert_eq!(leaf_set.find_responsible(&125)?, Some(2));
    assert_eq!(leaf_set.find_responsible(&150)?, Some(3));
    assert_eq!(leaf_set.find_responsible(&200)?, Some(3));

    leaf_set.insert(200, 200)?;
    // [x,x,100,150,200]
    assert_eq!(leaf_set.find_responsible(&175)?, Some(3));
    assert_eq!(leaf_set.find_responsible(&200)?, None);
    assert_eq!(leaf_set.find_responsible(&250)?, None);

    leaf_set.insert(50, 50)?;
    // [x,50,100,150,200]
    assert_eq!(leaf_set.find_responsible(&50)?, Some(1));
    assert_eq!(leaf_set.find_responsible(&75)?, Some(1));
    assert_eq!(leaf_set.find_responsible(&0)?, None);

    leaf_set.insert(0, 0)?;
    // [0,50,100,150,200]
    assert_eq!(leaf_set.find_responsible(&0)?, Some(0));
    assert_eq!(leaf_set.find_responsible(&25)?, Some(0));
    assert_eq!(leaf_set.find_responsible(&-25)?, None);

    Ok(())
}

#[test]
fn test_insert() -> Result<()> {
    let mut leaf_set = LeafSet::new(5, 100, 100)?;
    assert_eq!(
        leaf_set.get_set()?,
        vec![None, None, Some(KeyValuePair::new(100, 100)), None, None]
    );

    // [x,x,100,x,x] -> [x,x,100,150,x]
    leaf_set.insert(150, 150)?;
    assert_eq!(
        leaf_set.get_set()?,
        vec![
            None,
            None,
            Some(KeyValuePair::new(100, 100)),
            Some(KeyValuePair::new(150, 150)),
            None
        ]
    );

    // [x,x,100,150,x] -> [x,x,100,150,200]
    leaf_set.insert(200, 200)?;
    assert_eq!(
        leaf_set.get_set()?,
        vec![
            None,
            None,
            Some(KeyValuePair::new(100, 100)),
            Some(KeyValuePair::new(150, 150)),
            Some(KeyValuePair::new(200, 200)),
        ]
    );

    // [x,x,100,150,200] -> [x,x,100,150,175]
    leaf_set.insert(175, 175)?;
    assert_eq!(
        leaf_set.get_set()?,
        vec![
            None,
            None,
            Some(KeyValuePair::new(100, 100)),
            Some(KeyValuePair::new(150, 150)),
            Some(KeyValuePair::new(175, 175)),
        ]
    );

    // [x,x,100,150,175] -> [x,x,100,150,200]
    leaf_set.insert(200, 200)?;
    assert_eq!(
        leaf_set.get_set()?,
        vec![
            None,
            None,
            Some(KeyValuePair::new(100, 100)),
            Some(KeyValuePair::new(150, 150)),
            Some(KeyValuePair::new(200, 200)),
        ]
    );

    // [x,x,100,150,200] -> [x,x,100,125,150]
    leaf_set.insert(125, 125)?;
    assert_eq!(
        leaf_set.get_set()?,
        vec![
            None,
            None,
            Some(KeyValuePair::new(100, 100)),
            Some(KeyValuePair::new(125, 125)),
            Some(KeyValuePair::new(150, 150)),
        ]
    );

    // [x,x,100,125,150] -> [x,50,100,125,150]
    leaf_set.insert(50, 50)?;
    assert_eq!(
        leaf_set.get_set()?,
        vec![
            None,
            Some(KeyValuePair::new(50, 50)),
            Some(KeyValuePair::new(100, 100)),
            Some(KeyValuePair::new(125, 125)),
            Some(KeyValuePair::new(150, 150)),
        ]
    );

    // [x,50,100,125,150] -> [50,75,100,125,150]
    leaf_set.insert(75, 75)?;
    assert_eq!(
        leaf_set.get_set()?,
        vec![
            Some(KeyValuePair::new(50, 50)),
            Some(KeyValuePair::new(75, 75)),
            Some(KeyValuePair::new(100, 100)),
            Some(KeyValuePair::new(125, 125)),
            Some(KeyValuePair::new(150, 150)),
        ]
    );

    // [50,75,100,125,150] -> [0,75,100,125,150]
    leaf_set.insert(0, 0)?;
    assert_eq!(
        leaf_set.get_set()?,
        vec![
            Some(KeyValuePair::new(0, 0)),
            Some(KeyValuePair::new(75, 75)),
            Some(KeyValuePair::new(100, 100)),
            Some(KeyValuePair::new(125, 125)),
            Some(KeyValuePair::new(150, 150)),
        ]
    );

    Ok(())
}

#[test]
fn test_remove() -> Result<()> {
    let mut leaf_set = LeafSet::new(5, 100, 100)?;
    leaf_set.insert(150, 150)?;
    leaf_set.insert(200, 200)?;
    leaf_set.insert(50, 50)?;
    leaf_set.insert(0, 0)?;
    // [0,50,100,150,200]
    assert_eq!(
        leaf_set.get_set()?,
        vec![
            Some(KeyValuePair::new(0, 0)),
            Some(KeyValuePair::new(50, 50)),
            Some(KeyValuePair::new(100, 100)),
            Some(KeyValuePair::new(150, 150)),
            Some(KeyValuePair::new(200, 200)),
        ]
    );
    // [0,50,100,150,200] -> [0,50,100,150,x]
    leaf_set.remove(200)?;
    assert_eq!(
        leaf_set.get_set()?,
        vec![
            Some(KeyValuePair::new(0, 0)),
            Some(KeyValuePair::new(50, 50)),
            Some(KeyValuePair::new(100, 100)),
            Some(KeyValuePair::new(150, 150)),
            None
        ]
    );
    leaf_set.insert(200, 200)?;

    // [0,50,100,150,200] -> [0,50,100,200,x]
    leaf_set.remove(150)?;
    assert_eq!(
        leaf_set.get_set()?,
        vec![
            Some(KeyValuePair::new(0, 0)),
            Some(KeyValuePair::new(50, 50)),
            Some(KeyValuePair::new(100, 100)),
            Some(KeyValuePair::new(200, 200)),
            None
        ]
    );

    // [0,50,100,200,x] -> [0,50,100,x,x]
    leaf_set.remove(200)?;
    assert_eq!(
        leaf_set.get_set()?,
        vec![
            Some(KeyValuePair::new(0, 0)),
            Some(KeyValuePair::new(50, 50)),
            Some(KeyValuePair::new(100, 100)),
            None,
            None
        ]
    );
    leaf_set.insert(150, 150)?;
    leaf_set.insert(200, 200)?;

    // [0,50,100,150,200] -> [x,50,100,150,200]
    leaf_set.remove(0)?;
    assert_eq!(
        leaf_set.get_set()?,
        vec![
            None,
            Some(KeyValuePair::new(50, 50)),
            Some(KeyValuePair::new(100, 100)),
            Some(KeyValuePair::new(150, 150)),
            Some(KeyValuePair::new(200, 200))
        ]
    );
    leaf_set.insert(0, 0)?;

    // [0,50,100,150,200] -> [x,0,100,150,200]
    leaf_set.remove(50)?;
    assert_eq!(
        leaf_set.get_set()?,
        vec![
            None,
            Some(KeyValuePair::new(0, 0)),
            Some(KeyValuePair::new(100, 100)),
            Some(KeyValuePair::new(150, 150)),
            Some(KeyValuePair::new(200, 200))
        ]
    );

    Ok(())
}

#[test]
fn test_get() -> Result<()> {
    let mut leaf_set = LeafSet::new(5, 100, 100)?;
    leaf_set.insert(150, 150)?;
    leaf_set.insert(200, 200)?;
    leaf_set.insert(50, 50)?;
    leaf_set.insert(0, 0)?;

    // [0,50,100,150,200]
    assert_eq!(leaf_set.get(-25)?, None);
    assert_eq!(leaf_set.get(25)?, Some(0));
    assert_eq!(leaf_set.get(75)?, Some(50));
    assert_eq!(leaf_set.get(125)?, Some(100));
    assert_eq!(leaf_set.get(175)?, Some(150));
    assert_eq!(leaf_set.get(225)?, None);

    leaf_set.remove(200)?;
    // [0,50,100,150,x]
    assert_eq!(leaf_set.get(175)?, Some(150));

    leaf_set.remove(150)?;
    // [0,50,100,x,x]
    assert_eq!(leaf_set.get(150)?, Some(100));

    leaf_set.remove(0)?;
    // [x,50,100,x,x]
    assert_eq!(leaf_set.get(25)?, None);

    leaf_set.remove(50)?;
    // [x,x,100,x,x]
    assert_eq!(leaf_set.get(125)?, Some(100));
    assert_eq!(leaf_set.get(75)?, None);

    Ok(())
}
