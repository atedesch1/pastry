use std::{
    collections::HashMap,
    hash::{Hash, Hasher},
};

#[derive(Debug, PartialEq, Eq)]
struct PreHashedKey(u64);

impl Hash for PreHashedKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.0);
    }
}

impl From<u64> for PreHashedKey {
    fn from(value: u64) -> Self {
        PreHashedKey(value)
    }
}

#[derive(Debug)]
pub struct Store {
    store: HashMap<PreHashedKey, Vec<u8>>,
}

impl Store {
    pub fn new() -> Self {
        Store {
            store: HashMap::new(),
        }
    }

    pub fn get(&self, key: &u64) -> Option<&Vec<u8>> {
        self.store.get(&key.clone().into())
    }

    pub fn set(&mut self, key: &u64, value: &[u8]) -> Option<Vec<u8>> {
        self.store.insert(key.clone().into(), value.to_vec())
    }

    pub fn delete(&mut self, key: &u64) -> Option<Vec<u8>> {
        self.store.remove(&key.clone().into())
    }

    pub fn list(&self) -> Vec<(u64, &Vec<u8>)> {
        self.store.iter().map(|e| (e.0 .0, e.1)).collect()
    }

    pub async fn get_entries<F>(&self, f: F) -> Vec<(u64, &Vec<u8>)>
    where
        F: Fn(u64) -> bool,
    {
        self.store
            .iter()
            .filter(|(&ref key, _)| f(key.0))
            .map(|e| (e.0 .0, e.1))
            .collect()
    }
}
