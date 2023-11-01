use std::collections::HashMap;

#[derive(Debug)]
pub struct Store {
    store: HashMap<u64, Vec<u8>>,
}

impl Store {
    pub fn new() -> Self {
        Store {
            store: HashMap::new(),
        }
    }

    pub fn get(&self, key: &u64) -> Option<&Vec<u8>> {
        self.store.get(key)
    }

    pub fn set(&mut self, key: &u64, value: &[u8]) -> Option<Vec<u8>> {
        self.store.insert(*key, value.to_vec())
    }

    pub fn delete(&mut self, key: &u64) -> Option<Vec<u8>> {
        self.store.remove(key)
    }

    pub fn list(&self) -> Vec<(&u64, &Vec<u8>)> {
        self.store.iter().collect()
    }

    pub async fn get_entries<F>(&self, f: F) -> Vec<(&u64, &Vec<u8>)>
    where
        F: Fn(u64) -> bool,
    {
        self.store.iter().filter(|(&ref key, _)| f(*key)).collect()
    }
}
