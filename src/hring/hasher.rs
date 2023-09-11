use crate::error::Result;
use sha2::{Digest, Sha256};
use std::hash::Hasher;

pub struct Sha256Hasher(Sha256);

impl Default for Sha256Hasher {
    fn default() -> Self {
        Sha256Hasher(Sha256::default())
    }
}

impl Hasher for Sha256Hasher {
    fn write(&mut self, bytes: &[u8]) {
        self.0.update(bytes);
    }

    fn finish(&self) -> u64 {
        let hash = self.0.clone().finalize();
        let hash_bytes = &hash[..8];
        let hash_array: [u8; 8] = hash_bytes.try_into().unwrap();
        u64::from_be_bytes(hash_array)
    }
}

impl Sha256Hasher {
    pub fn hash_once(bytes: &[u8]) -> u64 {
        let mut hasher = Sha256Hasher::default();
        hasher.write(bytes);
        hasher.finish()
    }
    pub fn hash_once_to_vec(bytes: &[u8]) -> Vec<u8> {
        let mut hasher = Sha256Hasher::default();
        hasher.0.update(bytes);
        let hash = hasher.0.clone().finalize();
        let hash_bytes = &hash[..8];
        hash_bytes.try_into().unwrap()
    }
}

#[test]
fn test_hash() -> Result<()> {
    let key = "key".to_owned();

    assert_eq!(
        Sha256Hasher::hash_once(key.as_bytes()),
        Sha256Hasher::hash_once(key.as_bytes())
    );
    assert_eq!(
        Sha256Hasher::hash_once_to_vec(key.as_bytes()),
        Sha256Hasher::hash_once_to_vec(key.as_bytes())
    );

    Ok(())
}
