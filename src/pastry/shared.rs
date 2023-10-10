#[derive(Debug, Clone, PartialEq)]
pub struct KeyValuePair<T, U> {
    pub key: T,
    pub value: U,
}

impl<T, U> KeyValuePair<T, U> {
    pub fn new(key: T, value: U) -> Self {
        Self { key, value }
    }

    pub fn get_key(&self) -> &T {
        &self.key
    }

    pub fn get_value(&self) -> &U {
        &self.value
    }
}
