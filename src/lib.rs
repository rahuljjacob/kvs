use std::collections::HashMap;

pub struct KvStore {
    scores: HashMap<String, String>,
}

impl KvStore {
    pub fn new() -> KvStore {
        KvStore {
            scores: HashMap::new(),
        }
    }

    pub fn get(&self, key: String) -> Option<String> {
        self.scores.get(&key).cloned()
    }

    pub fn set(&mut self, key: String, value: String) {
        self.scores.insert(key, value);
    }

    pub fn remove(&mut self, key: String) {
        self.scores.remove(&key);
    }
}
