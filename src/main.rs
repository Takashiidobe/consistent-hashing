use std::{
    collections::{hash_map::DefaultHasher, BTreeMap},
    hash::{Hash, Hasher},
    marker::PhantomData,
    sync::Mutex,
};

#[derive(Debug)]
struct HashRing<T, R> {
    keys: BTreeMap<u64, T>,
    hasher: Mutex<DefaultHasher>,
    data: PhantomData<R>,
}

impl<T: Hash + Clone, R: Hash> From<Vec<T>> for HashRing<T, R> {
    fn from(value: Vec<T>) -> Self {
        let mut hash_ring = HashRing {
            keys: Default::default(),
            hasher: Mutex::new(DefaultHasher::new()),
            data: PhantomData,
        };
        for val in value {
            hash_ring.add_node(val);
        }
        hash_ring
    }
}

impl<T, R> Default for HashRing<T, R> {
    fn default() -> Self {
        HashRing {
            keys: Default::default(),
            hasher: Mutex::new(DefaultHasher::new()),
            data: PhantomData,
        }
    }
}

impl<T: Hash + Clone, R: Hash> HashRing<T, R> {
    pub fn add_node(&mut self, node: T) {
        let mut hasher = self.hasher.lock().unwrap().to_owned();
        node.hash(&mut hasher);
        let hash_key = hasher.finish();

        self.keys.insert(hash_key, node);
    }

    pub fn remove_node(&mut self, node: &T) {
        let mut hasher = self.hasher.lock().unwrap().to_owned();
        node.hash(&mut hasher);
        let hash_key = hasher.finish();

        if self.keys.is_empty() {
            return;
        }

        let node_to_remove = *self.keys.range(hash_key..).next().unwrap().0;
        self.keys.remove(&node_to_remove);
    }

    pub fn get_node(&self, key: &R) -> Option<&T> {
        if self.keys.is_empty() {
            return None;
        }

        let mut hasher = self.hasher.lock().unwrap().to_owned();
        key.hash(&mut hasher);
        let hash_key = hasher.finish();

        for key in &self.keys {
            if hash_key <= *key.0 {
                return Some(key.1);
            }
        }

        Some(self.keys.first_key_value().unwrap().1)
    }
}

#[derive(Debug, Clone, Hash, Eq, PartialEq, PartialOrd, Ord)]
struct Port<'a> {
    host: &'a str,
    port: u32,
}

fn main() {
    let nodes: Vec<Port> = vec![
        Port {
            host: "www.google.com",
            port: 443,
        },
        Port {
            host: "www.facebook.com",
            port: 80,
        },
        Port {
            host: "www.amazon.com",
            port: 1994,
        },
        Port {
            host: "www.netflix.com",
            port: 2001,
        },
        Port {
            host: "www.youtube.com",
            port: 2005,
        },
        Port {
            host: "www.runescape.com",
            port: 2001,
        },
    ];

    let mut hash_ring: HashRing<Port, String> = HashRing::from(nodes);

    println!(
        "Key: 'hello', Node: {:?}",
        hash_ring.get_node(&("hello").to_string()).unwrap()
    );

    println!(
        "Key: 'dude', Node: {:?}",
        hash_ring.get_node(&("dude").to_string()).unwrap()
    );

    println!(
        "Key: 'martian', Node: {:?}",
        hash_ring.get_node(&("martian").to_string()).unwrap()
    );

    println!(
        "Key: 'tardis', Node: {:?}",
        hash_ring.get_node(&("tardis").to_string()).unwrap()
    );

    hash_ring.remove_node(&Port {
        host: "localhost",
        port: 15329,
    });

    println!(
        "Key: 'hello', Node: {:?}",
        hash_ring.get_node(&("hello").to_string()).unwrap()
    );

    hash_ring.add_node(Port {
        host: "localhost",
        port: 15329,
    });

    println!(
        "Key: 'hello', Node: {:?}",
        hash_ring.get_node(&("hello").to_string()).unwrap()
    );

    println!(
        "Key: 'blah', Node: {:?}",
        hash_ring.get_node(&("blah").to_string()).unwrap()
    );
}
