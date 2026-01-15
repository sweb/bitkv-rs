use std::path::PathBuf;

use bitkv_rs::KvStore;

fn main() {
    let mut store = KvStore::open(PathBuf::from("data")).unwrap();

    store.set("user1".to_string(), "Alice".to_string()).unwrap();
    store.set("user2".to_string(), "Bob".to_string()).unwrap();

    let val = store.get("user1").unwrap();
    println!("Got value: {:?}", val); // Should be Some("Alice")
    store.remove("user1".to_string()).unwrap();
    let val = store.get("user1").unwrap();
    println!("Got value: {:?}", val); // Should be Some("Alice")
    store.compact().unwrap();
}
