use std::path::PathBuf;

use bitkv_rs::KvStore;

fn main() {
    let mut store = KvStore::open(PathBuf::from("data")).unwrap();

    store.set("user1".to_string(), "Alice".to_string()).unwrap();
    store.set("user2".to_string(), "Bob".to_string()).unwrap();

    let val = store.get("user1".to_string()).unwrap();
    println!("Got value: {:?}", val); // Should be Some("Alice")
    store.remove("user1".to_string()).unwrap();
    let val = store.get("user1".to_string()).unwrap();
    println!("Got value: {:?}", val); // Should be Some("Alice")
    store.compact().unwrap();

    // Restart logic:
    // If you run this code twice, the file "log.db" will persist.
    // The second time you run it, `open` will replay the log
    // and remember "Alice" even before you set it again.
}
