use bitkv_rs::KvStore;
use std::fs;
use std::path::PathBuf;

#[test]
fn test_cloned_store_updates_writer() {
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let store = KvStore::open(temp_dir.path().to_path_buf()).expect("open store");

    let writer_store = store.clone();
    for i in 0..50 {
        writer_store
            .clone()
            .set(format!("key{}", i), format!("value{}", i))
            .expect("set value");
    }

    let initial_count = count_db_files(temp_dir.path().to_path_buf());
    println!("Initial db files: {}", initial_count);
    assert!(initial_count >= 2, "Should have rotated at least once");

    for i in 0..10 {
        let mut s = store.clone();
        s.set(format!("newkey{}", i), format!("newvalue{}", i))
            .expect("set value");
    }

    let final_count = count_db_files(temp_dir.path().to_path_buf());
    println!("Final db files: {}", final_count);

    assert!(
        final_count - initial_count <= 1,
        "Too many files created! Potential stale writer bug. Initial: {}, Final: {}",
        initial_count,
        final_count
    );
}

fn count_db_files(dir: PathBuf) -> usize {
    fs::read_dir(dir)
        .expect("read dir")
        .filter_map(|entry| {
            let path = entry.ok()?.path();
            if path.extension()? == "db" {
                Some(1)
            } else {
                None
            }
        })
        .count()
}
