use std::{
    collections::HashMap,
    fs::{self, File},
    io::{self, BufReader, BufWriter, Read, Result, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, RwLock},
};

use serde::{Deserialize, Serialize};

const SPLIT_LIMIT: u64 = 1 * 1024; // 1 KB
const COMPACT_LIMIT: u64 = 5;

#[derive(Serialize, Deserialize, Debug)]
enum Command {
    Set { key: String, value: String },
    Remove { key: String },
}

#[derive(Clone, Copy, Debug)]
struct CommandPos {
    pos: u64,
    len: u64,
    generation: u64,
}

#[derive(Clone)]
pub struct KvStore {
    inner: Arc<RwLock<SharedData>>,
    writer: Arc<Mutex<BufWriter<fs::File>>>,
}

struct SharedData {
    index: HashMap<String, CommandPos>,
    directory: PathBuf,
    readers: HashMap<u64, BufReader<fs::File>>,
    current_generation: u64,
}

impl KvStore {
    pub fn open(directory: PathBuf) -> io::Result<Self> {
        fs::create_dir_all(&directory)?;
        let generation_files = fs::read_dir(&directory)?;
        let mut readers = HashMap::new();
        for dir_entry in generation_files {
            let path = dir_entry?.path();
            if path.extension() != Some(std::ffi::OsStr::new("db")) {
                continue;
            }
            let generation = match path
                .file_stem()
                .and_then(|s| s.to_str())
                .and_then(|s| s.parse::<u64>().ok())
            {
                Some(g) => g,
                None => continue,
            };
            let file = fs::OpenOptions::new().read(true).open(path)?;
            readers.insert(generation, BufReader::new(file));
        }
        // We always create a new generation on start up
        let current_generation = readers.keys().max().copied().unwrap_or(0) + 1;
        let (writer, reader) = new_log_file(&directory, current_generation)?;
        readers.insert(current_generation, reader);

        let index = HashMap::new();
        let data = SharedData {
            index,
            directory,
            readers,
            current_generation,
        };
        let mut store = KvStore {
            inner: Arc::new(RwLock::new(data)),
            writer: Arc::new(Mutex::new(writer)),
        };
        store.load()?;
        Ok(store)
    }

    fn load(&mut self) -> io::Result<()> {
        let mut inner_guard = self.inner.write().unwrap();
        let SharedData {
            ref mut readers,
            ref mut index,
            ..
        } = *inner_guard;
        let mut generations: Vec<u64> = readers.keys().copied().collect();
        generations.sort();

        for generation in generations {
            if let Some(reader) = readers.get_mut(&generation) {
                let mut pos = reader.seek(SeekFrom::Start(0))?;
                let mut stream =
                    serde_json::Deserializer::from_reader(reader).into_iter::<Command>();

                while let Some(command) = stream.next() {
                    let c = command?;
                    let new_pos = stream.byte_offset() as u64;
                    let len = new_pos - pos;
                    match c {
                        Command::Set { key, .. } => {
                            let cmd_pos = CommandPos {
                                pos,
                                len,
                                generation,
                            };
                            index.insert(key, cmd_pos);
                        }
                        Command::Remove { key } => {
                            index.remove(&key);
                        }
                    }
                    pos = new_pos;
                }
            }
        }
        Ok(())
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let cmd = Command::Set {
            key: key.clone(),
            value,
        };
        let mut writer_guard = self.writer.lock().unwrap();
        let mut pos = writer_guard.stream_position()?;
        let mut inner = self.inner.write().unwrap();
        if pos > SPLIT_LIMIT {
            drop(writer_guard);
            if inner.readers.len() as u64 > COMPACT_LIMIT {
                drop(inner);
                self.compact()?;
                writer_guard = self.writer.lock().unwrap();
                pos = writer_guard.stream_position()?;
                inner = self.inner.write().unwrap();
            } else {
                let new_generation = inner.current_generation + 1;
                let (writer, reader) = new_log_file(&inner.directory, new_generation)?;
                inner.readers.insert(new_generation, reader);
                inner.current_generation = new_generation;
                self.writer = Arc::new(Mutex::new(writer));
                writer_guard = self.writer.lock().unwrap();
                pos = writer_guard.stream_position()?;
            }
        }
        serde_json::to_writer(&mut *writer_guard, &cmd)?;
        writer_guard.flush()?;
        let ending_position = writer_guard.stream_position()?;
        let len = ending_position - pos;

        let generation = inner.current_generation;
        inner.index.insert(
            key,
            CommandPos {
                pos,
                len,
                generation,
            },
        );
        Ok(())
    }

    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        let mut inner = self.inner.write().unwrap();
        let cmd_pos = match inner.index.get(&key) {
            Some(value) => *value,
            None => return Ok(None),
        };
        if let Some(reader) = inner.readers.get_mut(&cmd_pos.generation) {
            reader.seek(SeekFrom::Start(cmd_pos.pos))?;
            let reader = reader.take(cmd_pos.len);
            let cmd = serde_json::from_reader(reader)?;
            match cmd {
                Command::Set { value, .. } => Ok(Some(value)),
                _ => Ok(None),
            }
        } else {
            Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("Log file for generation {} not found", cmd_pos.generation),
            ))
        }
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        let cmd = Command::Remove { key: key.clone() };
        let pos = self.writer.lock().unwrap().stream_position()?;
        if pos > SPLIT_LIMIT {
            let mut inner = self.inner.write().unwrap();
            if inner.readers.len() as u64 > COMPACT_LIMIT {
                drop(inner);
                self.compact()?;
            } else {
                let new_generation = inner.current_generation + 1;
                let (writer, reader) = new_log_file(&inner.directory, new_generation)?;

                self.writer = Arc::new(Mutex::new(writer));
                inner.readers.insert(new_generation, reader);
                inner.current_generation = new_generation;
            }
        }
        let mut writer_guard = self.writer.lock().unwrap();
        serde_json::to_writer(&mut *writer_guard, &cmd)?;
        let mut inner = self.inner.write().unwrap();
        inner.index.remove(&key);
        Ok(())
    }

    pub fn compact(&mut self) -> Result<()> {
        let mut inner = self.inner.write().unwrap();
        let compaction_generation = inner.current_generation + 1;
        inner.current_generation += 2;
        let (writer, reader) = new_log_file(&inner.directory, inner.current_generation)?;
        self.writer = Arc::new(Mutex::new(writer));
        let current_generation = inner.current_generation;
        inner.readers.insert(current_generation, reader);

        let (mut comp_writer, comp_reader) = new_log_file(&inner.directory, compaction_generation)?;
        let mut compaction_generations: Vec<u64> = inner
            .readers
            .keys()
            .copied()
            .filter(|g| g < &compaction_generation)
            .collect();
        compaction_generations.sort();
        let thread_generations = compaction_generations.clone();
        let thread_inner = self.inner.clone();
        let directory = inner.directory.clone();
        std::thread::spawn(move || {
            let try_compact = || -> std::io::Result<()> {
                let mut compacted_map: HashMap<String, String> = HashMap::new();
                for gen_id in &thread_generations {
                    let path = directory.join(format!("{}.db", gen_id));
                    let reader = BufReader::new(fs::OpenOptions::new().read(true).open(&path)?);
                    let mut stream =
                        serde_json::Deserializer::from_reader(reader).into_iter::<Command>();

                    while let Some(command) = stream.next() {
                        match command? {
                            Command::Set { key, value } => {
                                compacted_map.insert(key, value);
                            }
                            Command::Remove { key } => {
                                compacted_map.remove(&key);
                            }
                        }
                    }
                }
                let mut new_pos_map = HashMap::new();
                for (key, value) in compacted_map {
                    let pos = comp_writer.stream_position()?;
                    let cmd = Command::Set {
                        key: key.clone(),
                        value,
                    };
                    serde_json::to_writer(&mut comp_writer, &cmd)?;
                    let len = comp_writer.stream_position()? - pos;
                    new_pos_map.insert(
                        key,
                        CommandPos {
                            pos,
                            len,
                            generation: compaction_generation,
                        },
                    );
                }
                comp_writer.flush()?;
                let mut inner_guard = thread_inner.write().unwrap();
                for gen_id in &thread_generations {
                    inner_guard.readers.remove(&gen_id);
                }
                inner_guard
                    .readers
                    .insert(compaction_generation, comp_reader);
                for (k, new_pos) in new_pos_map {
                    if let Some(current_pos) = inner_guard.index.get(&k) {
                        if thread_generations.contains(&current_pos.generation) {
                            inner_guard.index.insert(k, new_pos);
                        }
                    }
                }
                for gen_id in &thread_generations {
                    fs::remove_file(directory.join(format!("{}.db", gen_id)))?;
                }
                Ok(())
            };
            if let Err(e) = try_compact() {
                eprintln!("Compaction failed: {}", e);
            }
        });
        Ok(())
    }
}

fn new_log_file(dir: &Path, generation: u64) -> io::Result<(BufWriter<File>, BufReader<File>)> {
    let path = dir.join(format!("{}.db", generation));
    let writer = BufWriter::new(
        fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(&path)?,
    );
    let reader = BufReader::new(fs::OpenOptions::new().read(true).open(&path)?);
    Ok((writer, reader))
}
