use std::{
    collections::HashMap,
    fs::{self, File},
    io::{self, BufReader, BufWriter, Read, Result, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

const SPLIT_LIMIT: u64 = 1 * 1024; // 1 KB

#[derive(Serialize, Deserialize, Debug)]
enum Command {
    Set { key: String, value: String },
    Remove { key: String },
}

struct CommandPos {
    pos: u64,
    len: u64,
    generation: u64,
}

pub struct KvStore {
    index: HashMap<String, CommandPos>,
    directory: PathBuf,
    readers: HashMap<u64, BufReader<fs::File>>,
    writer: BufWriter<fs::File>,
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
        let mut store = KvStore {
            index,
            directory,
            readers,
            current_generation,
            writer,
        };
        store.load()?;
        Ok(store)
    }

    fn load(&mut self) -> io::Result<()> {
        let mut generations: Vec<u64> = self.readers.keys().copied().collect();
        generations.sort();

        for generation in generations {
            if let Some(reader) = self.readers.get_mut(&generation) {
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
                            self.index.insert(key, cmd_pos);
                        }
                        Command::Remove { key } => {
                            self.index.remove(&key);
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
        let mut pos = self.writer.stream_position()?;
        if pos > SPLIT_LIMIT {
            let new_generation = self.current_generation + 1;
            let (writer, reader) = new_log_file(&self.directory, new_generation)?;

            self.writer = writer;
            self.readers.insert(new_generation, reader);
            self.current_generation = new_generation;
            pos = self.writer.stream_position()?;
        }
        serde_json::to_writer(&mut self.writer, &cmd)?;
        self.writer.flush()?;
        let ending_position = self.writer.seek(SeekFrom::End(0))?;
        let len = ending_position - pos;
        self.index.insert(
            key,
            CommandPos {
                pos,
                len,
                generation: self.current_generation,
            },
        );
        Ok(())
    }

    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        match self.index.get(&key) {
            Some(cmd_pos) => {
                if let Some(reader) = self.readers.get_mut(&cmd_pos.generation) {
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
            None => Ok(None),
        }
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        let cmd = Command::Remove { key: key.clone() };
        let pos = self.writer.stream_position()?;
        if pos > SPLIT_LIMIT {
            let new_generation = self.current_generation + 1;
            let (writer, reader) = new_log_file(&self.directory, new_generation)?;

            self.writer = writer;
            self.readers.insert(new_generation, reader);
            self.current_generation = new_generation;
        }
        serde_json::to_writer(&mut self.writer, &cmd)?;
        self.index.remove(&key);
        Ok(())
    }

    pub fn compact(&mut self) -> Result<()> {
        let compaction_generation = self.current_generation + 1;
        self.current_generation += 2;
        let (writer, reader) = new_log_file(&self.directory, self.current_generation)?;
        self.writer = writer;
        self.readers.insert(self.current_generation, reader);

        let (mut comp_writer, comp_reader) = new_log_file(&self.directory, compaction_generation)?;
        let mut compaction_generations: Vec<u64> = self
            .readers
            .keys()
            .copied()
            .filter(|g| g < &compaction_generation)
            .collect();
        compaction_generations.sort();
        let mut new_index: HashMap<String, CommandPos> = HashMap::new();
        for gen_id in &compaction_generations {
            let path = self.directory.join(format!("{}.db", gen_id));
            let mut reader = BufReader::new(fs::OpenOptions::new().read(true).open(&path)?);
            let mut read_pos = reader.seek(SeekFrom::Start(0))?;
            let mut stream = serde_json::Deserializer::from_reader(reader).into_iter::<Command>();

            while let Some(command) = stream.next() {
                let c = command?;
                if let Command::Set { key, .. } = &c {
                    if let Some(live_record) = self.index.get(key) {
                        if live_record.generation == *gen_id && live_record.pos == read_pos {
                            let pos = comp_writer.stream_position()?;
                            serde_json::to_writer(&mut comp_writer, &c)?;
                            let new_pos = comp_writer.stream_position()?;
                            let len = new_pos - pos;
                            let new_cmd_pos = CommandPos {
                                pos,
                                len,
                                generation: compaction_generation,
                            };
                            new_index.insert(key.clone(), new_cmd_pos);
                        }
                    }
                }
                read_pos = stream.byte_offset() as u64;
            }
        }
        comp_writer.flush()?;
        for gen_id in &compaction_generations {
            self.readers.remove(&gen_id);
        }
        for (k, v) in new_index {
            self.index.insert(k, v);
        }
        self.readers.insert(compaction_generation, comp_reader);
        for gen_id in &compaction_generations {
            fs::remove_file(self.directory.join(format!("{}.db", gen_id)))?;
        }
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
