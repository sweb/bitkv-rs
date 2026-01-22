use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use bitkv_rs::KvStore;
use bitkv_rs::protocol::{Request, Response};
use std::path::PathBuf;


#[tokio::main]
async fn main() -> std::io::Result<()> {
    let store = KvStore::open(PathBuf::from("./data"))?;
    let address = "127.0.0.1:6379";
    println!("BitKV server started on {}", address);
    let listener = TcpListener::bind(address).await?;

    loop {
        let (socket, _) = listener.accept().await?;
        println!("Accepted connection");
        let store = store.clone();
        tokio::spawn(async move {
            if let Err(e) = process_connection(socket, store).await {
                eprintln!("Connection error: {}", e);
            }
        });
    }
}

async fn process_connection(mut socket: TcpStream, store: KvStore) -> std::io::Result<()> {
    println!("Processing connection...");
    let (reader, mut writer) = socket.split();
    let stream = BufReader::new(reader);
    let mut lines = stream.lines();
    while let Ok(Some(line)) = lines.next_line().await {
            let req: Request = match serde_json::from_str(&line) {
                Ok(req) => req,
                Err(e) => {
                    let resp = Response::Error(format!("Invalid Request: {}", e));
                    let resp_json = serde_json::to_string(&resp)?;
                    writer.write_all(resp_json.as_bytes()).await?;
                    writer.write_all(b"\n").await?;
                    continue;
                }
            };
        let response = execute_request(req, store.clone()).await;

        let resp_json = serde_json::to_string(&response)?;
        writer.write_all(resp_json.as_bytes()).await?;
        writer.write_all(b"\n").await?;
    }
    Ok(())
}

async fn execute_request(req: Request, mut store: KvStore) -> Response {
    let result = tokio::task::spawn_blocking(move || {
        match req {
            Request::Get { key } => match store.get(&key) {
                Ok(Some(v)) => Response::Value(v),
                Ok(None) => Response::NotFound,
                Err(e) => Response::Error(e.to_string()),
            },
            Request::Set { key, value } => match store.set(key, value) {
                Ok(_) => Response::Ok,
                Err(e) => Response::Error(e.to_string()),
            },
            Request::Remove { key } => match store.remove(key) {
                Ok(_) => Response::Ok,
                Err(e) => Response::Error(e.to_string()),
            },
        }
    }).await;
    match result {
        Ok(response) => response,
        Err(e) => Response::Error(format!("Internal server error: {}", e)),
    }
}