use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug)]
pub enum Request {
    Get { key: String },
    Set { key: String, value: String },
    Remove { key: String }
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Response {
    Ok,
    Value(String),
    NotFound,
    Error(String)
}
