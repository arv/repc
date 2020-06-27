use crate::kv::idbstore::IdbStore;
use crate::kv::Store;
use async_std::sync::{channel, Receiver, Sender};
use nanoserde::{DeJson, SerJson};
use std::collections::HashMap;
use std::sync::Mutex;
use wasm_bindgen_futures::spawn_local;

struct Request {
    db_name: String,
    rpc: String,
    data: String,
    response: Sender<Response>,
}

type Response = Result<String, String>;

lazy_static! {
    static ref SENDER: Mutex<Sender::<Request>> = {
        let (tx, rx) = channel::<Request>(1);
        spawn_local(dispatch_loop(rx));
        Mutex::new(tx)
    };
}

async fn dispatch_loop(rx: Receiver<Request>) {
    let mut dispatcher = Dispatcher {
        connections: HashMap::new(),
    };

    loop {
        match rx.recv().await {
            Err(why) => log!("Recv failed: {}", why),
            Ok(req) => {
                let response = match req.rpc.as_str() {
                    "open" => Some(dispatcher.open(&req).await),
                    "close" => Some(dispatcher.close(&req).await),
                    "debug" => Some(dispatcher.debug(&req).await),
                    _ => None,
                };
                if let Some(response) = response {
                    req.response.send(response).await;
                    continue;
                }
                let db = {
                    match dispatcher.connections.get(&req.db_name[..]) {
                        Some(v) => v,
                        None => {
                            let err = Err(format!("\"{}\" not open", req.db_name));
                            req.response.send(err).await;
                            continue;
                        }
                    }
                };
                let response = match req.rpc.as_str() {
                    "has" => dispatcher.has(&**db, &req.data).await,
                    "get" => dispatcher.get(&**db, &req.data).await,
                    "put" => dispatcher.put(&**db, &req.data).await,
                    _ => Err("Unsupported rpc name".to_string()),
                };
                req.response.send(response).await;
            }
        }
    }
}

#[derive(DeJson)]
struct GetRequest {
    key: String,
}

#[derive(SerJson)]
struct GetResponse {
    value: Option<String>,
    has: bool, // Second to avoid trailing comma if value == None.
}

#[derive(DeJson)]
struct PutRequest {
    key: String,
    value: String,
}

struct Dispatcher {
    connections: HashMap<String, Box<dyn Store>>,
}

impl Dispatcher {
    async fn open(&mut self, req: &Request) -> Response {
        if req.db_name.is_empty() {
            return Err("db_name must be non-empty".to_string());
        }
        if self.connections.contains_key(&req.db_name[..]) {
            return Ok("".to_string());
        }
        match IdbStore::new(&req.db_name[..]).await {
            Err(e) => {
                log!("Failed to open! {}", e);
                return Err(format!("Failed to open: {}", e));
            }
            Ok(v) => {
                if let Some(v) = v {
                    self.connections.insert(req.db_name.clone(), Box::new(v));
                }
            }
        }
        Ok("".to_string())
    }

    async fn close(&mut self, req: &Request) -> Response {
        if !self.connections.contains_key(&req.db_name[..]) {
            return Ok("".to_string());
        }
        self.connections.remove(&req.db_name);

        Ok("".to_string())
    }

    async fn has(&self, db: &dyn Store, data: &String) -> Response {
        let req: GetRequest = match DeJson::deserialize_json(data) {
            Ok(v) => v,
            Err(_) => return Err("Failed to parse request".into()),
        };
        match db.has(&req.key).await {
            Ok(v) => Ok(SerJson::serialize_json(&GetResponse {
                has: v,
                value: None,
            })),
            Err(e) => Err(format!("{}", e)),
        }
    }

    async fn get(&self, db: &dyn Store, data: &String) -> Response {
        let req: GetRequest = match DeJson::deserialize_json(data) {
            Ok(v) => v,
            Err(_) => return Err("Failed to parse request".into()),
        };
        match db.get(&req.key).await {
            Ok(Some(v)) => match std::str::from_utf8(&v[..]) {
                Ok(v) => Ok(SerJson::serialize_json(&GetResponse {
                    has: true,
                    value: Some(v.to_string()),
                })),
                Err(e) => Err(e.to_string()),
            },
            Ok(None) => Ok(SerJson::serialize_json(&GetResponse {
                has: false,
                value: None,
            })),
            Err(e) => Err(format!("{}", e)),
        }
    }

    async fn put(&self, db: &dyn Store, data: &String) -> Response {
        let req: PutRequest = match DeJson::deserialize_json(data) {
            Ok(v) => v,
            Err(_) => return Err("Failed to parse request".into()),
        };
        match db.put(&req.key, &req.value.into_bytes()).await {
            Ok(_) => Ok("".into()),
            Err(e) => Err(format!("{}", e)),
        }
    }

    async fn debug(&mut self, req: &Request) -> Response {
        match req.data.as_str() {
            "open_dbs" => Ok(format!("{:?}", self.connections.keys())),
            _ => Err("Debug command not defined".to_string()),
        }
    }
}

pub async fn dispatch(db_name: String, rpc: String, data: String) -> Response {
    let (tx, rx) = channel::<Response>(1);
    let request = Request {
        db_name,
        rpc,
        data,
        response: tx,
    };
    match SENDER.lock() {
        Ok(v) => v.send(request).await,
        Err(v) => return Err(v.to_string()),
    }
    match rx.recv().await {
        Err(v) => Err(v.to_string()),
        Ok(v) => v,
    }
}