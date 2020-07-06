use crate::dag;
use crate::dag::store::Store;
use super::leaf::Leaf;
use super::leaf_generated::leaf;
use std::collections::BTreeMap;

pub enum Error {
    Storage(dag::Error),
}

impl From<dag::Error> for Error {
    fn from(err: dag::Error) -> Error {
        Error::Storage(err)
    }
}

type Result<T> = std::result::Result<T, Error>;

pub struct Map {
    store: Store,
    root: Leaf,
    mutations: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl Map {
    pub async fn new() -> Map {
    }

    pub async fn read(chunk: Chunk) -> {
        // do we need to store the chunk to have its hash
    }

    pub async fn has(&self, key: &[u8]) -> Result<bool> {
        match &mut self.root.entries() {
            Some(entries) => {
                Ok(entries.any(|entry| {
                    if let Some(cand) = entry.key() {
                        return cand == key;
                    }
                    return false;
                }))
            },
            None => Ok(false),
        }
    }

    pub async fn get<'a>(&'a self, key: &[u8]) -> Result<Option<leaf::LeafEntry<'a>>> {
        match &mut self.root.entries() {
            Some(entries) => {
                Ok(entries.find(|entry| {
                    if let Some(cand) = entry.key() {
                        return cand == key;
                    }
                    return false;
                }))
            },
            None => Ok(None),
        }
    }
}

struct Mutation {
    key: Vec<u8>,
    val: Option<Vec<u8>>,
}
