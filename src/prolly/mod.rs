mod buzhash;
pub mod chunker;
mod leaf;
#[allow(unused_imports)]
mod leaf_generated;
pub mod map;

use crate::dag;

#[derive(Debug)]
pub enum Error {
    Storage(dag::Error),
}

impl From<dag::Error> for Error {
    fn from(err: dag::Error) -> Error {
        Error::Storage(err)
    }
}

#[allow(dead_code)]
type Result<T> = std::result::Result<T, Error>;

#[allow(dead_code)]
pub struct Entry<'a> {
    pub key: &'a [u8],
    pub val: &'a [u8],
}
