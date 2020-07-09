use crate::dag;
use crate::dag::chunk::Chunk;
use crate::dag::store::Store;
use log::warn;
use std::collections::BTreeMap;
use std::collections::btree_map::Iter as BTreeMapIter;
use std::iter::{Iterator, Peekable};
use super::leaf_generated::leaf;

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
pub struct Map {
    store: Store,
    base: Option<Chunk>,
    pending: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
}

#[allow(dead_code)]
impl Map {
    pub async fn new(store: Store) -> Map {
        Map{
            store,
            base: None,
            pending: BTreeMap::new(),
        }
    }

    pub fn put(&mut self, key: Vec<u8>, val: Vec<u8>) {
        self.pending.insert(key, Some(val));
    }

    pub fn del(&mut self, key: Vec<u8>) {
        self.pending.insert(key, None);
    }

    fn iter<'a>(&'a mut self) -> impl Iterator<Item=Entry> {
        Iter{
            base: self.base.as_ref().and_then(|chunk| {
                leaf::get_root_as_leaf(chunk.data()).entries()
            }).and_then(|entries| {
                Some(entries.iter().peekable())
            }),
            pending: self.pending.iter().peekable(),
        }
    }
}

pub struct Iter<'a, BaseIter: Iterator<Item=leaf::LeafEntry<'a>>> {
    base: Option<Peekable<BaseIter>>,
    pending: Peekable<BTreeMapIter<'a, Vec<u8>, Option<Vec<u8>>>>,
}

impl<'a, BaseIter: Iterator<Item=leaf::LeafEntry<'a>>> Iter<'a, BaseIter> {
    fn next_base(&mut self) -> Option<(&'a [u8], Option<&'a [u8]>)> {
        self.base.as_mut().and_then(|base_iter| base_iter.next()).and_then(|base_entry| {
          let k =   base_entry.key();
          let v = base_entry.val();

          if k.is_none() || v.is_none() {
              warn!("Corrupt db entry: {:?}", base_entry);
              return self.next_base();
          }

          Some((k.unwrap(), Some(v.unwrap())))
        })
    }

    fn next_pending(&mut self) -> Option<(&'a [u8], Option<&'a[u8]>)> {
        self.pending.next().and_then(|(next_key, next_val)| {
            Some((next_key.as_slice(), next_val.as_ref().and_then(|nv| Some(nv.as_slice()))))
        })
    }

    fn next_internal(&mut self) -> Option<(&'a [u8], Option<&'a [u8]>)> {
        let base_key = self.base.as_mut().and_then(|i| i.peek()).and_then(|base_entry| base_entry.key());
        let pending_key = self.pending.peek().and_then(|pending_entry| Some((*pending_entry).0.as_slice()));

        match pending_key {
            None => self.next_base(),
            Some(pending_key) => {
                match base_key {
                    None => self.next_pending(),
                    Some(base_key) => {
                        if pending_key < base_key {
                            self.next_pending()
                        } else if base_key < pending_key {
                            self.next_base()
                        } else {
                            self.next_pending();
                            self.next_base()
                        }
                    }
                }
            }
        }
    }
}

impl<'a, BaseIter: Iterator<Item=leaf::LeafEntry<'a>>> Iterator for Iter<'a, BaseIter> {
    type Item = Entry<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.next_internal() {
                None => return None,
                Some((key, Some(val))) => return Some(Self::Item{key, val}),
                Some((_, None)) => (),
            }
        }
    }
}

pub struct Entry<'a> {
    pub key: &'a [u8],
    pub val: &'a [u8],
}
