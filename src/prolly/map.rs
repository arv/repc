use crate::dag;
use crate::dag::chunk::Chunk;
use crate::dag::store::Store;
use flatbuffers;
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

type Result<T> = std::result::Result<T, Error>;

pub struct Map {
    store: Store,
    base: Option<Chunk>,
    pending: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
}

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
            base: self.base.and_then(|chunk| {
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
    fn nextBase(&mut self) -> Option<(&[u8], Option<&[u8]>)> {
        self.base.and_then(|baseIter| baseIter.next()).and_then(|baseEntry| {
          let k =   baseEntry.key();
          let v = baseEntry.val();

          if k.is_none() || v.is_none() {
              warn!("Corrupt db entry: {:?}", baseEntry);
              return self.nextBase();
          }

          Some((k.unwrap(), Some(v.unwrap())))
        })
    }

    fn nextPending(&mut self) -> Option<(&[u8], Option<&[u8]>)> {
        self.pending.next().and_then(|(nextKey, nextVal)| {
            Some((nextKey.as_slice(), nextVal.as_ref().and_then(|nv| Some(nv.as_slice()))))
        })
    }

    fn nextInternal(&mut self) -> Option<(&[u8], Option<&[u8]>)> {
        let baseKey = self.base.and_then(|i| i.peek()).and_then(|baseEntry| baseEntry.key());
        let pendingKey = self.pending.peek().and_then(|pendingEntry| Some((*pendingEntry).0.as_slice()));

        match pendingKey {
            None => self.nextBase(),
            Some(pendingKey) => {
                match baseKey {
                    None => self.nextPending(),
                    Some(baseKey) => {
                        if pendingKey < baseKey {
                            self.nextPending()
                        } else if baseKey < pendingKey {
                            self.nextBase()
                        } else {
                            self.nextPending();
                            self.nextBase()
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
            let foo = self.nextInternal();
            if foo.is_none() {
                return None
            }
            if foo.unwrap().1.is_some() {
                return Some(Self::Item{
                    key: foo.unwrap().0,
                    val: foo.unwrap().1.unwrap(),
                })
            }
            /*
            match foo {
                None => return None,
                Some((key, Some(val))) => return Some(Self::Item{key, val}),
                Some((key, None)) => (),
            }
            */
        }
    }
}

pub struct Entry<'a> {
    pub key: &'a [u8],
    pub val: &'a [u8],
}
