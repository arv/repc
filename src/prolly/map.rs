use crate::dag;
use crate::dag::store::Store;
use std::collections::BTreeMap;
use std::collections::btree_map::Iter as BTreeMapIter;
use std::iter::{Iterator, Peekable};
use super::Entry;
use super::leaf::Leaf;

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
    base: Option<Leaf>,
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

    pub fn iter<'a>(&'a mut self) -> impl Iterator<Item=Entry> {
        Iter{
            base: Leaf::iter(self.base.as_ref()).peekable(),
            pending: self.pending.iter().peekable(),
        }
    }
}

pub struct Iter<'a, LeafIter: Iterator<Item = Entry<'a>>> {
    base: Peekable<LeafIter>,
    pending: Peekable<BTreeMapIter<'a, Vec<u8>, Option<Vec<u8>>>>,
}

impl<'a, LeafIter: Iterator<Item = Entry<'a>>> Iter<'a, LeafIter> {
    fn next_base(&mut self) -> Option<DeletableEntry<'a>> {
        self.base.next().map(|e| DeletableEntry{key: e.key, val: Some(e.val)})
    }

    fn next_pending(&mut self) -> Option<DeletableEntry<'a>> {
        self.pending.next().map(|(key, val)| {
            DeletableEntry{key, val: val.as_ref().map(|v| v.as_slice())}
        })
    }

    fn next_internal(&mut self) -> Option<DeletableEntry<'a>> {
        let base_key = self.base.peek().map(|base_entry| base_entry.key);
        let pending_key = self.pending.peek().map(|pending_entry| (*pending_entry).0.as_slice());

        match pending_key {
            None => self.next_base(),
            Some(pending_key) => {
                match base_key {
                    None => self.next_pending(),
                    Some(base_key) => {
                        let mut r: Option<DeletableEntry<'a>> = None;
                        if pending_key <= base_key {
                            r = self.next_pending();
                        }
                        if base_key <= pending_key {
                            r = self.next_base();
                        }
                        r
                    }
                }
            }
        }
    }
}

impl<'a, LeafIter: Iterator<Item = Entry<'a>>> Iterator for Iter<'a, LeafIter> {
    type Item = Entry<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.next_internal() {
                None => return None,
                Some(DeletableEntry{key, val: Some(val)}) => return Some(Entry{key, val}),
                Some(DeletableEntry{key: _, val: None}) => (),
            }
        }
    }
}

pub struct DeletableEntry<'a> {
    pub key: &'a [u8],
    pub val: Option<&'a [u8]>,
}
