use crate::dag::chunk::Chunk;
use super::Entry;
use super::leaf_generated::leaf;
use log::warn;

// Leaf is a leaf level node in the map tree structure.
#[derive(Debug)]
pub struct Leaf {
    chunk: Chunk,
}

#[allow(dead_code)]
impl Leaf {
    pub fn iter<'a>(s: Option<&'a Self>) -> impl Iterator<Item = Entry<'a>> {
        let root = s.map(|leaf| leaf::get_root_as_leaf(leaf.chunk.data()));
        LeafIter{
            fb_iter: root.and_then(|r| r.entries()).map(|e| e.iter()),
        }
    }
}

#[allow(dead_code)]
struct LeafIter<'a, FBIter: Iterator<Item=leaf::LeafEntry<'a>>> {
    fb_iter: Option<FBIter>,
}

impl<'a, FBIter: Iterator<Item=leaf::LeafEntry<'a>>> Iterator for LeafIter<'a, FBIter> {
    type Item = Entry<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.fb_iter.as_mut() {
            None => None,
            Some(fb_iter) => {
                match fb_iter.next() {
                    None => None,
                    Some(leaf_entry) => leaf_entry.into(),
                }
            }
        }
    }
}

impl<'a> From<leaf::LeafEntry<'a>> for Option<Entry<'a>> {
    fn from(leaf_entry: leaf::LeafEntry<'a>) -> Self {
        let key = leaf_entry.key();
        let val = leaf_entry.val();
        if key.is_none() || val.is_none() {
            warn!("Corrupt entry: {:?}", leaf_entry);
            return None
        }
        Some(Entry{
            key: key.unwrap(),
            val: val.unwrap(),
        })
    }
}
