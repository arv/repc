use crate::dag::chunk::Chunk;
use flatbuffers::FlatBufferBuilder;
use super::Entry;
use super::leaf_generated::leaf;
use log::warn;

// Leaf is a leaf level node in the map tree structure.
// It wraps a chunk containing a flatbuffer and exposes handy
// utilities to inspect the buffer more easily.
#[derive(Debug)]
pub struct Leaf {
    chunk: Chunk,
}

#[allow(dead_code)]
impl Leaf {
    pub fn chunk(&self) -> &Chunk {
        &self.chunk
    }

    pub fn new<'a>(entries: impl Iterator<Item = Entry<'a>>) -> Leaf {
        let mut builder = FlatBufferBuilder::default();
        let entries = entries.map(|e| {
            let builder = &mut builder;
            let args = &leaf::LeafEntryArgs{
                key: Some(builder.create_vector(e.key)),
                val: Some(builder.create_vector(e.val)),
            };
            leaf::LeafEntry::create(builder, args)
        }).collect::<Vec<flatbuffers::WIPOffset<leaf::LeafEntry>>>();
        let entries = builder.create_vector(&entries);
        let root = leaf::Leaf::create(&mut builder, &leaf::LeafArgs{
            entries: Some(entries),
        });
        builder.finish(root, None);

        Leaf{
            chunk: Chunk::new( builder.collapse(), &vec![]),
        }
    }

    pub fn iter<'a>(s: Option<&'a Self>) -> impl Iterator<Item = Entry<'a>> {
        let root = s.map(|leaf| leaf::get_root_as_leaf(leaf.chunk.data()));
        LeafIter{
            fb_iter: root.and_then(|r| r.entries()).map(|e| e.iter()),
        }
    }
}

// LeafIter simplifies iteration over the leaf entries. Unfortunately it needs to be
// generic because the type returned by flatbuffer::Vector<T>::iter(). The only way
// to encapsulate that type appears to be by making it generic.
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
