use crate::dag::chunk::Chunk;
use super::leaf_generated::leaf;
use flatbuffers;
use flatbuffers::FlatBufferBuilder;

// Leaf is a leaf level node in the map tree structure.
#[derive(Debug)]
pub struct Leaf {
    chunk: Chunk,
}

impl Leaf {
    pub fn entries<'a>(&'a self) -> Option<impl Iterator<Item = leaf::LeafEntry<'a>>> {
        match leaf::get_root_as_leaf(self.chunk.data()).entries() {
            Some(entries) => Some(entries.iter()),
            None => None,
        }
    }
}
