mod buzhash;
pub mod chunker;
mod leaf;
#[allow(unused_imports)]
mod leaf_generated;
pub mod map;

#[allow(dead_code)]
pub struct Entry<'a> {
    pub key: &'a [u8],
    pub val: &'a [u8],
}
