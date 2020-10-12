use super::key::Key;
use super::{chunk::Chunk, meta_generated::meta};
use super::{read, Result};
use crate::kv;
use async_recursion::async_recursion;
use async_std::sync::RwLock;
use futures::future::try_join_all;
use futures::try_join;
use std::collections::HashSet;

#[derive(Debug, Default)]
struct HeadChange {
    new: Option<String>,
    old: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum RefCountChange {
    Increment = 1,
    Decrement = -1,
}

pub struct Write<'a> {
    kvw: Box<dyn kv::Write + 'a>,
    changed_heads: RwLock<Vec<HeadChange>>,
    mutated_chunks: RwLock<HashSet<String>>,
}

impl<'a> Write<'_> {
    pub fn new(kvw: Box<dyn kv::Write + 'a>) -> Write {
        Write {
            kvw,
            changed_heads: Default::default(),
            mutated_chunks: Default::default(),
        }
    }

    pub fn read(&self) -> read::Read {
        read::Read::new(self.kvw.as_read())
    }

    pub async fn put_chunk(&mut self, c: &Chunk) -> Result<()> {
        // TODO: These can be done in parallel.

        self.kvw
            .put(&Key::ChunkData(c.hash()).to_string(), c.data())
            .await?;

        if let Some(buf) = c.meta() {
            // Put chunk is not supposed to change the ref count.
            // Hack for now.
            let old_ref_count = self.get_ref_count(&c.hash().to_string()).await?;
            if old_ref_count != 0u16 {
                let refs = match meta::get_root_as_meta(buf).refs() {
                    None => vec![],
                    Some(refs) => refs.iter().collect(),
                };

                let (buf, start) = Chunk::create_meta(&refs, old_ref_count).unwrap();
                self.kvw
                    .put(&Key::ChunkMeta(c.hash()).to_string(), &buf[start..])
                    .await?;
            } else {
                self.kvw
                    .put(&Key::ChunkMeta(c.hash()).to_string(), buf)
                    .await?;
            }
        }

        self.mutated_chunks
            .write()
            .await
            .insert(c.hash().to_string());

        Ok(())
    }

    pub async fn set_head(&mut self, name: &str, hash: Option<&str>) -> Result<()> {
        let old_hash = self.read().get_head(name).await?;

        // TODO: These can be done in parallel.

        let head_key = Key::Head(name).to_string();
        match hash {
            None => self.kvw.del(&head_key).await?,
            Some(h) => self.kvw.put(&head_key, h.as_bytes()).await?,
        }

        self.changed_heads.write().await.push(HeadChange {
            new: hash.map(str::to_string),
            old: old_hash,
        });

        Ok(())
    }

    pub async fn commit(self) -> Result<()> {
        self.collect_garbage().await?;
        Ok(self.kvw.commit().await?)
    }

    #[allow(dead_code)]
    pub async fn rollback(self) -> Result<()> {
        Ok(self.kvw.rollback().await?)
    }

    async fn collect_garbage(&self) -> Result<()> {
        // We increment all the ref counts before we do all the decrements. This
        // is so that we do not remove an item that goes from 1 -> 0 -> 1

        let changed_heads = self.changed_heads.read().await;
        let (new, old): (Vec<Option<&str>>, Vec<Option<&str>>) = changed_heads
            .iter()
            .map(|HeadChange { new, old }| (new.as_deref(), old.as_deref()))
            .unzip();

        for n in new.iter().filter_map(Option::as_ref) {
            self.change_ref_count(n, RefCountChange::Increment).await?;
        }

        for o in old.iter().filter_map(Option::as_ref) {
            self.change_ref_count(o, RefCountChange::Decrement).await?;
        }

        // Now we go through the mutated chunks to see if any of them are still orphaned.
        let mutated_chunks = self.mutated_chunks.read().await;
        try_join_all(mutated_chunks.iter().map(|hash| async move {
            let count = self.get_ref_count(&hash).await?;
            if count == 0u16 {
                self.remove_all_related_keys(&hash, false).await?;
            }
            Ok(()) as Result<()>
        }))
        .await?;

        Ok(())
    }

    #[async_recursion(?Send)]
    async fn change_ref_count(&self, hash: &str, rc: RefCountChange) -> Result<()> {
        let old_count = self.get_ref_count(hash).await?;
        let new_count = (old_count as i32) + (rc as i32);

        if old_count == 0 && rc == RefCountChange::Increment
            || old_count == 1 && rc == RefCountChange::Decrement
        {
            let meta_key = Key::ChunkMeta(hash).to_string();
            let buf = self.kvw.get(&meta_key).await?;
            if let Some(buf) = buf {
                let meta = meta::get_root_as_meta(&buf);
                if let Some(refs) = meta.refs() {
                    for r in refs.iter() {
                        self.change_ref_count(r, rc).await?;
                    }
                }
            }
        }

        if new_count == 0 {
            self.remove_all_related_keys(hash, true).await?;
        } else {
            self.set_ref_count(hash, new_count as u16).await?;
        }

        Ok(())
    }

    async fn set_ref_count(&self, hash: &str, count: u16) -> Result<()> {
        // Ref count is represented as a u16 stored as 2 bytes using BE.
        let meta_key = Key::ChunkMeta(hash).to_string();
        let buf = self.kvw.as_read().get(&meta_key).await?;
        if let Some(buf) = buf {
            let m = meta::get_root_as_meta(&buf);
            let refs = match m.refs() {
                None => vec![],
                Some(refs) => refs.iter().collect(),
            };
            match Chunk::create_meta(&refs, count) {
                None => self.kvw.del(&meta_key).await?,
                Some((buf, start)) => self.kvw.put(&meta_key, &buf[start..]).await?,
            };
        } else {
            match Chunk::create_meta(&vec![], count) {
                None => (),
                Some((buf, start)) => self.kvw.put(&meta_key, &buf[start..]).await?,
            }
        }
         
        Ok(())
    }

    async fn get_ref_count(&self, hash: &str) -> Result<u16> {
        let buf = self.kvw.get(&Key::ChunkMeta(hash).to_string()).await?;
        Ok(match buf {
            None => 0u16,
            Some(buf) => {
                let m = meta::get_root_as_meta(&buf);
                m.count()
            }
        })
    }

    async fn remove_all_related_keys(&self, hash: &str, update_mutated_chunks: bool) -> Result<()> {
        let data_key = Key::ChunkData(hash).to_string();
        let meta_key = Key::ChunkMeta(hash).to_string();

        try_join!(
            self.kvw.del(&data_key),
            self.kvw.del(&meta_key),
        )?;

        // Getting the write lock could be done in parallel too but seems like
        // we do not want to hold on to that for the entire duration of the
        // above writes.

        if update_mutated_chunks {
            let mut mutated_chunks = self.mutated_chunks.write().await;
            mutated_chunks.remove(hash);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use kv::Read;

    use super::*;
    use crate::kv::memstore::MemStore;
    use crate::kv::Store;
    use crate::util::rlog::LogContext;

    #[async_std::test]
    async fn put_chunk() {
        async fn test(data: &[u8], refs: &[&str]) {
            let kv = MemStore::new();
            let kvw = kv.write(LogContext::new()).await.unwrap();
            let mut w = Write::new(kvw);

            let c = Chunk::new((data.to_vec(), 0), refs, 0);
            w.put_chunk(&c).await.unwrap();

            let kd = Key::ChunkData(c.hash()).to_string();
            let km = Key::ChunkMeta(c.hash()).to_string();

            // The chunk data should always be there.
            assert_eq!(w.kvw.get(&kd).await.unwrap().unwrap().as_slice(), c.data());

            // The chunk meta should only be there if there were refs.
            if refs.is_empty() {
                assert!(!w.kvw.has(&km).await.unwrap());
            } else {
                assert_eq!(
                    w.kvw.get(&km).await.unwrap().unwrap().as_slice(),
                    c.meta().unwrap()
                );
            }
        }

        test(&vec![], &vec![]).await;
        test(&vec![0], &vec!["r1"]).await;
        test(&vec![0, 1], &vec!["r1", "r2"]).await;
    }

    async fn assert_ref_count(kvr: &dyn Read, hash: &str, count: u16) {
        let buf = kvr
            .get(&Key::ChunkMeta(hash).to_string())
            .await
            .unwrap();
        if let Some(buf) = buf {
            let m = meta::get_root_as_meta(&buf);
            assert_eq!(count, m.count());
        }  else {
            assert_eq!(count, 0u16);
        }
    }

    #[async_std::test]
    async fn set_head() {
        let kv = MemStore::new();
        async fn test(kv: &MemStore, name: &str, hash: Option<&str>) {
            let kvw = kv.write(LogContext::new()).await.unwrap();
            let mut w = Write::new(kvw);
            w.set_head(name, hash).await.unwrap();
            match hash {
                Some(h) => assert_eq!(
                    h,
                    String::from_utf8(w.kvw.get(&format!("h/{}", name)).await.unwrap().unwrap())
                        .unwrap()
                ),
                None => assert!(w.kvw.get(&format!("h/{}", name)).await.unwrap().is_none()),
            }
            w.commit().await.unwrap();
        }

        {
            test(&kv, "", Some("")).await;
            let kvr = kv.read(LogContext::new()).await.unwrap();
            assert_ref_count(kvr.as_ref(), "", 1).await;
        }
        {
            test(&kv, "", Some("h1")).await;
            let kvr = kv.read(LogContext::new()).await.unwrap();
            assert_ref_count(kvr.as_ref(), "h1", 1).await;
            assert_ref_count(kvr.as_ref(), "", 0).await;
        }
        {
            test(&kv, "n1", Some("")).await;
            let kvr = kv.read(LogContext::new()).await.unwrap();
            assert_ref_count(kvr.as_ref(), "", 1).await;
        }
        {
            test(&kv, "n1", Some("h1")).await;
            let kvr = kv.read(LogContext::new()).await.unwrap();
            assert_ref_count(kvr.as_ref(), "h1", 2).await;
            assert_ref_count(kvr.as_ref(), "", 0).await;
        }
        {
            test(&kv, "n1", Some("h1")).await;
            let kvr = kv.read(LogContext::new()).await.unwrap();
            assert_ref_count(kvr.as_ref(), "h1", 2).await;
            assert_ref_count(kvr.as_ref(), "", 0).await;
        }
        {
            test(&kv, "n1", None).await;
            let kvr = kv.read(LogContext::new()).await.unwrap();
            assert_ref_count(kvr.as_ref(), "h1", 1).await;
            assert_ref_count(kvr.as_ref(), "", 0).await;
        }
        {
            test(&kv, "", None).await;
            let kvr = kv.read(LogContext::new()).await.unwrap();
            assert_ref_count(kvr.as_ref(), "h1", 0).await;
            assert_ref_count(kvr.as_ref(), "", 0).await;
        }
    }

    #[async_std::test]
    async fn commit_rollback() {
        async fn test(commit: bool, set_head: bool) {
            let key: String;
            let kv = MemStore::new();
            {
                let kvw = kv.write(LogContext::new()).await.unwrap();
                let mut w = Write::new(kvw);
                let c = Chunk::new((vec![0, 1], 0), &vec![], 0);
                w.put_chunk(&c).await.unwrap();

                key = Key::ChunkData(c.hash()).to_string();

                // The changes should be present inside the tx.
                assert!(w.kvw.has(&key).await.unwrap());

                if commit {
                    if set_head {
                        w.set_head("test", Some(c.hash())).await.unwrap();
                    }
                    w.commit().await.unwrap();
                } else {
                    w.rollback().await.unwrap();
                }
            }

            // The data shoul only persist if we set the head and commit.
            let kvr = kv.read(LogContext::new()).await.unwrap();
            assert_eq!(set_head && commit, kvr.has(&key).await.unwrap());
        }

        test(true, false).await;
        test(false, false).await;
        test(true, true).await;
    }

    #[async_std::test]
    async fn roundtrip() {
        async fn test(name: &str, data: &[u8], refs: &[&str]) {
            let kv = MemStore::new();
            let c = Chunk::new((data.to_vec(), 0), refs, 0);
            {
                let kvw = kv.write(LogContext::new()).await.unwrap();
                let mut w = Write::new(kvw);
                w.put_chunk(&c).await.unwrap();
                w.set_head(name, Some(c.hash())).await.unwrap();

                // Read the changes inside the tx.
                let c2 = w.read().get_chunk(c.hash()).await.unwrap().unwrap();
                let h = w.read().get_head(name).await.unwrap().unwrap();
                assert_eq!(c, c2);
                assert_eq!(h, c.hash());

                w.commit().await.unwrap();
            }

            // Read the changes outside the tx.
            let r = read::OwnedRead::new(kv.read(LogContext::new()).await.unwrap());
            let c2 = r.read().get_chunk(c.hash()).await.unwrap().unwrap();
            let h = r.read().get_head(name).await.unwrap().unwrap();
            let c_expected = Chunk::new((data.to_vec(), 0), refs, 1);
            assert_eq!(c_expected, c2);
            assert_eq!(h, c.hash());
        }

        test("", &vec![], &vec![]).await;
        test("n1", &vec![0], &vec!["r1"]).await;
        test("n2", &vec![0, 1], &vec!["r1", "r2"]).await;
    }
}
