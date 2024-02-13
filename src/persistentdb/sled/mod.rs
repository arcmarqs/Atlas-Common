use std::{path::Path};
use sled::{Config, Db, Tree, IVec, Batch};
use anyhow::{anyhow, Context};
use std::fmt::format;
use crate::error::*;
use crate::Err;
use crate::persistentdb::PersStorage;

#[derive(Debug,Clone)]
pub(crate) struct SledKVDB {
    db: Db,
}

impl SledKVDB {
    pub fn new<T>(db_location: T, prefixes: Vec<&'static str>) -> Result<Self>
    where
        T: AsRef<Path>,
    {
        let conf = Config::default()
        .mode(sled::Mode::LowSpace)
        .path(db_location)
        .cache_capacity(3*1024*1024*1024);

        let db = conf.open().unwrap();

        for tree in prefixes {

            let _ = db.open_tree(tree);
        }
       

        Ok(SledKVDB { db })
    }

    fn get_handle(&self, prefix: &'static str) -> Result<Tree> {     
           let handle = self.db.open_tree(prefix);       

        if let Ok(handle) = handle {
            Ok(handle)
        } else {
            Err!(PersStorage::NoPrefix(prefix))
        }
    }

    pub fn get<T>(&self, prefix: &'static str, key: T) -> Result<Option<Vec<u8>>>
    where
        T: AsRef<[u8]>,
    {
        let tree = self.get_handle(prefix)?;

        if let Ok(res) = tree.get(key) {
            return Ok(res.map(|ivec| ivec.to_vec()));
        } else { 
            Err!(PersStorage::NoPrefix(prefix))
        }

    }

    pub fn get_all<T, Y>(&self, keys: T) -> Result<Vec<Result<Option<Vec<u8>>>>>
    where
        T: Iterator<Item = (&'static str, Y)>,
        Y: AsRef<[u8]>,
    {
        let mut values = Vec::new();
        let final_keys =
            keys.map(|(prefix, key)| (self.get_handle(prefix).expect("Failed to get handle"), key));

        for (tree,key) in final_keys {
            if let Ok(res) = tree.get(key) {
              values.push(Ok(res.map(|ivec| ivec.to_vec())));
            }
        }

        Ok(values) 
    }

    pub fn exists<T>(&self, prefix: &'static str, key: T) -> Result<bool>
    where
        T: AsRef<[u8]>,
    {
        let handle = self.get_handle(prefix)?;

        if let Ok(res) = handle.contains_key(key) {
            Ok(res)
        } else {
            Err!(PersStorage::NoPrefix(prefix))
        }
        
    }

    pub fn set<T, Y>(&self, prefix: &'static str, key: T, data: Y) -> Result<()>
    where
        T: AsRef<[u8]>,
        Y: AsRef<[u8]>,
        IVec: std::convert::From<Y>
    {
        let handle = self.get_handle(prefix)?;

        if let Ok(res) = handle.insert(key, data) {
            println!("set res : {:?}", res.is_some());
            Ok(())
        } else {
            Err!(PersStorage::NoPrefix(prefix))
        }
    }

    pub fn set_all<T, Y, Z>(&self, prefix: &'static str, values: T) -> Result<()>
    where
        T: Iterator<Item = (Y, Z)>,
        Y: AsRef<[u8]>,
        Z: AsRef<[u8]>,
        IVec: std::convert::From<Z>,
        IVec: std::convert::From<Y>,

    {
        let handle = self.get_handle(prefix)?;

       let mut batch = Batch::default();

        for (key, value) in values {
            batch.insert(key, value);
        }

        let ret = handle.apply_batch(batch).context(format!("Failed to set keys"));
        
        //handle.flush();

        ret
    }

    pub fn erase<T>(&self, prefix: &'static str, key: T) -> Result<()>
    where
        T: AsRef<[u8]>,
    {
        let handle = self.get_handle(prefix)?;


        handle.remove(key).map(|_| ()).context(format!("Failed to erase key in prefix {:?}", prefix))
    }

    /// Delete a set of keys
    /// Accepts an [`&[&[u8]]`], in any possible form, as long as it can be dereferenced
    /// all the way to the intended target.
    pub fn erase_keys<T, Y>(&self, prefix: &'static str, keys: T) -> Result<()>
    where
        T: Iterator<Item = Y>,
        Y: AsRef<[u8]>,
        IVec: std::convert::From<Y>,
    {
        let handle = self.get_handle(prefix)?;

        let mut batch = Batch::default();

        for key in keys {
            batch.remove(key)
        }

        handle.apply_batch(batch).context(format!("Failed to erase in prefix {:?}", prefix))
    }

    pub fn erase_range<T>(&self, prefix: &'static str, start: T, end: T) -> Result<()>
    where
        T: AsRef<[u8]>,
    {
        let handle = self.get_handle(prefix)?;
        let iter = handle.range(start..end);
        let _ = iter.map(|res| handle.remove(res.expect("Kv pair not found").0));
        Ok(())
    }

    pub fn compact_range<T, Y>(
        &self,
        prefix: &'static str,
        start: Option<T>,
        end: Option<Y>,
    ) -> Result<()>
    where
        T: AsRef<[u8]>,
        Y: AsRef<[u8]>,
    {

        Ok(())
    }

    pub fn iter_range<T>(
        &self,
        prefix: &'static str,
        start: Option<T>,
        end: Option<T>,
    ) -> Result<Box<dyn Iterator<Item = Result<(Box<[u8]>, Box<[u8]>)>> + '_>>
    where
        T: AsRef<[u8]>,
    {
        let handle = self.get_handle(prefix)?;
        let iter = match (start, end) {
            (None, None) => handle.iter(),
            (None, Some(end)) => handle.range(..end),
            (Some(start), None) => handle.range(start..),
            (Some(start), Some(end)) => handle.range(start..end),
        };

        Ok(Box::new(iter.map(|r| {
            r.map(|(k, v)| (k.to_vec().into_boxed_slice(), v.to_vec().into_boxed_slice()))
            .with_context(|| format!("Failed to erase in range"))
        })))
    }
}