use std::fmt::format;
use std::iter::once;
use std::path::Path;
use anyhow::{anyhow, Context};

use rocksdb::{
    ColumnFamily, ColumnFamilyDescriptor, CompactOptions, DBWithThreadMode, Direction,
    IteratorMode, Options, SingleThreaded, WriteBatchWithTransaction, DB,
};
use crate::Err;

use crate::error::*;
use crate::persistentdb::PersStorage;

pub(crate) struct RocksKVDB {
    db: DBWithThreadMode<SingleThreaded>,
}

impl RocksKVDB {
    pub fn new<T>(db_location: T, prefixes: Vec<&'static str>) -> Result<Self>
        where
            T: AsRef<Path>,
    {
        let mut cfs = Vec::with_capacity(prefixes.len());

        for cf in prefixes {
            let mut cf_opts = Options::default();

            if cf.eq("state") {
                print!("state");
                cf_opts.set_enable_blob_files(true);
                cf_opts.set_blob_file_size(0x8000000);
                cf_opts.set_write_buffer_size(0x8000000);
                cf_opts.set_min_blob_size(0x4000000/64);
                cf_opts.set_target_file_size_base(0x2000000);
                cf_opts.optimize_for_point_lookup(0x4000000);
                cf_opts.set_max_bytes_for_level_base(8*0x2000000);
            }

            cfs.push(ColumnFamilyDescriptor::new(cf, cf_opts));
        }

        let mut db_opts = Options::default();
        db_opts.increase_parallelism(48);
        db_opts.create_missing_column_families(true);
        db_opts.create_if_missing(true);

        let db = DB::open_cf_descriptors(&db_opts, db_location, cfs).unwrap();

        Ok(RocksKVDB { db })
    }

    fn get_handle(&self, prefix: &'static str) -> Result<&ColumnFamily> {
        let handle = self.db.cf_handle(prefix);

        if let Some(handle) = handle {
            Ok(handle)
        } else {
            Err!(PersStorage::NoPrefix(prefix))
        }
    }

    pub fn get<T>(&self, prefix: &'static str, key: T) -> Result<Option<Vec<u8>>>
        where
            T: AsRef<[u8]>,
    {
        let handle = self.get_handle(prefix)?;

        self.db
            .get_cf(handle, key)
            .with_context(|| format!("Failed to get for prefix {:?}", prefix))
    }

    pub fn get_all<T, Y>(&self, keys: T) -> Result<Vec<Result<Option<Vec<u8>>>>>
        where
            T: Iterator<Item=(&'static str, Y)>,
            Y: AsRef<[u8]>,
    {
        let (prefix, keys): (Vec<_>,Vec<_>) = keys.into_iter().unzip();

        let handle = self.get_handle(prefix[0]).unwrap();
        Ok(self.db
            .batched_multi_get_cf(handle,keys,false)
            .into_iter()
            .map(|r| {
                if let Ok(result) = r {
                    match result {
                        Some(res) => 
                        {
                            Ok(Some(res.to_vec()))
                        },
                        None => Ok(None),
                    }
                } else {
                    Err(anyhow!(""))
                }
            }).collect())
    }

    pub fn exists<T>(&self, prefix: &'static str, key: T) -> Result<bool>
        where
            T: AsRef<[u8]>,
    {
        let handle = self.get_handle(prefix)?;

        Ok(self.db.key_may_exist_cf(handle, key))
    }

    pub fn set<T, Y>(&self, prefix: &'static str, key: T, data: Y) -> Result<()>
        where
            T: AsRef<[u8]>,
            Y: AsRef<[u8]>,
    {
        let handle = self.get_handle(prefix)?;

        self.db
            .put_cf(handle, key, data)
            .context(format!("Failed to set in prefix {:?}", prefix))
    }

    pub fn set_all<T, Y, Z>(&self, prefix: &'static str, values: T) -> Result<()>
        where
            T: Iterator<Item=(Y, Z)>,
            Y: AsRef<[u8]>,
            Z: AsRef<[u8]>,
    {
        let handle = self.get_handle(prefix)?;

        let mut batch = WriteBatchWithTransaction::<false>::default();

        for (key, value) in values {
            batch.put_cf(handle, key, value)
        }

        self.db.write(batch)
            .context(format!("Failed to set keys"))
    }

    pub fn erase<T>(&self, prefix: &'static str, key: T) -> Result<()>
        where
            T: AsRef<[u8]>,
    {
        let handle = self.get_handle(prefix)?;

        self.db
            .delete_cf(handle, key)
            .context(format!("Failed to erase key in prefix {:?}", prefix))
    }

    /// Delete a set of keys
    /// Accepts an [`&[&[u8]]`], in any possible form, as long as it can be dereferenced
    /// all the way to the intended target.
    pub fn erase_keys<T, Y>(&self, prefix: &'static str, keys: T) -> Result<()>
        where
            T: Iterator<Item=Y>,
            Y: AsRef<[u8]>,
    {
        let handle = self.get_handle(prefix)?;

        let mut batch = WriteBatchWithTransaction::<false>::default();

        for key in keys {
            batch.delete_cf(handle, key)
        }

        self.db.write(batch)
            .context(format!("Failed to erase in prefix {:?}", prefix))
    }

    pub fn erase_range<T>(&self, prefix: &'static str, start: T, end: T) -> Result<()>
        where
            T: AsRef<[u8]>,
    {
        let handle = self.get_handle(prefix)?;

        self.db
            .delete_range_cf(handle, start, end)
            .with_context(|| format!("Failed to erase in prefix {:?}", prefix))
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
        let handle = self.get_handle(prefix)?;

        Ok(self
            .db
            .compact_range_cf_opt(handle, start, end, &CompactOptions::default()))
    }

    pub fn iter_range<T, Y>(
        &self,
        prefix: &'static str,
        start: Option<T>,
        end: Option<Y>,
    ) -> Result<Box<dyn Iterator<Item=Result<(Box<[u8]>, Box<[u8]>)>> + '_>>
        where
            T: AsRef<[u8]>,
            Y: AsRef<[u8]>
    {
        let handle = self.get_handle(prefix)?;

        let mut iterator = if let Some(start) = start {
            self.db.iterator_cf(
                handle,
                IteratorMode::From(start.as_ref(), Direction::Forward),
            )
        } else {
            self.db.iterator_cf(handle, IteratorMode::Start)
        };

        if let Some(end) = end {
            iterator.set_mode(IteratorMode::From(end.as_ref(), Direction::Reverse));
        }

        let mut bytes = vec![];

        for futures in iterator {
            bytes.push(Ok(futures?));
        }

        Ok(Box::new(bytes.into_iter()))
    }
}
