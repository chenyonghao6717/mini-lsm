// Copyright (c) 2022-2025 Alex Chi Z
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::collections::HashMap;
use std::fs::File;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use anyhow::{Result, anyhow};
use bytes::Bytes;
use parking_lot::{Mutex, MutexGuard, RwLock};

use crate::block::Block;
use crate::compact::{
    CompactionController, CompactionOptions, CompactionTask, LeveledCompactionController,
    LeveledCompactionOptions, SimpleLeveledCompactionController, SimpleLeveledCompactionOptions,
    TieredCompactionController,
};
use crate::iterators::StorageIterator;
use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::key::KeySlice;
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::manifest::{Manifest, ManifestRecord};
use crate::mem_table::{MemTable, MemTableIterator};
use crate::mvcc::LsmMvccInner;
use crate::table::{FileObject, SsTable, SsTableBuilder, SsTableIterator};

pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

/// Represents the state of the storage engine.
#[derive(Clone)]
pub struct LsmStorageState {
    /// The current memtable.
    pub memtable: Arc<MemTable>,
    /// Immutable memtables, from latest to earliest.
    /// A newly frozon memtable is inserted at index 0.
    pub imm_memtables: Vec<Arc<MemTable>>,
    /// L0 SSTs, from latest to earliest.
    pub l0_sstables: Vec<usize>,
    /// SsTables sorted by key range; L1 - L_max for leveled compaction, or tiers for tiered
    /// compaction.
    pub levels: Vec<(usize, Vec<usize>)>,
    /// SST objects.
    pub sstables: HashMap<usize, Arc<SsTable>>,
}

pub enum WriteBatchRecord<T: AsRef<[u8]>> {
    Put(T, T),
    Del(T),
}

impl LsmStorageState {
    fn create(options: &LsmStorageOptions) -> Self {
        let levels = match &options.compaction_options {
            CompactionOptions::Leveled(LeveledCompactionOptions { max_levels, .. })
            | CompactionOptions::Simple(SimpleLeveledCompactionOptions { max_levels, .. }) => (1
                ..=*max_levels)
                .map(|level| (level, Vec::new()))
                .collect::<Vec<_>>(),
            CompactionOptions::Tiered(_) => Vec::new(),
            CompactionOptions::NoCompaction => vec![(1, Vec::new())],
        };
        Self {
            memtable: Arc::new(MemTable::create(0)),
            imm_memtables: Vec::new(),
            l0_sstables: Vec::new(),
            levels,
            sstables: Default::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LsmStorageOptions {
    // Block size in bytes
    pub block_size: usize,
    // SST size in bytes, also the approximate memtable capacity limit
    pub target_sst_size: usize,
    // Maximum number of memtables in memory, flush to L0 when exceeding this limit
    pub num_memtable_limit: usize,
    pub compaction_options: CompactionOptions,
    pub enable_wal: bool,
    pub serializable: bool,
}

impl LsmStorageOptions {
    pub fn default_for_week1_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 50,
            serializable: false,
        }
    }

    pub fn default_for_week1_day6_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }

    pub fn default_for_week2_test(compaction_options: CompactionOptions) -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 1 << 20, // 1MB
            compaction_options,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }
}

#[derive(Clone, Debug)]
pub enum CompactionFilter {
    Prefix(Bytes),
}

/// The storage interface of the LSM tree.
pub(crate) struct LsmStorageInner {
    pub(crate) state: Arc<RwLock<Arc<LsmStorageState>>>,
    pub(crate) state_lock: Mutex<()>,
    path: PathBuf,
    pub(crate) block_cache: Arc<BlockCache>,
    next_sst_id: AtomicUsize,
    pub(crate) options: Arc<LsmStorageOptions>,
    pub(crate) compaction_controller: CompactionController,
    pub(crate) manifest: Option<Manifest>,
    pub(crate) mvcc: Option<LsmMvccInner>,
    pub(crate) compaction_filters: Arc<Mutex<Vec<CompactionFilter>>>,
}

/// A thin wrapper for `LsmStorageInner` and the user interface for MiniLSM.
pub struct MiniLsm {
    pub(crate) inner: Arc<LsmStorageInner>,
    /// Notifies the L0 flush thread to stop working. (In week 1 day 6)
    flush_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the flush thread. (In week 1 day 6)
    flush_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Notifies the compaction thread to stop working. (In week 2)
    compaction_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the compaction thread. (In week 2)
    compaction_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Drop for MiniLsm {
    fn drop(&mut self) {
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();
    }
}

impl MiniLsm {
    pub fn close(&self) -> Result<()> {
        self.flush_notifier.send(()).map_err(|e| anyhow!("{}", e))?;

        {
            let mut flush_thread = self.flush_thread.lock();
            if let Some(handle) = flush_thread.take() {
                handle.join().map_err(|e| anyhow!("{:?}", e))?;
            }
        }

        // Cleanup
        if !self.inner.options.enable_wal {
            {
                let state_lock_observer = self.inner.state_lock.lock();
                self.inner.force_freeze_memtable(&state_lock_observer)?;
            }
            let memtable_num = self.inner.state.read().imm_memtables.len();
            for _ in 0..memtable_num {
                self.inner.force_flush_next_imm_memtable()?;
            }
        }

        Ok(())
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Arc<Self>> {
        let inner = Arc::new(LsmStorageInner::open(path, options)?);
        let (tx1, rx) = crossbeam_channel::unbounded();
        let compaction_thread = inner.spawn_compaction_thread(rx)?;
        let (tx2, rx) = crossbeam_channel::unbounded();
        let flush_thread = inner.spawn_flush_thread(rx)?;
        Ok(Arc::new(Self {
            inner,
            flush_notifier: tx2,
            flush_thread: Mutex::new(flush_thread),
            compaction_notifier: tx1,
            compaction_thread: Mutex::new(compaction_thread),
        }))
    }

    pub fn new_txn(&self) -> Result<()> {
        self.inner.new_txn()
    }

    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        self.inner.write_batch(batch)
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        self.inner.add_compaction_filter(compaction_filter)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put(key, value)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.delete(key)
    }

    pub fn sync(&self) -> Result<()> {
        self.inner.sync()
    }

    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        self.inner.scan(lower, upper)
    }

    /// Only call this in test cases due to race conditions
    pub fn force_flush(&self) -> Result<()> {
        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .force_freeze_memtable(&self.inner.state_lock.lock())?;
        }
        if !self.inner.state.read().imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        self.inner.force_full_compaction()
    }
}

impl LsmStorageInner {
    pub(crate) fn next_sst_id(&self) -> usize {
        self.next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    pub(crate) fn mvcc(&self) -> &LsmMvccInner {
        self.mvcc.as_ref().unwrap()
    }

    fn load_sst(id: usize, block_cache: &Arc<BlockCache>, path: &Path) -> Result<Arc<SsTable>> {
        let sst_path = Self::path_of_sst_static(path, id);
        let sst = SsTable::open(
            id,
            Some(Arc::clone(block_cache)),
            FileObject::open(&sst_path)?,
        )?;
        Ok(Arc::new(sst))
    }

    fn load_compaction_manifest(
        compaction_controller: &CompactionController,
        mut snapshot: LsmStorageState,
        task: &CompactionTask,
        new_sst_ids: &[usize],
        block_cache: &Arc<BlockCache>,
        path: &Path,
        in_recovery: bool,
    ) -> Result<LsmStorageState> {
        for new_sst_id in new_sst_ids {
            let sst = Self::load_sst(*new_sst_id, block_cache, path)?;
            snapshot.sstables.insert(*new_sst_id, sst);
        }
        let (new_state, _sst_ids_to_delete) = compaction_controller.apply_compaction_result(
            &snapshot,
            task,
            new_sst_ids,
            in_recovery,
        );
        // for id in &sst_ids_to_delete {
        //     new_state.sstables.remove(id);
        // }
        Ok(new_state)
    }

    fn load_manifest(
        compaction_controller: &CompactionController,
        manifest_records: &[ManifestRecord],
        options: &LsmStorageOptions,
        block_cache: Arc<BlockCache>,
        path: &Path,
        manifest_path: &Path,
    ) -> Result<LsmStorageState> {
        let mut state = LsmStorageState::create(options);
        for record in manifest_records {
            state = match record {
                ManifestRecord::Flush(sst_id) => {
                    let sst = Self::load_sst(*sst_id, &block_cache, path)?;
                    state.sstables.insert(*sst_id, sst);
                    state.l0_sstables.insert(0, *sst_id);
                    state
                }
                ManifestRecord::Compaction(task, new_sst_ids) => Self::load_compaction_manifest(
                    compaction_controller,
                    state,
                    task,
                    new_sst_ids,
                    &block_cache,
                    path,
                    true,
                )?,
                _ => unreachable!(),
            }
        }

        // Sort all levels
        for (_, sst_ids) in &mut state.levels {
            sst_ids.sort_by_key(|id| state.sstables[id].first_key());
        }

        Ok(state)
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub(crate) fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self> {
        let compaction_controller = match &options.compaction_options {
            CompactionOptions::Leveled(options) => {
                CompactionController::Leveled(LeveledCompactionController::new(options.clone()))
            }
            CompactionOptions::Tiered(options) => {
                CompactionController::Tiered(TieredCompactionController::new(options.clone()))
            }
            CompactionOptions::Simple(options) => CompactionController::Simple(
                SimpleLeveledCompactionController::new(options.clone()),
            ),
            CompactionOptions::NoCompaction => CompactionController::NoCompaction,
        };

        let block_cache = Arc::new(BlockCache::new(1024));
        let manifest_path = path.as_ref().join("MANIFEST");
        let in_recovery = path.as_ref().try_exists()?;
        if in_recovery {
            let (manifest, manifest_records) = Manifest::recover(&manifest_path)?;
            let state = Self::load_manifest(
                &compaction_controller,
                &manifest_records,
                &options,
                Arc::clone(&block_cache),
                path.as_ref(),
                &manifest_path,
            )?;
            Ok(Self {
                state: Arc::new(RwLock::new(Arc::new(state))),
                state_lock: Mutex::new(()),
                path: path.as_ref().to_path_buf(),
                block_cache,
                next_sst_id: AtomicUsize::new(1),
                compaction_controller,
                manifest: Some(manifest),
                options: options.into(),
                mvcc: None,
                compaction_filters: Arc::new(Mutex::new(Vec::new())),
            })
        } else {
            let state = LsmStorageState::create(&options);
            Ok(Self {
                state: Arc::new(RwLock::new(Arc::new(state))),
                state_lock: Mutex::new(()),
                path: path.as_ref().to_path_buf(),
                block_cache,
                next_sst_id: AtomicUsize::new(1),
                compaction_controller,
                manifest: Some(Manifest::create(manifest_path)?),
                options: options.into(),
                mvcc: None,
                compaction_filters: Arc::new(Mutex::new(Vec::new())),
            })
        }
    }

    pub fn sync(&self) -> Result<()> {
        unimplemented!()
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        let mut compaction_filters = self.compaction_filters.lock();
        compaction_filters.push(compaction_filter);
    }

    /// Find the value of a key in sstables denoted by sst_ids. We don't convert
    /// a value with empty data(tombstone) to Option::None here.
    fn get_in_level(
        &self,
        engine: &LsmStorageState,
        _key: &[u8],
        sst_ids: &[usize],
    ) -> Result<Option<Bytes>> {
        for id in sst_ids {
            if let Some(sst) = engine.sstables.get(id) {
                if !sst.may_contain(_key) {
                    continue;
                }
                let iter = SsTableIterator::create_and_seek_to_key(
                    sst.clone(),
                    KeySlice::from_slice(_key),
                )?;
                let k = iter.key();
                let v = iter.value();
                if iter.key().raw_ref() != _key {
                    continue;
                }
                return Ok(Some(Bytes::copy_from_slice(iter.value())));
            } else {
                return Err(anyhow!("Sstable {} not found!", id));
            }
        }

        Ok(None)
    }

    fn get_from_sstables(&self, engine: &LsmStorageState, _key: &[u8]) -> Result<Option<Bytes>> {
        let mut value: Option<Bytes> = None;
        let value_of_l0 = self.get_in_level(engine, _key, &engine.l0_sstables)?;
        if value_of_l0.is_some() {
            value = value_of_l0;
        } else {
            for (level, sst_ids) in &engine.levels {
                let value_of_level = self.get_in_level(engine, _key, sst_ids)?;
                if value_of_level.is_some() {
                    value = value_of_level;
                    break;
                }
            }
        }

        Ok(value)
    }

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    pub fn get(&self, _key: &[u8]) -> Result<Option<Bytes>> {
        let engine = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };

        // Check the mutable memtable.
        let key = Bytes::copy_from_slice(_key);
        let value = engine.memtable.get(&key);
        if let Some(v) = &value {
            if v.is_empty() {
                return Ok(None);
            } else {
                return Ok(value);
            }
        }

        // Check immutable memtables
        for imm_memtable in engine.imm_memtables.iter() {
            let value = imm_memtable.get(&key);
            if let Some(v) = &value {
                if v.is_empty() {
                    return Ok(None);
                } else {
                    return Ok(value);
                }
            }
        }

        // Check sstables.
        let value = self.get_from_sstables(&engine, key.as_ref())?;
        if value.is_none() || value.as_ref().unwrap().is_empty() {
            Ok(None)
        } else {
            Ok(value)
        }
    }

    /// Write a batch of data into the storage. Implement in week 2 day 7.
    pub fn write_batch<T: AsRef<[u8]>>(&self, _batch: &[WriteBatchRecord<T>]) -> Result<()> {
        unimplemented!()
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(&self, _key: &[u8], _value: &[u8]) -> Result<()> {
        // Only the read lock is required because only the internal state of the
        // engine is being modified, the engine itself stays the same.
        // The write lock is required only when the engine itself is replaced, that is,
        // when freezing the current memtable and creating a new one.
        // By using CoW, the writer trying to freeze memtable doesn't need to wait
        // readers to finish because it copies a new one. The write lock only blocks
        // later readers and writer from seeing a mid-state engine.
        let needs_freeze = {
            let engine = self.state.read();
            engine.memtable.put(_key, _value)?;
            engine.memtable.approximate_size() > self.options.target_sst_size
        };

        if needs_freeze {
            let state_lock = &self.state_lock.lock();
            let actual_needs_freeze = {
                let engine = self.state.read();
                engine.memtable.approximate_size() > self.options.target_sst_size
            };

            if actual_needs_freeze {
                self.force_freeze_memtable(state_lock)?;
            }
        }

        Ok(())
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(&self, _key: &[u8]) -> Result<()> {
        self.put(_key, &[])
    }

    pub(crate) fn path_of_sst_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.sst", id))
    }

    pub(crate) fn path_of_sst(&self, id: usize) -> PathBuf {
        Self::path_of_sst_static(&self.path, id)
    }

    pub(crate) fn path_of_wal_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.wal", id))
    }

    pub(crate) fn path_of_wal(&self, id: usize) -> PathBuf {
        Self::path_of_wal_static(&self.path, id)
    }

    pub(super) fn sync_dir(&self) -> Result<()> {
        let dir = File::open(&self.path)?;
        dir.sync_all()?;
        Ok(())
    }

    pub(crate) fn write_manifest_record(
        &self,
        record: ManifestRecord,
        _state_lock_observer: &MutexGuard<()>,
    ) -> Result<()> {
        if let Some(manifest) = &self.manifest {
            manifest.add_record(_state_lock_observer, record)?;
        }
        Ok(())
    }

    /// Force freeze the current memtable to an immutable memtable
    pub fn force_freeze_memtable(&self, _state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {
        let new_engine = {
            let engine = self.state.read();
            let mut new_engine = (**engine).clone();
            new_engine
                .imm_memtables
                .insert(0, Arc::clone(&new_engine.memtable));
            new_engine.memtable = Arc::new(MemTable::create(self.next_sst_id()));
            new_engine
        };

        let mut engine = self.state.write();
        *engine = Arc::new(new_engine);

        Ok(())
    }

    fn get_earliest_imm_memtable(&self) -> Option<Arc<MemTable>> {
        let guard = self.state.read();
        guard.imm_memtables.last().cloned()
    }

    fn flush_earliest_memtable(&self, _state_lock_observer: &MutexGuard<()>) -> Result<()> {
        let mut new_engine = {
            let engine = self.state.read();
            (**engine).clone()
        };
        let memtable = new_engine.imm_memtables.last().unwrap();
        let sst_id = self.next_sst_id();

        // Build SST (protected by self.state_lock)
        let mut table_builder = SsTableBuilder::new(self.options.block_size);
        memtable.flush(&mut table_builder)?;
        let sstable = table_builder.build(
            sst_id,
            Some(Arc::clone(&self.block_cache)),
            self.path_of_sst(sst_id),
        )?;

        // Sync folder after sst is created.
        self.sync_dir()?;

        // Write manifest.
        self.write_manifest_record(
            ManifestRecord::Flush(sstable.sst_id()),
            _state_lock_observer,
        )?;

        // Insert new sstable and remove memtable
        if self.compaction_controller.flush_to_l0() {
            new_engine.l0_sstables.insert(0, sst_id);
        } else {
            // Tiered strategy doesn't use l0.
            new_engine.levels.insert(0, (sst_id, vec![sst_id]));
        }
        new_engine.sstables.insert(sst_id, Arc::new(sstable));
        new_engine.imm_memtables.pop();

        let mut engine = self.state.write();
        *engine = Arc::new(new_engine);

        Ok(())
    }

    /// Force flush the earliest-created immutable memtable to disk
    pub fn force_flush_next_imm_memtable(&self) -> Result<()> {
        // Double check lock.
        let earliest_memtable = self.get_earliest_imm_memtable();
        if earliest_memtable.is_none() {
            return Ok(());
        }

        let _mutex = self.state_lock.lock();

        let earliest_memtable_ = self.get_earliest_imm_memtable();
        // If the above checked memtable is removed, do nothing
        if earliest_memtable_.is_none()
            || !Arc::ptr_eq(
                earliest_memtable.as_ref().unwrap(),
                earliest_memtable_.as_ref().unwrap(),
            )
        {
            return Ok(());
        }

        self.flush_earliest_memtable(&_mutex)?;

        Ok(())
    }

    pub fn new_txn(&self) -> Result<()> {
        // no-op
        Ok(())
    }

    fn to_memtable_merge_iter(
        engine: Arc<LsmStorageState>,
        _lower: Bound<&[u8]>,
        _upper: Bound<&[u8]>,
    ) -> MergeIterator<MemTableIterator> {
        let mut memtable_iters: Vec<Box<MemTableIterator>> =
            vec![Box::new(engine.memtable.scan(_lower, _upper))];
        memtable_iters.extend(
            engine
                .imm_memtables
                .iter()
                .map(|x| Box::new(x.scan(_lower, _upper))),
        );
        MergeIterator::create(memtable_iters)
    }

    fn to_l0_sst_merge_iter(
        engine: Arc<LsmStorageState>,
        _lower: Bound<&[u8]>,
        _upper: Bound<&[u8]>,
    ) -> Result<MergeIterator<SsTableIterator>> {
        let mut sstable_iters = Vec::<Box<SsTableIterator>>::new();
        for idx in &engine.l0_sstables {
            let table = engine.sstables.get(idx).map(Arc::clone);
            if let Some(t) = table {
                if t.has_overlap(_lower, _upper) {
                    let iter = SsTableIterator::create_and_seek_to_first(t)?;
                    sstable_iters.push(Box::new(iter));
                }
            } else {
                return Err(anyhow!("Sstable {} not found!", idx));
            }
        }

        match _lower {
            Bound::Included(key) => {
                for sst_iter in sstable_iters.iter_mut() {
                    sst_iter.seek_to_key(KeySlice::from_slice(key))?
                }
            }
            Bound::Excluded(key) => {
                for sst_iter in sstable_iters.iter_mut() {
                    let key_slice = KeySlice::from_slice(key);
                    // Seek to the key first. Call next() one more time if iter key == target key
                    // since bound is Excluded
                    sst_iter.seek_to_key(key_slice)?;
                    if sst_iter.is_valid() && sst_iter.key() == key_slice {
                        sst_iter.next()?
                    }
                }
            }
            _ => {}
        }

        Ok(MergeIterator::create(sstable_iters))
    }

    fn to_concat_iter(
        engine: Arc<LsmStorageState>,
        sst_indices: &[usize],
        _lower: Bound<&[u8]>,
    ) -> Result<SstConcatIterator> {
        let mut tables = Vec::<Arc<SsTable>>::new();
        for idx in sst_indices {
            let table = engine.sstables.get(idx).map(Arc::clone);
            if let Some(t) = table {
                tables.push(t)
            } else {
                return Err(anyhow!("Sstable {} not found!", idx));
            }
        }
        match _lower {
            Bound::Included(key) => {
                SstConcatIterator::create_and_seek_to_key(tables, KeySlice::from_slice(key))
            }
            Bound::Excluded(key) => {
                let mut iter =
                    SstConcatIterator::create_and_seek_to_key(tables, KeySlice::from_slice(key))?;
                if iter.is_valid() && iter.key().raw_ref() == key {
                    iter.next()?;
                }
                Ok(iter)
            }
            _ => SstConcatIterator::create_and_seek_to_first(tables),
        }
    }

    fn to_merge_concat_iter(
        engine: Arc<LsmStorageState>,
        _lower: Bound<&[u8]>,
        _upper: Bound<&[u8]>,
    ) -> Result<MergeIterator<SstConcatIterator>> {
        let mut concat_iters = Vec::<Box<SstConcatIterator>>::new();
        for (level, sst_indices) in &engine.levels {
            if sst_indices.is_empty() {
                continue;
            }
            concat_iters.push(Box::new(Self::to_concat_iter(
                Arc::clone(&engine),
                sst_indices,
                _lower,
            )?));
        }
        Ok(MergeIterator::create(concat_iters))
    }

    /// Create an iterator over a range of keys.
    pub fn scan(
        &self,
        _lower: Bound<&[u8]>,
        _upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };

        let memtable_merge_iter =
            Self::to_memtable_merge_iter(Arc::clone(&snapshot), _lower, _upper);
        // New sstables are pushed into l0 continuously so they may have overlaps. We still need to use MergeIterator for l0.
        let l0_sst_merge_iter = Self::to_l0_sst_merge_iter(Arc::clone(&snapshot), _lower, _upper)?;
        // Sstables in levels other than l0 don't have overlaps so SstConcatIterator can be applied here.
        let merge_concat_iter = Self::to_merge_concat_iter(Arc::clone(&snapshot), _lower, _upper)?;

        let memtable_and_l0_merge_iter =
            TwoMergeIterator::create(memtable_merge_iter, l0_sst_merge_iter)?;
        let iter = LsmIterator::new(
            TwoMergeIterator::create(memtable_and_l0_merge_iter, merge_concat_iter)?,
            _upper.map(Bytes::copy_from_slice),
        )?;
        Ok(FusedIterator::new(iter))
    }
}
