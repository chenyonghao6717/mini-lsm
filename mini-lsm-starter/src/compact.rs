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

mod leveled;
mod simple_leveled;
mod tiered;

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, anyhow};
pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
use serde::{Deserialize, Serialize};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};

use crate::iterators::StorageIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::lsm_storage::{LsmStorageInner, LsmStorageState};
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

#[derive(Debug, Serialize, Deserialize)]
pub enum CompactionTask {
    Leveled(LeveledCompactionTask),
    Tiered(TieredCompactionTask),
    Simple(SimpleLeveledCompactionTask),
    ForceFullCompaction {
        l0_sstables: Vec<usize>,
        l1_sstables: Vec<usize>,
    },
}

impl CompactionTask {
    fn compact_to_bottom_level(&self) -> bool {
        match self {
            CompactionTask::ForceFullCompaction { .. } => true,
            CompactionTask::Leveled(task) => task.is_lower_level_bottom_level,
            CompactionTask::Simple(task) => task.is_lower_level_bottom_level,
            CompactionTask::Tiered(task) => task.bottom_tier_included,
        }
    }
}

pub(crate) enum CompactionController {
    Leveled(LeveledCompactionController),
    Tiered(TieredCompactionController),
    Simple(SimpleLeveledCompactionController),
    NoCompaction,
}

impl CompactionController {
    pub fn generate_compaction_task(&self, snapshot: &LsmStorageState) -> Option<CompactionTask> {
        match self {
            CompactionController::Leveled(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Leveled),
            CompactionController::Simple(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Simple),
            CompactionController::Tiered(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Tiered),
            CompactionController::NoCompaction => unreachable!(),
        }
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &CompactionTask,
        output: &[usize],
        in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        match (self, task) {
            (CompactionController::Leveled(ctrl), CompactionTask::Leveled(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output, in_recovery)
            }
            (CompactionController::Simple(ctrl), CompactionTask::Simple(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Tiered(ctrl), CompactionTask::Tiered(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            _ => unreachable!(),
        }
    }
}

impl CompactionController {
    pub fn flush_to_l0(&self) -> bool {
        matches!(
            self,
            Self::Leveled(_) | Self::Simple(_) | Self::NoCompaction
        )
    }
}

#[derive(Debug, Clone)]
pub enum CompactionOptions {
    /// Leveled compaction with partial compaction + dynamic level support (= RocksDB's Leveled
    /// Compaction)
    Leveled(LeveledCompactionOptions),
    /// Tiered compaction (= RocksDB's universal compaction)
    Tiered(TieredCompactionOptions),
    /// Simple leveled compaction
    Simple(SimpleLeveledCompactionOptions),
    /// In no compaction mode (week 1), always flush to L0
    NoCompaction,
}

enum IteratorType {
    // If some sstables have overlaps.
    Merge,
    // If all sstables are sorted and don't have overlaps.
    Concat,
}

impl LsmStorageInner {
    fn collect_sstables(
        sst_ids: &[usize],
        snapshot: &LsmStorageState,
    ) -> Result<Vec<Arc<SsTable>>> {
        sst_ids
            .iter()
            .map(|idx| {
                snapshot
                    .sstables
                    .get(idx)
                    .ok_or_else(|| anyhow!("Sstable of idx {} not found", idx))
                    .map(Arc::clone)
            })
            .collect::<Result<Vec<_>>>()
    }

    fn get_table_merge_iter(
        sst_ids: &[usize],
        snapshot: &LsmStorageState,
    ) -> Result<MergeIterator<SsTableIterator>> {
        let sstable_iters = Self::collect_sstables(sst_ids, snapshot)?
            .iter()
            .map(|table| SsTableIterator::create_and_seek_to_first(Arc::clone(table)).map(Box::new))
            .collect::<Result<_>>()?;
        Ok(MergeIterator::create(sstable_iters))
    }

    fn update_engine_sstables(
        engine: &mut LsmStorageState,
        consumed_sst_ids: &[usize],
        new_sstables: Vec<Arc<SsTable>>,
    ) {
        let sstables = &mut engine.sstables;
        for consumed_id in consumed_sst_ids {
            sstables.remove(consumed_id);
        }
        for sstable in new_sstables {
            sstables.insert(sstable.sst_id(), sstable);
        }
    }

    fn compact_tier(
        &self,
        task: &TieredCompactionTask,
        snapshot: &LsmStorageState,
    ) -> Result<Vec<Arc<SsTable>>> {
        if let Some((last_tier, upper_tiers)) = task.tiers.split_last() {
            let upper_tier_sst_ids = upper_tiers
                .iter()
                .flat_map(|(_, tier)| tier.to_vec())
                .collect::<Vec<usize>>();
            let last_tier_sst_ids = last_tier.1.to_vec();
            self.compact_2_levels(
                snapshot,
                &upper_tier_sst_ids,
                &last_tier_sst_ids,
                task.bottom_tier_included,
            )
        } else {
            Ok(Vec::new())
        }
    }

    fn compact_2_levels(
        &self,
        snapshot: &LsmStorageState,
        upper_sst_ids: &[usize],
        lower_sst_ids: &[usize],
        is_lower_level_bottom_level: bool,
    ) -> Result<Vec<Arc<SsTable>>> {
        let mut two_merge_iterator = TwoMergeIterator::create(
            Self::get_table_merge_iter(upper_sst_ids, snapshot)?,
            Self::get_table_merge_iter(lower_sst_ids, snapshot)?,
        )?;
        let mut new_tables = Vec::<Arc<SsTable>>::new();
        let mut cur_builder = SsTableBuilder::new(self.options.block_size);

        while two_merge_iterator.is_valid() {
            if cur_builder.estimated_size() > self.options.target_sst_size {
                let id = self.next_sst_id();
                let new_table = cur_builder.build(
                    id,
                    Some(Arc::clone(&self.block_cache)),
                    self.path_of_sst(id),
                )?;
                new_tables.push(Arc::new(new_table));
                cur_builder = SsTableBuilder::new(self.options.block_size);
            }
            let value = two_merge_iterator.value();
            // Remove empty values in the lowest layer.
            if !is_lower_level_bottom_level || !value.is_empty() {
                cur_builder.add(two_merge_iterator.key(), two_merge_iterator.value());
            }
            two_merge_iterator.next()?;
        }

        // Process the last current builder.
        if cur_builder.estimated_size() > 0 {
            let id = self.next_sst_id();
            let new_table = cur_builder.build(
                id,
                Some(Arc::clone(&self.block_cache)),
                self.path_of_sst(id),
            )?;
            new_tables.push(Arc::new(new_table));
        }

        Ok(new_tables)
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };

        let l0_sst_ids = snapshot.l0_sstables.to_vec();
        let l1_sst_ids = snapshot
            .levels
            .iter()
            .find(|level| level.0 == 1)
            .map(|level| level.1.to_vec())
            .unwrap();

        // Replace with the new engine
        let new_l1_tables = self.compact_2_levels(&snapshot, &l0_sst_ids, &l1_sst_ids, true)?;
        let state_lock = self.state_lock.lock();
        let mut new_engine = {
            let guard = self.state.read();
            (**guard).clone()
        };

        // Drop old tables
        for idx in &snapshot.l0_sstables {
            new_engine.sstables.remove(idx);
        }
        let l1_sstables = snapshot
            .levels
            .iter()
            .find(|level| level.0 == 1)
            .map(|level| level.1.to_vec())
            .unwrap();
        for idx in l1_sstables {
            new_engine.sstables.remove(&idx);
        }

        // Update table ids.
        // During merging, new l0 tables might be added, them should be kept.
        new_engine
            .l0_sstables
            .retain(|idx| !snapshot.l0_sstables.contains(idx));
        new_engine.levels.retain(|level| level.0 != 1);
        let l1_sstables = new_l1_tables
            .iter()
            .map(|table| table.sst_id())
            .collect::<Vec<usize>>();
        new_engine.levels.insert(0, (1, l1_sstables));

        // Add new tables into the new engine.
        for table in new_l1_tables {
            new_engine.sstables.insert(table.sst_id(), table);
        }

        let mut guard = self.state.write();
        *guard = Arc::new(new_engine);

        Ok(())
    }

    fn trigger_simple_compaction(
        &self,
        snapshot: &LsmStorageState,
        controller: SimpleLeveledCompactionController,
    ) -> Result<()> {
        let _task = controller.generate_compaction_task(snapshot);
        if _task.is_none() {
            return Ok(());
        }

        let task = _task.unwrap();
        let compacted_sstabls = self.compact_2_levels(
            snapshot,
            &task.upper_level_sst_ids,
            &task.lower_level_sst_ids,
            task.is_lower_level_bottom_level,
        )?;
        let compacted_sst_ids = compacted_sstabls
            .iter()
            .map(|sst| sst.sst_id())
            .collect::<Vec<usize>>();

        let (mut new_engine, consumed_sst_ids) =
            controller.apply_compaction_result(snapshot, &task, &compacted_sst_ids);
        Self::update_engine_sstables(&mut new_engine, &consumed_sst_ids, compacted_sstabls);

        let mut guard = self.state.write();
        *guard = Arc::new(new_engine);

        Ok(())
    }

    fn trigger_tier_compaction(
        &self,
        snapshot: &LsmStorageState,
        controller: TieredCompactionController,
    ) -> Result<()> {
        let _task = controller.generate_compaction_task(snapshot);
        if _task.is_none() {
            return Ok(());
        }

        let task = _task.unwrap();
        let compacted_sstables = self.compact_tier(&task, snapshot)?;
        let compacted_sst_ids = compacted_sstables
            .iter()
            .map(|sst| sst.sst_id())
            .collect::<Vec<usize>>();

        let (mut new_engine, consumed_sst_ids) =
            controller.apply_compaction_result(snapshot, &task, &compacted_sst_ids);
        Self::update_engine_sstables(&mut new_engine, &consumed_sst_ids, compacted_sstables);

        let mut guard = self.state.write();
        *guard = Arc::new(new_engine);

        Ok(())
    }

    fn trigger_compaction(&self) -> Result<()> {
        let _lock = self.state_lock.lock();
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };
        match &self.options.compaction_options {
            CompactionOptions::Simple(options) => {
                let controller = SimpleLeveledCompactionController::new(options.clone());
                self.trigger_simple_compaction(&snapshot, controller)
            }
            CompactionOptions::Tiered(options) => {
                let controller = TieredCompactionController::new(options.clone());
                self.trigger_tier_compaction(&snapshot, controller)
            }
            _ => unimplemented!(),
        }
    }

    pub(crate) fn spawn_compaction_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        if let CompactionOptions::Leveled(_)
        | CompactionOptions::Simple(_)
        | CompactionOptions::Tiered(_) = self.options.compaction_options
        {
            let this = self.clone();
            let handle = std::thread::spawn(move || {
                let ticker = crossbeam_channel::tick(Duration::from_millis(50));
                loop {
                    crossbeam_channel::select! {
                        recv(ticker) -> _ => if let Err(e) = this.trigger_compaction() {
                            eprintln!("compaction failed: {}", e);
                        },
                        recv(rx) -> _ => return
                    }
                }
            });
            return Ok(Some(handle));
        }
        Ok(None)
    }

    fn trigger_flush(&self) -> Result<()> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };

        let mutable_memtable_num = 1;
        if self.options.num_memtable_limit < snapshot.imm_memtables.len() + mutable_memtable_num {
            self.force_flush_next_imm_memtable()?;
        }

        Ok(())
    }

    pub(crate) fn spawn_flush_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        let this = self.clone();
        let handle = std::thread::spawn(move || {
            let ticker = crossbeam_channel::tick(Duration::from_millis(50));
            loop {
                crossbeam_channel::select! {
                    recv(ticker) -> _ => if let Err(e) = this.trigger_flush() {
                        eprintln!("flush failed: {}", e);
                    },
                    recv(rx) -> _ => return
                }
            }
        });
        Ok(Some(handle))
    }
}
