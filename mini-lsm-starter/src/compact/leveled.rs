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

use std::cmp::max;

use serde::{Deserialize, Serialize};

use crate::{key::KeyBytes, lsm_storage::LsmStorageState};

const MB: usize = 1024 * 1024;

#[derive(Debug, Serialize, Deserialize)]
pub struct LeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

#[derive(Debug, Clone)]
pub struct LeveledCompactionOptions {
    pub level_size_multiplier: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
    pub base_level_size_mb: usize,
}

pub struct LeveledCompactionController {
    options: LeveledCompactionOptions,
}

impl LeveledCompactionController {
    pub fn new(options: LeveledCompactionOptions) -> Self {
        Self { options }
    }

    fn get_level_size(_snapshot: &LsmStorageState, level_id: usize) -> usize {
        let level_idx = level_id - 1;
        let (_, level) = &_snapshot.levels[level_idx];
        let level_sstables = level
            .iter()
            .filter_map(|id| _snapshot.sstables.get(id))
            .collect::<Vec<_>>();

        level_sstables
            .iter()
            .map(|sst| sst.file.size() as usize)
            .sum::<usize>()
    }

    fn get_level_target_sizes(&self, _snapshot: &LsmStorageState) -> Vec<usize> {
        let max_level_id = _snapshot.levels.len();
        let last_level_size_byte = Self::get_level_size(_snapshot, max_level_id);

        let mut target_sizes = vec![0; max_level_id];
        target_sizes[max_level_id - 1] =
            max(last_level_size_byte, self.options.base_level_size_mb * MB);

        for level_id in (1..max_level_id).rev() {
            let level_idx = level_id - 1;
            let next_level_target_size = target_sizes[level_idx + 1];
            if next_level_target_size <= self.options.base_level_size_mb * MB {
                break;
            }
            // According to the design, there is at most 1 level with a size <= base_level_size_mb.
            // All levels upper of it have a size of 0.
            target_sizes[level_idx] = next_level_target_size / self.options.level_size_multiplier;
        }
        target_sizes
    }

    fn find_first_level_with_positive_target_size(&self, _snapshot: &LsmStorageState) -> usize {
        let level_target_sizes = self.get_level_target_sizes(_snapshot);
        let first_level_with_positive_target_size_idx = level_target_sizes
            .iter()
            .position(|size| *size > 0)
            .unwrap();
        first_level_with_positive_target_size_idx + 1
    }

    fn get_level_priorities(&self, _snapshot: &LsmStorageState) -> Vec<f64> {
        let level_sizes = _snapshot
            .levels
            .iter()
            .map(|(id, _)| Self::get_level_size(_snapshot, *id))
            .collect::<Vec<usize>>();
        let target_sizes = self.get_level_target_sizes(_snapshot);
        level_sizes
            .iter()
            .zip(target_sizes.iter())
            .map(|(actual, target)| {
                if *target == 0 {
                    0.0
                } else {
                    *actual as f64 / *target as f64
                }
            })
            .collect()
    }

    fn get_top_priority_level(&self, _snapshot: &LsmStorageState) -> Option<usize> {
        let mut max_priority: f64 = 0.0;
        let mut max_priority_id: Option<usize> = None;
        let last_level_id = _snapshot.levels.len();
        let priorities = self.get_level_priorities(_snapshot);
        priorities.iter().enumerate().for_each(|(idx, priority)| {
            let level_id = idx + 1;
            // Always merge Ln with Ln+1 so current level can't be the last level.
            if *priority > 1.0 && *priority > max_priority && level_id < last_level_id {
                max_priority = *priority;
                max_priority_id = Some(level_id);
            }
        });
        max_priority_id
    }

    fn find_key_range(_snapshot: &LsmStorageState, _sst_ids: &[usize]) -> (KeyBytes, KeyBytes) {
        let first_key = _sst_ids
            .iter()
            .filter_map(|id| _snapshot.sstables.get(id).map(|sst| sst.first_key()))
            .min()
            .cloned()
            .unwrap();
        let last_key = _sst_ids
            .iter()
            .filter_map(|id| _snapshot.sstables.get(id).map(|sst| sst.last_key()))
            .max()
            .cloned()
            .unwrap();
        (first_key, last_key)
    }

    fn find_overlapping_ssts(
        _snapshot: &LsmStorageState,
        _sst_ids: &[usize],
        // Lower level
        _in_level: usize,
    ) -> Vec<usize> {
        let (first_key, last_key) = Self::find_key_range(_snapshot, _sst_ids);
        _snapshot.levels[_in_level - 1]
            .1
            .iter()
            .filter_map(|id| _snapshot.sstables.get(id))
            .filter(|lower_sst| {
                !(&last_key < lower_sst.first_key() || &first_key > lower_sst.last_key())
            })
            .map(|sst| sst.sst_id())
            .collect::<Vec<usize>>()
    }

    fn generate_merge_l0_task(&self, _snapshot: &LsmStorageState) -> Option<LeveledCompactionTask> {
        let upper_level_sst_ids = _snapshot.l0_sstables.to_vec();
        let first_non_empty_level_id = self.find_first_level_with_positive_target_size(_snapshot);
        let overlapping_lower_sst_ids =
            Self::find_overlapping_ssts(_snapshot, &upper_level_sst_ids, first_non_empty_level_id);
        Some(LeveledCompactionTask {
            upper_level: None,
            upper_level_sst_ids,
            lower_level: first_non_empty_level_id,
            lower_level_sst_ids: overlapping_lower_sst_ids,
            is_lower_level_bottom_level: first_non_empty_level_id == _snapshot.levels.len(),
        })
    }

    fn generate_non_merge_l0_task(
        &self,
        _snapshot: &LsmStorageState,
    ) -> Option<LeveledCompactionTask> {
        let top_priority_level = self.get_top_priority_level(_snapshot)?;
        let first_upper_sst_id = *_snapshot.levels[top_priority_level - 1].1.first().unwrap();
        let upper_level_sst_ids = vec![first_upper_sst_id];
        let overlapping_lower_sst_ids =
            Self::find_overlapping_ssts(_snapshot, &upper_level_sst_ids, top_priority_level + 1);
        Some(LeveledCompactionTask {
            upper_level: Some(top_priority_level),
            upper_level_sst_ids,
            lower_level: top_priority_level + 1,
            lower_level_sst_ids: overlapping_lower_sst_ids,
            is_lower_level_bottom_level: top_priority_level + 1 == _snapshot.levels.len(),
        })
    }

    pub fn generate_compaction_task(
        &self,
        _snapshot: &LsmStorageState,
    ) -> Option<LeveledCompactionTask> {
        if _snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
            self.generate_merge_l0_task(_snapshot)
        } else {
            self.generate_non_merge_l0_task(_snapshot)
        }
    }

    fn apply_compaction_result_to_upper(
        _snapshot: &LsmStorageState,
        new_engine: &mut LsmStorageState,
        _task: &LeveledCompactionTask,
    ) {
        if let Some(upper_level_id) = _task.upper_level {
            let new_upper_level = _snapshot.levels[upper_level_id - 1]
                .1
                .iter()
                .copied()
                .filter(|id| !_task.upper_level_sst_ids.contains(id))
                .collect::<Vec<usize>>();
            new_engine.levels[upper_level_id - 1] = (upper_level_id, new_upper_level);
        } else {
            let new_l0 = _snapshot
                .l0_sstables
                .iter()
                .copied()
                .filter(|id| !_task.upper_level_sst_ids.contains(id))
                .collect::<Vec<usize>>();
            new_engine.l0_sstables = new_l0;
        }
    }

    fn apply_compaction_result_to_lower(
        _snapshot: &LsmStorageState,
        new_engine: &mut LsmStorageState,
        _task: &LeveledCompactionTask,
        _output: &[usize],
    ) {
        let lower_level_id = _task.lower_level;
        let mut new_lower_level = _snapshot.levels[lower_level_id - 1]
            .1
            .iter()
            .copied()
            .filter(|id| !_task.lower_level_sst_ids.contains(id))
            .collect::<Vec<usize>>();

        new_lower_level.extend_from_slice(_output);
        new_lower_level.sort_by(|a, b| {
            let key1 = new_engine.sstables[a].first_key();
            let key2 = new_engine.sstables[b].first_key();
            key1.cmp(key2)
        });
        new_engine.levels[lower_level_id - 1] = (lower_level_id, new_lower_level);
    }

    // Say an upper sst A is merged with all overlapping lower sst C and D, the outputs are
    // F and G, and the original lower level has B, C, D, E. C and  D need to be removed
    // while F and G need to be insert into where C, B were.
    pub fn apply_compaction_result(
        &self,
        _snapshot: &LsmStorageState,
        _task: &LeveledCompactionTask,
        _output: &[usize],
        _in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        let mut sst_ids_to_remove = Vec::<usize>::new();
        sst_ids_to_remove.extend_from_slice(&_task.upper_level_sst_ids);
        sst_ids_to_remove.extend_from_slice(&_task.lower_level_sst_ids);

        let mut new_engine = _snapshot.clone();

        Self::apply_compaction_result_to_upper(_snapshot, &mut new_engine, _task);
        Self::apply_compaction_result_to_lower(_snapshot, &mut new_engine, _task, _output);

        (new_engine, sst_ids_to_remove)
    }
}
