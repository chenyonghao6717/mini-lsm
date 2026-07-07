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

use std::cmp::{self};
use std::collections::BinaryHeap;
use std::collections::binary_heap::PeekMut;

use anyhow::Result;

use crate::key::{KeySlice, TS_DEFAULT};

use super::StorageIterator;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// BinaryHeap of rust is a max-heap so the compare result needs to be reversed.
///
impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.1
            .key()
            .cmp(&other.1.key())
            .then(self.0.cmp(&other.0))
            .reverse()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, prefer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
    // current is popped from the heap
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        let mut heap = BinaryHeap::new();

        for (idx, iter) in iters.into_iter().enumerate() {
            if iter.is_valid() {
                heap.push(HeapWrapper(idx, iter));
            }
        }

        let top = heap.pop();
        Self {
            current: top,
            iters: heap,
        }
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice<'_> {
        if let Some(wrapper) = &self.current {
            wrapper.1.key()
        } else {
            KeySlice::from_slice(&[], TS_DEFAULT)
        }
    }

    fn value(&self) -> &[u8] {
        if let Some(wrapper) = &self.current {
            wrapper.1.value()
        } else {
            &[]
        }
    }

    fn is_valid(&self) -> bool {
        self.current.is_some() && self.current.as_ref().unwrap().1.is_valid()
    }

    /// In this function we do:
    /// 1. advance current and all iterators have the same key of current
    /// 2. drop all invalid iterators(that don't have more values)
    fn next(&mut self) -> Result<()> {
        if !self.is_valid() {
            return Ok(());
        }

        let key = self.current.as_ref().unwrap().1.key();

        while let Some(mut wrapper) = self.iters.peek_mut() {
            if !wrapper.1.is_valid() {
                PeekMut::pop(wrapper);
            } else if wrapper.1.key() == key {
                match wrapper.1.next() {
                    Err(e) => {
                        PeekMut::pop(wrapper);
                        return Err(e);
                    }
                    Ok(()) => {
                        if !wrapper.1.is_valid() {
                            PeekMut::pop(wrapper);
                        }
                    }
                }
            } else {
                break;
            }
        }

        let mut current = self.current.take().unwrap();
        current.1.next()?;
        if current.1.is_valid() {
            self.iters.push(current);
        }

        self.current = self.iters.pop();

        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.iters.len() + if self.current.is_some() { 1 } else { 0 }
    }
}
