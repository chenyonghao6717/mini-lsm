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

use anyhow::Result;

use super::StorageIterator;

enum CompareState {
    ALess,
    BLess,
    Equal,
}

/// Merges two iterators of different types into one. If the two iterators have the same key, only
/// produce the key once and prefer the entry from A.
pub struct TwoMergeIterator<A: StorageIterator, B: StorageIterator> {
    a: A,
    b: B,
    compare_state: CompareState,
}

impl<
    A: 'static + StorageIterator,
    B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
> TwoMergeIterator<A, B>
{
    fn get_compare_state(a: &A, b: &B) -> CompareState {
        if !a.is_valid() {
            CompareState::BLess
        } else if !b.is_valid() {
            CompareState::ALess
        } else {
            let key_a = a.key();
            let key_b = b.key();
            if key_a == key_b {
                CompareState::Equal
            } else if key_a < key_b {
                CompareState::ALess
            } else {
                CompareState::BLess
            }
        }
    }

    pub fn create(a: A, b: B) -> Result<Self> {
        let compare_state = Self::get_compare_state(&a, &b);
        Ok(Self {
            a,
            b,
            compare_state,
        })
    }
}

impl<
    A: 'static + StorageIterator,
    B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
> StorageIterator for TwoMergeIterator<A, B>
{
    type KeyType<'a> = A::KeyType<'a>;

    fn key(&self) -> Self::KeyType<'_> {
        match &self.compare_state {
            CompareState::ALess | CompareState::Equal => self.a.key(),
            CompareState::BLess => self.b.key(),
        }
    }

    fn value(&self) -> &[u8] {
        match &self.compare_state {
            CompareState::ALess | CompareState::Equal => self.a.value(),
            CompareState::BLess => self.b.value(),
        }
    }

    fn is_valid(&self) -> bool {
        self.a.is_valid() || self.b.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        match self.compare_state {
            CompareState::ALess => {
                self.a.next()?;
            }
            CompareState::BLess => {
                self.b.next()?;
            }
            CompareState::Equal => {
                self.a.next()?;
                self.b.next()?;
            }
        }
        self.compare_state = Self::get_compare_state(&self.a, &self.b);
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.a.num_active_iterators() + self.b.num_active_iterators()
    }
}
