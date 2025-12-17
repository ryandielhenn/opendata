// Tsdb is the coordination layer for OpenTSDB, ensuring the ingestion and query
// layers are synchronized. It maintains a three-level hierarchy:
//
// 1. Mutable Head: this stores incoming data that has not yet been
//    flushed to storage. They are updated via deltas, which are applied to
//    the ehad chunk for the corresponding time bucket.
//
// 2. Frozen Head: on a trigger (currently time-based), the head
//    is frozen and its data is ready to be flushed to storage. Since the time
//    it takes to flush the data is variable, the frozen head chunks are maintained
//    in memory until the flush is complete. At this point the frozen chunk is
//    atomically discarded with the update of the storage snapshot.
//
// 3. Storage Snapshot: a snapshot of the storage layer that does not yet have the
//    any of the head chunks' data. This is used for consistency as it is possible
//    that data has already been flushed to storage before the frozen chunks are
//    discarded (which would result in duplicate reads).
//
// In addition to the three levels, the Tsdb maintains a cache of the entire series
// dictionary for each active time bucket since this is required for ingestion.

use std::sync::{Arc, atomic::AtomicU32};

use async_trait::async_trait;
use dashmap::DashMap;
use opendata_common::{Storage, StorageRead};
use tokio::sync::{Mutex, RwLock, RwLockReadGuard};

use roaring::RoaringBitmap;

use crate::delta::{TsdbDelta, TsdbDeltaBuilder};
use crate::head::TsdbHead;
use crate::index::{ForwardIndex, ForwardIndexLookup, InvertedIndex, InvertedIndexLookup};
use crate::model::{
    Attribute, Sample, SampleWithAttributes, SeriesFingerprint, SeriesId, SeriesSpec, TimeBucket,
};
use crate::query::QueryReader;
use crate::serde::key::TimeSeriesKey;
use crate::serde::timeseries::TimeSeriesIterator;
use crate::storage::OpenTsdbStorageReadExt;
use crate::util::{OpenTsdbError, Result};

/// Internal state of the MiniTsdb, protected by RwLock.
struct MiniState {
    head: TsdbHead,
    frozen_head: Option<TsdbHead>,
    snapshot: Arc<dyn StorageRead>,
}

/// A view into forward index data across all tiers.
/// Provides efficient lookups without cloning data from head/frozen tiers upfront.
/// Only data from storage is pre-loaded (for series not found in head/frozen).
pub(crate) struct ForwardIndexView<'a> {
    head: &'a ForwardIndex,
    frozen: Option<&'a ForwardIndex>,
    storage: ForwardIndex,
}

impl ForwardIndexLookup for ForwardIndexView<'_> {
    fn get_spec(&self, series_id: &SeriesId) -> Option<SeriesSpec> {
        // Check head first
        if let Some(spec) = self.head.series.get(series_id) {
            return Some(spec.value().clone());
        }
        // Check frozen
        if let Some(frozen) = self.frozen
            && let Some(spec) = frozen.series.get(series_id)
        {
            return Some(spec.value().clone());
        }
        // Check storage
        if let Some(spec) = self.storage.series.get(series_id) {
            return Some(spec.value().clone());
        }
        None
    }
}

/// A view into inverted index data across all tiers.
/// Provides efficient lookups by unioning bitmaps from head/frozen/storage on demand.
/// Only data from storage is pre-loaded (for terms not fully covered by head/frozen).
pub(crate) struct InvertedIndexView<'a> {
    head: &'a InvertedIndex,
    frozen: Option<&'a InvertedIndex>,
    storage: InvertedIndex,
}

impl InvertedIndexLookup for InvertedIndexView<'_> {
    fn intersect(&self, terms: Vec<Attribute>) -> RoaringBitmap {
        if terms.is_empty() {
            return RoaringBitmap::new();
        }

        // For each term, get the union of bitmaps across all tiers
        let mut term_bitmaps: Vec<RoaringBitmap> = Vec::with_capacity(terms.len());

        for term in &terms {
            let mut bitmap = RoaringBitmap::new();

            // Union from head
            if let Some(head_bitmap) = self.head.postings.get(term) {
                bitmap |= head_bitmap.value();
            }

            // Union from frozen
            if let Some(frozen) = self.frozen
                && let Some(frozen_bitmap) = frozen.postings.get(term)
            {
                bitmap |= frozen_bitmap.value();
            }

            // Union from storage
            if let Some(storage_bitmap) = self.storage.postings.get(term) {
                bitmap |= storage_bitmap.value();
            }

            // If any term has no matches, the intersection is empty
            if bitmap.is_empty() {
                return RoaringBitmap::new();
            }

            term_bitmaps.push(bitmap);
        }

        // Sort by size for efficient intersection
        term_bitmaps.sort_by_key(|b| b.len());

        // Intersect all term bitmaps
        let mut result = term_bitmaps.remove(0);
        for bitmap in term_bitmaps {
            result &= bitmap;
        }

        result
    }
}

/// Read-only view for queries across all data tiers.
/// Hides the head/frozen/storage layering from callers.
///
/// This struct provides methods to query data without exposing
/// the internal three-tier architecture (head, frozen head, storage).
pub(crate) struct MiniQueryReader<'a> {
    bucket: &'a TimeBucket,
    head: &'a TsdbHead,
    frozen_head: Option<&'a TsdbHead>,
    snapshot: &'a Arc<dyn StorageRead>,
}

#[async_trait]
impl<'a> QueryReader for MiniQueryReader<'a> {
    fn bucket(&self) -> &TimeBucket {
        self.bucket
    }

    async fn forward_index_view(
        &self,
        series_ids: &[SeriesId],
    ) -> Result<Box<dyn ForwardIndexLookup + Send + Sync + '_>> {
        // Find which IDs are missing from head/frozen and need to be loaded from storage
        let missing: Vec<SeriesId> = series_ids
            .iter()
            .filter(|&id| {
                // Not in head
                !self.head.forward_index().series.contains_key(id)
                    // Not in frozen (if present)
                    && self
                        .frozen_head
                        .map(|f| !f.forward_index().series.contains_key(id))
                        .unwrap_or(true)
            })
            .copied()
            .collect();

        // Only load from storage for missing IDs
        let storage = if !missing.is_empty() {
            self.snapshot
                .get_forward_index_series(self.bucket, &missing)
                .await?
        } else {
            ForwardIndex::default()
        };

        Ok(Box::new(ForwardIndexView {
            head: self.head.forward_index(),
            frozen: self.frozen_head.map(|f| f.forward_index()),
            storage,
        }))
    }

    async fn inverted_index_view(
        &self,
        terms: &[Attribute],
    ) -> Result<Box<dyn InvertedIndexLookup + Send + Sync + '_>> {
        // Load storage data for all terms (we need to query storage anyway)
        let storage = self
            .snapshot
            .get_inverted_index_terms(self.bucket, terms)
            .await?;

        Ok(Box::new(InvertedIndexView {
            head: self.head.inverted_index(),
            frozen: self.frozen_head.map(|f| f.inverted_index()),
            storage,
        }))
    }

    async fn get_samples(
        &self,
        series_id: SeriesId,
        start_ms: u64,
        end_ms: u64,
    ) -> Result<Vec<Sample>> {
        // Storage samples - read from storage
        let storage_key = TimeSeriesKey {
            time_bucket: self.bucket.start,
            bucket_size: self.bucket.size,
            series_id,
        };
        let storage_record = self.snapshot.get(storage_key.encode()).await?;

        // Collect storage samples immediately to avoid lifetime issues
        let storage_samples: Vec<Sample> = if let Some(record) = storage_record {
            if let Some(iter) = TimeSeriesIterator::new(record.value.as_ref()) {
                iter.collect::<std::result::Result<Vec<_>, _>>()
                    .map_err(|e| {
                        OpenTsdbError::Internal(format!(
                            "Error decoding timeseries from storage: {}",
                            e
                        ))
                    })?
            } else {
                Vec::new()
            }
        } else {
            Vec::new()
        };

        // Collect samples from all layers with priority markers
        // (timestamp_ms, value, priority) where lower priority = higher precedence
        let mut all_samples: Vec<(u64, f64, u8)> = Vec::new();

        // Collect head samples (priority 0 = highest)
        if let Some(head_samples_ref) = self.head.samples().get(&series_id) {
            for sample in head_samples_ref.iter() {
                all_samples.push((sample.timestamp, sample.value, 0));
            }
        }

        // Collect frozen head samples (priority 1)
        if let Some(frozen) = self.frozen_head
            && let Some(frozen_samples_ref) = frozen.samples().get(&series_id)
        {
            for sample in frozen_samples_ref.iter() {
                all_samples.push((sample.timestamp, sample.value, 1));
            }
        }

        // Collect storage samples (priority 2 = lowest)
        for sample in storage_samples {
            all_samples.push((sample.timestamp, sample.value, 2));
        }

        // Sort by timestamp (ascending), then by priority (ascending) for deduplication
        all_samples.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.2.cmp(&b.2)));

        // Deduplicate: for same timestamp, keep only the one with highest priority (lowest number)
        let mut result: Vec<Sample> = Vec::new();
        let mut last_timestamp: Option<u64> = None;
        for (timestamp_ms, value, _priority) in all_samples {
            if let Some(last_ts) = last_timestamp
                && timestamp_ms == last_ts
            {
                // Skip duplicates (we already have the one with better priority due to sorting)
                continue;
            }

            // Filter by time range: timestamp > start_ms && timestamp <= end_ms
            // Following PromQL lookback window semantics with exclusive start
            if timestamp_ms > start_ms && timestamp_ms <= end_ms {
                result.push(Sample {
                    timestamp: timestamp_ms,
                    value,
                });
            }
            last_timestamp = Some(timestamp_ms);
        }

        Ok(result)
    }
}

/// Guard that holds the read lock on MiniTsdb state and provides MiniQueryReader access.
///
/// This struct owns the RwLockReadGuard and provides a way to get a MiniQueryReader
/// that references the locked state.
pub(crate) struct QueryReaderGuard<'a> {
    state: RwLockReadGuard<'a, MiniState>,
    bucket: &'a TimeBucket,
}

impl<'a> QueryReaderGuard<'a> {
    /// Get the MiniQueryReader from this guard
    pub(crate) fn reader(&self) -> MiniQueryReader<'_> {
        MiniQueryReader {
            bucket: self.bucket,
            head: &self.state.head,
            frozen_head: self.state.frozen_head.as_ref(),
            snapshot: &self.state.snapshot,
        }
    }
}

pub(crate) struct MiniTsdb {
    bucket: TimeBucket,
    series_dict: DashMap<SeriesFingerprint, SeriesId>,
    next_series_id: AtomicU32,
    /// The state of the Tsdb is protected by a read-write
    /// lock to allow us to atomically flush and update the
    /// storage snapshot. Note that only the read lock is
    /// required for queries and ingestion (which modifies the
    /// underlying data structures in a thread-safe way)
    state: Arc<RwLock<MiniState>>,
    /// Mutex to ensure only one flush operation can run at a time.
    /// This prevents concurrent flushes from interfering with each other.
    flush_mutex: Arc<Mutex<()>>,
}

impl MiniTsdb {
    /// Returns a reference to the time bucket
    pub(crate) fn bucket(&self) -> &TimeBucket {
        &self.bucket
    }

    /// Create a query reader for read operations.
    /// The returned guard holds the read lock and provides access to QueryReader.
    pub(crate) async fn query_reader(&self) -> QueryReaderGuard<'_> {
        let state = self.state.read().await;
        QueryReaderGuard {
            state,
            bucket: &self.bucket,
        }
    }

    pub(crate) async fn load(bucket: TimeBucket, storage: Arc<dyn StorageRead>) -> Result<Self> {
        let series_dict = DashMap::new();
        let next_series_id = storage
            .load_series_dictionary(&bucket, |fingerprint, series_id| {
                series_dict.insert(fingerprint, series_id);
            })
            .await?;

        Ok(Self {
            bucket: bucket.clone(),
            series_dict,
            next_series_id: AtomicU32::new(next_series_id),
            state: Arc::new(RwLock::new(MiniState {
                head: TsdbHead::new(bucket.clone()),
                frozen_head: None,
                snapshot: storage,
            })),
            flush_mutex: Arc::new(Mutex::new(())),
        })
    }

    /// Ingest samples with attributes (this is the main entry point for ingestion)
    pub(crate) async fn ingest(&self, samples: Vec<SampleWithAttributes>) -> Result<()> {
        let mut builder =
            TsdbDeltaBuilder::new(self.bucket.clone(), &self.series_dict, &self.next_series_id);

        for sample in samples {
            builder.ingest(sample);
        }

        let delta = builder.build();
        self.ingest_delta(&delta).await
    }

    /// Ingest a delta directly (this can be used when ingesting data as a
    /// read-only replica to stay up to date with the main writer)
    pub(crate) async fn ingest_delta(&self, delta: &TsdbDelta) -> Result<()> {
        // TODO(agavra): log the delta to a WAL to avoid losing data
        let state = self.state.read().await;
        state.head.merge(delta)?;
        Ok(())
    }

    /// Flush the head to storage, making it durable. Eventually we will support
    /// a native WAL so that we can get durability without waiting until a flush,
    /// but for now the WAL is coupled with the storage layer so we accept the risk
    /// of losing a small amount of data in the event of a crash.
    pub(crate) async fn flush(&self, storage: Arc<dyn Storage>) -> Result<()> {
        let _flush_guard = self.flush_mutex.lock().await;

        // blocking section: freeze the head and replaces it
        // with a new head block, keeping a reference to the old
        // frozen head
        {
            let mut state = self.state.write().await;
            state.head.freeze();
            let frozen_head =
                std::mem::replace(&mut state.head, TsdbHead::new(self.bucket.clone()));
            state.frozen_head = Some(frozen_head);
        }

        // non-blocking section: flush the frozen head to storage
        // this can take time so its important that it only holds
        // the read lock
        let snapshot = {
            let state = self.state.read().await;
            let frozen_head = state
                .frozen_head
                .as_ref()
                .expect("frozen_head should be set after write lock above");
            // Clone storage to avoid moving it and for clarity
            frozen_head.flush(storage.clone()).await?
        };

        // blocking section: update the storage snapshot and
        // discard the frozen head
        {
            let mut state = self.state.write().await;
            state.snapshot = snapshot;
            state.frozen_head = None;
        }
        Ok(())
    }
}
