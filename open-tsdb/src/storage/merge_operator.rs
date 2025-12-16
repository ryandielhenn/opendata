use bytes::Bytes;

use crate::model::RecordTag;
use crate::serde::bucket_list::BucketListValue;
use crate::serde::inverted_index::InvertedIndexValue;
use crate::serde::timeseries::merge_time_series;
use crate::serde::{EncodingError, RecordType};

/// Merge operator for OpenTSDB that handles merging of different record types.
///
/// Routes merge operations to the appropriate merge function based on the
/// record type encoded in the key.
pub(crate) struct OpenTsdbMergeOperator;

impl opendata_common::storage::MergeOperator for OpenTsdbMergeOperator {
    fn merge(&self, key: &Bytes, existing_value: Option<Bytes>, new_value: Bytes) -> Bytes {
        // If no existing value, just return the new value
        let Some(existing) = existing_value else {
            return new_value;
        };

        // Decode record type from key
        if key.len() < 2 {
            panic!("Invalid key: key length is less than 2 bytes");
        }

        let record_tag =
            RecordTag::from_byte(key[1]).expect("Failed to decode record tag from key");

        let record_type = record_tag
            .record_type()
            .expect("Failed to get record type from record tag");

        match record_type {
            RecordType::InvertedIndex => {
                merge_inverted_index(existing, new_value).expect("Failed to merge inverted index")
            }
            RecordType::TimeSeries => {
                merge_time_series(existing, new_value).expect("Failed to merge time series")
            }
            RecordType::BucketList => {
                merge_bucket_list(existing, new_value).expect("Failed to merge bucket list")
            }
            _ => {
                // For other record types (SeriesDictionary, ForwardIndex), just use new value
                // These should use Put, not Merge, but handle gracefully
                new_value
            }
        }
    }
}

/// Merge inverted index posting lists by unioning RoaringBitmaps.
fn merge_inverted_index(existing: Bytes, new_value: Bytes) -> Result<Bytes, EncodingError> {
    let existing_bitmap = InvertedIndexValue::decode(existing.as_ref())?.postings;
    let new_bitmap = InvertedIndexValue::decode(new_value.as_ref())?.postings;

    let merged = existing_bitmap | new_bitmap;
    (InvertedIndexValue { postings: merged }).encode()
}

/// Merge bucket lists by taking the union of buckets and sorting.
/// Used by the merge operator to handle concurrent updates to the BucketsList.
fn merge_bucket_list(existing: Bytes, new_value: Bytes) -> Result<Bytes, EncodingError> {
    let mut base_buckets = BucketListValue::decode(existing.as_ref())?.buckets;
    let other_buckets = BucketListValue::decode(new_value.as_ref())?.buckets;

    // Add buckets from other that aren't in base
    for bucket in other_buckets {
        if !base_buckets.contains(&bucket) {
            base_buckets.push(bucket);
        }
    }

    // Sort by start time
    base_buckets.sort_by_key(|b| b.1);

    Ok(BucketListValue {
        buckets: base_buckets,
    }
    .encode())
}
