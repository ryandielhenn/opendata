use std::collections::{HashMap, HashSet};

use promql_parser::label::{METRIC_NAME, MatchOp};
use promql_parser::parser::VectorSelector;
use roaring::RoaringBitmap;

use crate::index::{ForwardIndex, InvertedIndex};
use crate::minitsdb::MiniTsdb;
use crate::model::{Attribute, SeriesId, SeriesSpec};
use crate::storage::OpenTsdbStorageReadExt;
use crate::util::Result;

/// Evaluates a PromQL vector selector against the MiniTsdb.
///
/// Uses a layered approach: evaluate on each tier (head, frozen, storage)
/// independently, then union the results. Data is non-overlapping across tiers.
pub(crate) async fn evaluate_selector(
    tsdb: &MiniTsdb,
    selector: &VectorSelector,
) -> Result<HashSet<SeriesId>> {
    let terms = extract_equality_terms(selector);
    if terms.is_empty() {
        return Ok(HashSet::new());
    }

    let state = tsdb.state().read().await;

    // Evaluate on head
    let head_result = evaluate_on_indexes(
        state.head().forward_index(),
        state.head().inverted_index(),
        selector,
    );

    // Evaluate on frozen head if present
    let frozen_result = state
        .frozen_head()
        .map(|frozen| {
            evaluate_on_indexes(frozen.forward_index(), frozen.inverted_index(), selector)
        })
        .unwrap_or_default();

    // Evaluate on storage
    // TODO(query-cache): Cache storage data to avoid loading on every query.
    let storage_inverted = state
        .snapshot()
        .get_inverted_index_terms(tsdb.bucket(), &terms)
        .await?;

    let storage_candidates = find_candidates_from_map(&storage_inverted, &terms);
    let storage_result = if !storage_candidates.is_empty() && has_not_equal_matchers(selector) {
        let series_ids: Vec<SeriesId> = storage_candidates.iter().copied().collect();
        let storage_forward = state
            .snapshot()
            .get_forward_index_series(tsdb.bucket(), &series_ids)
            .await?;
        apply_not_equal_matchers_map(&storage_forward, storage_candidates, selector)
    } else {
        storage_candidates
    };

    // Merge results from all layers
    let mut result = head_result;
    result.extend(frozen_result);
    result.extend(storage_result);
    Ok(result)
}

/// Evaluate selector on in-memory indexes.
fn evaluate_on_indexes(
    forward_index: &ForwardIndex,
    inverted_index: &InvertedIndex,
    selector: &VectorSelector,
) -> HashSet<SeriesId> {
    let terms = extract_equality_terms(selector);
    if terms.is_empty() {
        return HashSet::new();
    }

    let candidates: HashSet<SeriesId> = inverted_index.intersect(terms).iter().collect();
    if candidates.is_empty() || !has_not_equal_matchers(selector) {
        return candidates;
    }

    apply_not_equal_matchers(forward_index, candidates, selector)
}

/// Extract equality terms from the selector.
fn extract_equality_terms(selector: &VectorSelector) -> Vec<Attribute> {
    let mut terms = Vec::new();
    if let Some(ref name) = selector.name {
        terms.push(Attribute {
            key: METRIC_NAME.to_string(),
            value: name.clone(),
        });
    }
    for matcher in &selector.matchers.matchers {
        if matches!(matcher.op, MatchOp::Equal) {
            terms.push(Attribute {
                key: matcher.name.clone(),
                value: matcher.value.clone(),
            });
        }
    }
    terms
}

fn has_not_equal_matchers(selector: &VectorSelector) -> bool {
    selector
        .matchers
        .matchers
        .iter()
        .any(|m| matches!(m.op, MatchOp::NotEqual))
}

/// Find candidates by intersecting posting lists from a HashMap.
fn find_candidates_from_map(
    postings: &HashMap<Attribute, RoaringBitmap>,
    terms: &[Attribute],
) -> HashSet<SeriesId> {
    if terms.is_empty() {
        return HashSet::new();
    }

    let mut bitmaps: Vec<&RoaringBitmap> = Vec::new();
    for term in terms {
        match postings.get(term) {
            Some(bitmap) => bitmaps.push(bitmap),
            None => return HashSet::new(),
        }
    }

    bitmaps.sort_by_key(|b| b.len());
    let mut result = bitmaps[0].clone();
    for bitmap in &bitmaps[1..] {
        result &= *bitmap;
    }
    result.iter().collect()
}

/// Apply not-equal matchers using ForwardIndex.
fn apply_not_equal_matchers(
    forward_index: &ForwardIndex,
    mut candidates: HashSet<SeriesId>,
    selector: &VectorSelector,
) -> HashSet<SeriesId> {
    for matcher in selector
        .matchers
        .matchers
        .iter()
        .filter(|m| matches!(m.op, MatchOp::NotEqual))
    {
        candidates.retain(|id| {
            forward_index
                .series
                .get(id)
                .map(|spec| !has_attr(&spec.attributes, &matcher.name, &matcher.value))
                .unwrap_or(false)
        });
    }
    candidates
}

/// Apply not-equal matchers using a HashMap.
fn apply_not_equal_matchers_map(
    forward_index: &HashMap<SeriesId, SeriesSpec>,
    mut candidates: HashSet<SeriesId>,
    selector: &VectorSelector,
) -> HashSet<SeriesId> {
    for matcher in selector
        .matchers
        .matchers
        .iter()
        .filter(|m| matches!(m.op, MatchOp::NotEqual))
    {
        candidates.retain(|id| {
            forward_index
                .get(id)
                .map(|spec| !has_attr(&spec.attributes, &matcher.name, &matcher.value))
                .unwrap_or(false)
        });
    }
    candidates
}

fn has_attr(attributes: &[Attribute], key: &str, value: &str) -> bool {
    attributes.iter().any(|a| a.key == key && a.value == value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{MetricType, SeriesSpec};
    use promql_parser::label::{Matcher, Matchers};

    fn empty_matchers() -> Matchers {
        Matchers {
            matchers: vec![],
            or_matchers: vec![],
        }
    }

    fn create_test_indexes() -> (ForwardIndex, InvertedIndex) {
        let forward = ForwardIndex::default();
        let inverted = InvertedIndex::default();

        let series = vec![
            (1, "http_requests_total", "GET", "prod"),
            (2, "http_requests_total", "POST", "prod"),
            (3, "http_requests_total", "GET", "staging"),
        ];

        for (id, metric, method, env) in series {
            let attrs = vec![
                Attribute {
                    key: METRIC_NAME.to_string(),
                    value: metric.to_string(),
                },
                Attribute {
                    key: "method".to_string(),
                    value: method.to_string(),
                },
                Attribute {
                    key: "env".to_string(),
                    value: env.to_string(),
                },
            ];
            forward.series.insert(
                id,
                SeriesSpec {
                    metric_unit: None,
                    metric_type: MetricType::Gauge,
                    attributes: attrs.clone(),
                },
            );
            for attr in attrs {
                inverted
                    .postings
                    .entry(attr)
                    .or_default()
                    .value_mut()
                    .insert(id);
            }
        }
        (forward, inverted)
    }

    #[test]
    fn should_match_by_metric_name() {
        let (forward, inverted) = create_test_indexes();
        let selector = VectorSelector {
            name: Some("http_requests_total".to_string()),
            matchers: empty_matchers(),
            offset: None,
            at: None,
        };

        let result = evaluate_on_indexes(&forward, &inverted, &selector);

        assert_eq!(result.len(), 3);
    }

    #[test]
    fn should_match_by_equality_matcher() {
        let (forward, inverted) = create_test_indexes();
        let selector = VectorSelector {
            name: Some("http_requests_total".to_string()),
            matchers: Matchers {
                matchers: vec![Matcher::new(MatchOp::Equal, "method", "GET")],
                or_matchers: vec![],
            },
            offset: None,
            at: None,
        };

        let result = evaluate_on_indexes(&forward, &inverted, &selector);

        assert_eq!(result.len(), 2);
        assert!(result.contains(&1));
        assert!(result.contains(&3));
    }

    #[test]
    fn should_exclude_by_not_equal_matcher() {
        let (forward, inverted) = create_test_indexes();
        let selector = VectorSelector {
            name: Some("http_requests_total".to_string()),
            matchers: Matchers {
                matchers: vec![Matcher::new(MatchOp::NotEqual, "method", "GET")],
                or_matchers: vec![],
            },
            offset: None,
            at: None,
        };

        let result = evaluate_on_indexes(&forward, &inverted, &selector);

        assert_eq!(result.len(), 1);
        assert!(result.contains(&2));
    }

    #[test]
    fn should_combine_equal_and_not_equal() {
        let (forward, inverted) = create_test_indexes();
        let selector = VectorSelector {
            name: Some("http_requests_total".to_string()),
            matchers: Matchers {
                matchers: vec![
                    Matcher::new(MatchOp::Equal, "method", "GET"),
                    Matcher::new(MatchOp::NotEqual, "env", "staging"),
                ],
                or_matchers: vec![],
            },
            offset: None,
            at: None,
        };

        let result = evaluate_on_indexes(&forward, &inverted, &selector);

        assert_eq!(result.len(), 1);
        assert!(result.contains(&1));
    }

    #[test]
    fn should_return_empty_for_unknown_metric() {
        let (forward, inverted) = create_test_indexes();
        let selector = VectorSelector {
            name: Some("unknown".to_string()),
            matchers: empty_matchers(),
            offset: None,
            at: None,
        };

        let result = evaluate_on_indexes(&forward, &inverted, &selector);

        assert!(result.is_empty());
    }

    #[test]
    fn should_return_empty_for_no_equality_matchers() {
        let (forward, inverted) = create_test_indexes();
        let selector = VectorSelector {
            name: None,
            matchers: empty_matchers(),
            offset: None,
            at: None,
        };

        let result = evaluate_on_indexes(&forward, &inverted, &selector);

        assert!(result.is_empty());
    }
}
