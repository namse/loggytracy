    use super::*;
    use crate::tenant::test_tenant;

    fn encode_btf1(line_blooms: &[BloomFilter]) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(BLOOM_MAGIC_V1);
        bytes.extend_from_slice(&(line_blooms.len() as u32).to_le_bytes());
        for bloom in line_blooms {
            let encoded = bloom.encode();
            bytes.extend_from_slice(&(encoded.len() as u32).to_le_bytes());
            bytes.extend_from_slice(&encoded);
        }
        bytes
    }

    fn encode_btf2(line_blooms: &[BloomFilter], exact_blooms: &[Option<BloomFilter>]) -> Vec<u8> {
        assert_eq!(line_blooms.len(), exact_blooms.len());
        let mut bytes = Vec::new();
        bytes.extend_from_slice(BLOOM_MAGIC_V2);
        bytes.extend_from_slice(&(line_blooms.len() as u32).to_le_bytes());
        for (line, exact) in line_blooms.iter().zip(exact_blooms) {
            // V2 had no way to say "nothing indexed here", so it spent an empty
            // filter. Reproducing that is what makes this a real downgrade.
            let empty = BloomFilter::with_capacity(1, 0.01);
            for bloom in [line, exact.as_ref().unwrap_or(&empty)] {
                let encoded = bloom.encode();
                bytes.extend_from_slice(&(encoded.len() as u32).to_le_bytes());
                bytes.extend_from_slice(&encoded);
            }
        }
        bytes
    }

    /// Replaces the bloom section of a part's index container, leaving the
    /// stream index alone, and repairs both checksums. Downgrade tests need to
    /// put an older bloom encoding on disk without disturbing anything else.
    fn replace_bloom_section(part: &Part, replacement: &[u8]) {
        let bytes = fs::read(part.index_path()).unwrap();
        let (_, streams) = split_index(&bytes).unwrap();
        let mut rebuilt = Vec::new();
        rebuilt.extend_from_slice(INDEX_MAGIC);
        for section in [replacement, streams] {
            rebuilt.extend_from_slice(&(section.len() as u32).to_le_bytes());
            rebuilt.extend_from_slice(section);
        }
        fs::write(part.index_path(), &rebuilt).unwrap();

        let meta_path = part.meta_path();
        let mut meta: MetaFile =
            serde_json::from_str(&fs::read_to_string(&meta_path).unwrap()).unwrap();
        meta.integrity.index_crc32 = crc32fast::hash(&rebuilt);
        meta.integrity.metadata_crc32 = 0;
        meta.integrity.metadata_crc32 = metadata_crc32(&meta).unwrap();
        fs::write(meta_path, serde_json::to_string_pretty(&meta).unwrap()).unwrap();
    }

    fn part_blooms(part: &Part) -> DecodedBlooms {
        let bytes = fs::read(part.index_path()).unwrap();
        let (bloom, _) = split_index(&bytes).unwrap();
        decode_blooms(bloom, part.meta.row_group_count as usize).unwrap()
    }

    fn rewrite_part_bloom_as_v1(part: &Part) {
        let decoded = part_blooms(part);
        replace_bloom_section(part, &encode_btf1(&decoded.line));
    }

    fn rewrite_part_bloom_as_v2(part: &Part) {
        let decoded = part_blooms(part);
        let legacy = encode_btf2(&decoded.line, decoded.exact_fields.as_ref().unwrap());
        replace_bloom_section(part, &legacy);
    }

    fn make_rows() -> Vec<Row> {
        let mut labels1: Labels = BTreeMap::new();
        labels1.insert("app".to_string(), "test".to_string());
        labels1.insert("host".to_string(), "h1".to_string());
        let mut labels2: Labels = BTreeMap::new();
        labels2.insert("app".to_string(), "other".to_string());
        vec![
            Row {
                tenant: test_tenant(),
                timestamp_ns: 1_700_000_000_000_000_000,
                labels: labels1.clone(),
                line: "error connecting to database".to_string(),
                structured_metadata: vec![],
            },
            Row {
                tenant: test_tenant(),
                timestamp_ns: 1_700_000_001_000_000_000,
                labels: labels1,
                line: "all good now".to_string(),
                structured_metadata: vec![("trace_id".to_string(), "abc".to_string())],
            },
            Row {
                tenant: test_tenant(),
                timestamp_ns: 1_700_000_002_000_000_000,
                labels: labels2,
                line: "other app log line".to_string(),
                structured_metadata: vec![],
            },
        ]
    }

    #[test]
    fn flush_then_query_roundtrip() {
        let tmp = tempfile_dir();
        let rows = make_rows();
        let parts = flush_rows(rows.clone(), &tmp, 2).expect("flush");
        assert_eq!(parts.len(), 1);
        let reader = PartReader::open(parts.into_iter().next().unwrap()).expect("open");

        // all
        let r = reader
            .query(&test_tenant(), &[], &[], crate::part::QueryTimeRange::closed(i64::MIN, i64::MAX), 100, true)
            .expect("q");
        let total: usize = r.iter().map(|s| s.entries.len()).sum();
        assert_eq!(total, 3);

        // label matcher app="test"
        let m = LabelMatcher::new("app".to_string(), MatcherOp::Eq, "test".to_string()).unwrap();
        let r = reader
            .query(&test_tenant(), std::slice::from_ref(&m), &[], crate::part::QueryTimeRange::closed(i64::MIN, i64::MAX), 100, true)
            .expect("q");
        let total: usize = r.iter().map(|s| s.entries.len()).sum();
        assert_eq!(total, 2);

        // line filter "error"
        let f = LineFilter::Contains("error".to_string());
        let r = reader
            .query(&test_tenant(), &[], &[f], crate::part::QueryTimeRange::closed(i64::MIN, i64::MAX), 100, true)
            .expect("q");
        let total: usize = r.iter().map(|s| s.entries.len()).sum();
        assert_eq!(total, 1);

        // time range
        let r = reader
            .query(&test_tenant(),

                &[],
                &[], crate::part::QueryTimeRange::closed(1_700_000_001_000_000_000, 1_700_000_003_000_000_000),
                100,
                true,
            )
            .expect("q");
        let total: usize = r.iter().map(|s| s.entries.len()).sum();
        assert_eq!(total, 2);
    }

    #[test]
    fn bloom_prunes_nonexistent_substring() {
        let tmp = tempfile_dir();
        let rows = make_rows();
        let parts = flush_rows(rows, &tmp, 100).expect("flush");
        let reader = PartReader::open(parts.into_iter().next().unwrap()).expect("open");
        let f = LineFilter::Contains("zzzzzz-not-present".to_string());
        assert!(
            reader
                .select_row_groups(&test_tenant(),

                    &[],
                    std::slice::from_ref(&f),
                    QueryTimeRange::closed(i64::MIN, i64::MAX),
                )
                .is_empty(),
            "bloom miss must avoid selecting the parquet row group"
        );
        let r = reader
            .query(&test_tenant(), &[], &[f], crate::part::QueryTimeRange::closed(i64::MIN, i64::MAX), 100, true)
            .expect("q");
        let total: usize = r.iter().map(|s| s.entries.len()).sum();
        assert_eq!(total, 0);
    }

    #[test]
    fn btf2_exact_field_bloom_prunes_structured_metadata_by_row_group() {
        let tmp = tempfile_dir();
        let mut rows = make_rows();
        rows[0].structured_metadata = vec![("trace_id".to_string(), "first".to_string())];
        rows[1].structured_metadata = vec![("trace_id".to_string(), "second".to_string())];
        let part = flush_rows(rows, &tmp, 1).unwrap().remove(0);
        let index_bytes = fs::read(part.index_path()).unwrap();
        assert_eq!(&index_bytes[..INDEX_MAGIC.len()], INDEX_MAGIC);
        assert_eq!(&split_index(&index_bytes).unwrap().0[..4], BLOOM_MAGIC_V4);
        let reader = PartReader::open(part).unwrap();

        let selected = reader.select_row_groups_with_exact_fields(&test_tenant(),

            &[],
            &[],
            &[ExactFieldPredicate::new("trace_id", "second")],
            QueryTimeRange::closed(i64::MIN, i64::MAX),
        );
        assert_eq!(selected, vec![1]);

        // Stream labels are pipeline fields too, but are not in the exact
        // field bloom. Their predicate must therefore remain conservative.
        let app = LabelMatcher::new("app".to_string(), MatcherOp::Eq, "test".to_string()).unwrap();
        assert_eq!(
            reader.select_row_groups_with_exact_fields(&test_tenant(),

                std::slice::from_ref(&app),
                &[],
                &[ExactFieldPredicate::new("app", "test")],
                QueryTimeRange::closed(i64::MIN, i64::MAX),
            ),
            vec![0, 1]
        );

        assert!(!reader.may_match_exact_fields(&test_tenant(),

            &[],
            &[],
            &[ExactFieldPredicate::new("trace_id", "not-present")], crate::part::QueryTimeRange::closed(i64::MIN, i64::MAX),
        ));
        assert!(reader.may_match_exact_fields(&test_tenant(),

            &[],
            &[],
            &[ExactFieldPredicate::new("missing", "")], crate::part::QueryTimeRange::closed(i64::MIN, i64::MAX),
        ));
    }

    /// A row group with nothing to index spends no exact-field filter.
    ///
    /// `optimal_bits` has a 1024-bit floor, so the empty filter V3 stored cost
    /// 140 bytes whether or not there was a token to put in it, and
    /// tenant-aligned row groups charge that per tenant per part. Absence has
    /// to keep pruning exactly as the empty filter did, which is the second
    /// half of this test.
    #[test]
    fn a_row_group_with_no_exact_field_token_stores_no_filter() {
        let tmp = tempfile_dir();
        let mut plain = make_rows();
        for row in &mut plain {
            row.line = "nothing here parses as a field".to_string();
            row.structured_metadata = vec![];
        }
        let plain_part = flush_rows(plain.clone(), &tmp, 1).unwrap().remove(0);
        let plain_bytes = fs::metadata(plain_part.index_path()).unwrap().len();

        let mut indexed = plain;
        for row in &mut indexed {
            row.structured_metadata = vec![("trace_id".to_string(), "abc".to_string())];
        }
        let indexed_part = flush_rows(indexed, &tempfile_dir(), 1).unwrap().remove(0);
        let indexed_bytes = fs::metadata(indexed_part.index_path()).unwrap().len();

        let row_groups = plain_part.meta.row_group_count as u64;
        assert!(row_groups >= 2, "need more than one row group to see a floor");
        assert!(
            indexed_bytes >= plain_bytes + 140 * row_groups,
            "an unused filter used to cost the 1024-bit floor per row group: \
{indexed_bytes} vs {plain_bytes} over {row_groups} row groups"
        );

        // Absence prunes. A predicate on a field the row group never indexed
        // cannot match it, which is the same answer the all-zero filter gave.
        let reader = PartReader::open(plain_part).unwrap();
        assert!(
            reader
                .select_row_groups_with_exact_fields(
                    &test_tenant(),
                    &[],
                    &[],
                    &[ExactFieldPredicate::new("trace_id", "abc")],
                    QueryTimeRange::closed(i64::MIN, i64::MAX),
                )
                .is_empty(),
            "no token indexed means no exact-field predicate can match"
        );
        // The conservative cases still are: a stream label is not in this
        // filter at all, and an empty value stands for field absence.
        let app = LabelMatcher::new("app".to_string(), MatcherOp::Eq, "test".to_string()).unwrap();
        assert!(!reader
            .select_row_groups_with_exact_fields(
                &test_tenant(),
                std::slice::from_ref(&app),
                &[],
                &[ExactFieldPredicate::new("app", "test")],
                QueryTimeRange::closed(i64::MIN, i64::MAX),
            )
            .is_empty());
        assert!(reader.may_match_exact_fields(
            &test_tenant(),
            &[],
            &[],
            &[ExactFieldPredicate::new("trace_id", "")], crate::part::QueryTimeRange::closed(i64::MIN, i64::MAX),
        ));
    }

    #[test]
    fn btf2_exact_field_bloom_indexes_parser_scalars_without_raw_substring_assumptions() {
        let tmp = tempfile_dir();
        let mut rows = make_rows();
        rows[0].line = r#"{"user":"\u0061lice","namespace:key":"value"}"#.to_string();
        rows[1].line = r#"user=bob elapsed=250ms"#.to_string();
        let part = flush_rows(rows, &tmp, 1).unwrap().remove(0);
        let reader = PartReader::open(part).unwrap();
        let time_range = QueryTimeRange::closed(i64::MIN, i64::MAX);

        assert_eq!(
            reader.select_row_groups_with_exact_fields(&test_tenant(),

                &[],
                &[],
                &[ExactFieldPredicate::new_with_extraction(
                    "user", "alice", true,
                )],
                time_range,
            ),
            vec![0]
        );
        assert_eq!(
            reader.select_row_groups_with_exact_fields(&test_tenant(),

                &[],
                &[],
                &[ExactFieldPredicate::new_with_extraction(
                    "user", "bob", true,
                )],
                time_range,
            ),
            vec![1]
        );
        assert_eq!(
            reader.select_row_groups_with_exact_fields(&test_tenant(),

                &[],
                &[],
                &[ExactFieldPredicate::new_with_extraction(
                    "namespace_key",
                    "value",
                    true,
                )],
                time_range,
            ),
            vec![0]
        );
    }

    #[test]
    fn btf2_exact_field_bloom_indexes_canonical_numeric_and_duration_values() {
        let tmp = tempfile_dir();
        let rows = vec![
            Row {
                tenant: test_tenant(),
                timestamp_ns: 1,
                labels: BTreeMap::new(),
                line: r#"{"value":9007199254740992,"elapsed":"1s"}"#.to_string(),
                structured_metadata: vec![],
            },
            Row {
                tenant: test_tenant(),
                timestamp_ns: 2,
                labels: BTreeMap::new(),
                line: r#"{"value":9007199254740993,"elapsed":"1000ms"}"#.to_string(),
                structured_metadata: vec![],
            },
        ];
        let part = flush_rows(rows, &tmp, 1).unwrap().remove(0);
        let reader = PartReader::open(part).unwrap();
        let range = QueryTimeRange::closed(i64::MIN, i64::MAX);

        let numeric = crate::logql::parse("{} | json | value=9007199254740993").unwrap();
        assert_eq!(
            reader.select_row_groups_with_exact_fields(&test_tenant(),

                &[],
                &[],
                &numeric.exact_field_predicates(),
                range,
            ),
            vec![1]
        );

        let duration = crate::logql::parse("{} | json | elapsed=1s").unwrap();
        assert_eq!(
            reader.select_row_groups_with_exact_fields(&test_tenant(),

                &[],
                &[],
                &duration.exact_field_predicates(),
                range,
            ),
            vec![0, 1]
        );
    }

    #[test]
    fn btf2_indexes_scan_typed_equality_conservatively() {
        let tmp = tempfile_dir();
        let rows = vec![
            Row {
                tenant: test_tenant(),
                timestamp_ns: 1,
                labels: BTreeMap::new(),
                line: r#"{"value":500.0}"#.to_string(),
                structured_metadata: vec![],
            },
            Row {
                tenant: test_tenant(),
                timestamp_ns: 2,
                labels: BTreeMap::new(),
                line: r#"{"value":999}"#.to_string(),
                structured_metadata: vec![],
            },
        ];
        let part = flush_rows(rows, &tmp, 1).unwrap().remove(0);
        rewrite_part_bloom_as_v2(&part);
        let reader = PartReader::open(load_part(&part.dir).unwrap()).unwrap();
        let query = crate::logql::parse("{} | json | value=500").unwrap();
        let selected = reader.select_row_groups_with_exact_fields(&test_tenant(),

            &[],
            &[],
            &query.exact_field_predicates(),
            QueryTimeRange::closed(i64::MIN, i64::MAX),
        );
        assert_eq!(selected, vec![0, 1]);
    }

    #[test]
    fn btf1_part_loads_and_exact_fields_fall_back_to_scanning() {
        let tmp = tempfile_dir();
        let part = flush_rows(make_rows(), &tmp, 1).unwrap().remove(0);
        rewrite_part_bloom_as_v1(&part);

        let legacy_part = load_part(&part.dir).unwrap();
        let reader = PartReader::open(legacy_part).unwrap();
        assert!(reader.exact_field_bloom.is_none());
        assert!(reader.may_match_exact_fields(&test_tenant(),

            &[],
            &[],
            &[ExactFieldPredicate::new("trace_id", "not-present")], crate::part::QueryTimeRange::closed(i64::MIN, i64::MAX),
        ));
    }

    #[test]
    fn bloom_container_versions_reject_invalid_framing() {
        let bloom = BloomFilter::with_capacity(1, 0.01);
        let legacy = encode_btf1(&[bloom]);
        let decoded = decode_blooms(&legacy, 1).unwrap();
        assert_eq!(decoded.line.len(), 1);
        assert!(decoded.exact_fields.is_none());
        assert!(
            decode_blooms(&legacy, 2)
                .err()
                .unwrap()
                .contains("row group count mismatch")
        );

        let mut trailing = legacy.clone();
        trailing.push(0);
        assert!(
            decode_blooms(&trailing, 1)
                .err()
                .unwrap()
                .contains("trailing bytes")
        );

        let mut unknown = legacy.clone();
        unknown[..4].copy_from_slice(b"BTF9");
        assert!(
            decode_blooms(&unknown, 1)
                .err()
                .unwrap()
                .contains("magic mismatch")
        );

        let mut truncated_v2 = Vec::new();
        truncated_v2.extend_from_slice(BLOOM_MAGIC_V2);
        truncated_v2.extend_from_slice(&1u32.to_le_bytes());
        truncated_v2.extend_from_slice(&legacy[8..]);
        assert!(
            decode_blooms(&truncated_v2, 1)
                .err()
                .unwrap()
                .contains("length truncated")
        );
    }

    #[test]
    fn forward_limit_stops_physical_part_scan() {
        let tmp = tempfile_dir();
        let rows: Vec<Row> = (0..20)
            .map(|timestamp_ns| Row {
                tenant: test_tenant(),
                timestamp_ns,
                labels: BTreeMap::new(),
                line: format!("line-{timestamp_ns}"),
                structured_metadata: vec![],
            })
            .collect();
        let part = flush_rows(rows, &tmp, 20).unwrap().remove(0);
        let reader = PartReader::open(part).unwrap();
        let result = reader
            .query_with_exact_field_pruning_and_scan_limit(&test_tenant(),

                &[],
                ExactFieldPruning::new(&[], &[]), crate::part::QueryTimeRange::closed(0, 19),
                1,
                true,
                Some(100),
                None,
            )
            .unwrap();
        assert_eq!(result.scanned_rows, 1);
        assert_eq!(result.results[0].entries[0].timestamp_ns, 0);
    }

    #[test]
    fn scan_limit_stops_before_collecting_the_rest_of_a_part() {
        let tmp = tempfile_dir();
        let rows: Vec<Row> = (0..20)
            .map(|timestamp_ns| Row {
                tenant: test_tenant(),
                timestamp_ns,
                labels: BTreeMap::new(),
                line: format!("line-{timestamp_ns}"),
                structured_metadata: vec![],
            })
            .collect();
        let part = flush_rows(rows, &tmp, 20).unwrap().remove(0);
        let reader = PartReader::open(part).unwrap();
        let result = reader
            .query_with_exact_field_pruning_and_scan_limit(&test_tenant(),

                &[],
                ExactFieldPruning::new(&[], &[]), crate::part::QueryTimeRange::closed(0, 19),
                usize::MAX,
                true,
                Some(3),
                None,
            )
            .unwrap();
        assert_eq!(result.scanned_rows, 3);
        assert_eq!(result.results[0].entries.len(), 3);
    }

    /// One row per row group, so the row-group bound and the row bound are both
    /// exercised on the same boundary. A pruning bound tighter than the row
    /// bound drops rows silently, so the two are asserted against each other
    /// rather than each on its own.
    #[test]
    fn a_row_on_end_belongs_to_a_closed_window_and_not_to_a_half_open_one() {
        let tmp = tempfile_dir();
        let rows: Vec<Row> = [100, 200, 300]
            .into_iter()
            .map(|timestamp_ns| Row {
                tenant: test_tenant(),
                timestamp_ns,
                labels: BTreeMap::new(),
                line: format!("line-{timestamp_ns}"),
                structured_metadata: vec![],
            })
            .collect();
        let reader = PartReader::open(flush_rows(rows, &tmp, 1).unwrap().remove(0)).unwrap();

        let timestamps = |range| {
            let mut seen: Vec<i64> = reader
                .query(&test_tenant(), &[], &[], range, 100, true)
                .expect("query")
                .iter()
                .flat_map(|stream| stream.entries.iter().map(|entry| entry.timestamp_ns))
                .collect();
            seen.sort_unstable();
            seen
        };
        let selected = |range| reader.select_row_groups(&test_tenant(), &[], &[], range);

        assert_eq!(timestamps(QueryTimeRange::closed(100, 200)), vec![100, 200]);
        assert_eq!(timestamps(QueryTimeRange::half_open(100, 200)), vec![100]);
        assert_eq!(timestamps(QueryTimeRange::half_open(100, 201)), vec![
            100, 200
        ]);
        assert_eq!(
            timestamps(QueryTimeRange::half_open(200, 200)),
            Vec::<i64>::new(),
            "an empty window returns nothing, not the row on its boundary"
        );

        // Row groups here are one row each, so the group a boundary row lives in
        // must be selected exactly when that row is returned.
        assert_eq!(selected(QueryTimeRange::closed(100, 200)).len(), 2);
        assert_eq!(selected(QueryTimeRange::half_open(100, 200)).len(), 1);
        for range in [
            QueryTimeRange::closed(100, 200),
            QueryTimeRange::half_open(100, 200),
            QueryTimeRange::half_open(100, 201),
            QueryTimeRange::half_open(200, 200),
        ] {
            assert_eq!(
                selected(range).len(),
                timestamps(range).len(),
                "row-group pruning and the row-level test must agree"
            );
        }

        assert!(
            reader.may_match_exact_fields(
                &test_tenant(),
                &[],
                &[],
                &[],
                QueryTimeRange::closed(200, 200)
            )
        );
        assert!(
            !reader.may_match_exact_fields(
                &test_tenant(),
                &[],
                &[],
                &[],
                QueryTimeRange::half_open(200, 200)
            ),
            "an empty window cannot need a part restored for it"
        );
    }

    #[test]
    fn label_index_prunes_wrong_app() {
        let tmp = tempfile_dir();
        let rows = make_rows();
        let parts = flush_rows(rows, &tmp, 100).expect("flush");
        let reader = PartReader::open(parts.into_iter().next().unwrap()).expect("open");
        let m = LabelMatcher::new("app".to_string(), MatcherOp::Eq, "missing".to_string()).unwrap();
        assert!(
            reader
                .select_row_groups(&test_tenant(),

                    std::slice::from_ref(&m),
                    &[],
                    QueryTimeRange::closed(i64::MIN, i64::MAX),
                )
                .is_empty(),
            "stream-index miss must avoid selecting the parquet row group"
        );
        let r = reader
            .query(&test_tenant(), &[m], &[], crate::part::QueryTimeRange::closed(i64::MIN, i64::MAX), 100, true)
            .expect("q");
        let total: usize = r.iter().map(|s| s.entries.len()).sum();
        assert_eq!(total, 0);
    }

    #[test]
    fn discover_parts_after_flush() {
        let tmp = tempfile_dir();
        let rows = make_rows();
        let _ = flush_rows(rows, &tmp, 100).expect("flush");
        let parts = discover_parts(&tmp).unwrap();
        assert_eq!(parts.len(), 1);
    }

    #[test]
    fn backward_limit_returns_most_recent() {
        let tmp = tempfile_dir();
        let mut labels: Labels = BTreeMap::new();
        labels.insert("app".to_string(), "test".to_string());
        let rows: Vec<Row> = (0..3_000)
            .map(|i| Row {
                tenant: test_tenant(),
                timestamp_ns: 1_700_000_000_000_000_000 + i * 1_000_000_000,
                labels: labels.clone(),
                line: format!("line-{i:04}"),
                structured_metadata: vec![],
            })
            .collect();
        let parts = flush_rows(rows, &tmp, 3_000).expect("flush");
        let reader = PartReader::open(parts.into_iter().next().unwrap()).expect("open");

        let r = reader
            .query(&test_tenant(), &[], &[], crate::part::QueryTimeRange::closed(i64::MIN, i64::MAX), 3, false)
            .expect("q");
        let lines: Vec<&str> = r
            .iter()
            .flat_map(|s| s.entries.iter().map(|e| e.line.as_str()))
            .collect();
        assert_eq!(lines, vec!["line-2999", "line-2998", "line-2997"]);

        let r = reader
            .query(&test_tenant(), &[], &[], crate::part::QueryTimeRange::closed(i64::MIN, i64::MAX), 3, true)
            .expect("q");
        let lines: Vec<&str> = r
            .iter()
            .flat_map(|s| s.entries.iter().map(|e| e.line.as_str()))
            .collect();
        assert_eq!(lines, vec!["line-0000", "line-0001", "line-0002"]);
    }

    #[test]
    fn series_returns_actual_label_sets() {
        let tmp = tempfile_dir();
        let rows = make_rows();
        let parts = flush_rows(rows, &tmp, 100).expect("flush");
        let reader = PartReader::open(parts.into_iter().next().unwrap()).expect("open");

        let all = reader.series(&test_tenant(), &[]);
        assert_eq!(all.len(), 2);
        let app_test: Vec<&Labels> = all
            .iter()
            .filter(|l| l.get("app").map(|v| v.as_str()) == Some("test"))
            .collect();
        assert_eq!(app_test.len(), 1);
        assert_eq!(app_test[0].get("host").map(|s| s.as_str()), Some("h1"));

        let m = LabelMatcher::new("app".to_string(), MatcherOp::Eq, "other".to_string()).unwrap();
        let r = reader.series(&test_tenant(), &[m]);
        assert_eq!(r.len(), 1);
        assert!(r[0].get("app").map(|v| v.as_str()) == Some("other"));
        assert!(!r[0].contains_key("host"));
    }

    #[test]
    fn concurrent_queries_on_same_part_no_race() {
        let tmp = tempfile_dir();
        let mut labels: Labels = BTreeMap::new();
        labels.insert("app".to_string(), "concurrent".to_string());
        let rows: Vec<Row> = (0..50)
            .map(|i| Row {
                tenant: test_tenant(),
                timestamp_ns: 1_700_000_000_000_000_000 + i * 1_000_000_000,
                labels: labels.clone(),
                line: format!("concurrent-line-{:02}", i),
                structured_metadata: vec![],
            })
            .collect();
        let parts = flush_rows(rows, &tmp, 8).expect("flush");
        let reader = Arc::new(PartReader::open(parts.into_iter().next().unwrap()).expect("open"));

        let matcher =
            LabelMatcher::new("app".to_string(), MatcherOp::Eq, "concurrent".to_string()).unwrap();

        let num_threads = 16;
        let queries_per_thread = 50;
        let mut handles = Vec::with_capacity(num_threads);
        for thread_index in 0..num_threads {
            let reader = reader.clone();
            let matcher = matcher.clone();
            handles.push(std::thread::spawn(move || {
                let mut errors = 0u32;
                let mut wrong = 0u32;
                for q in 0..queries_per_thread {
                    let forward = (thread_index + q) % 2 == 0;
                    let limit = 3 + (q % 5);
                    let result = reader
                        .query(&test_tenant(),

                            std::slice::from_ref(&matcher),
                            &[], crate::part::QueryTimeRange::closed(i64::MIN, i64::MAX),
                            limit,
                            forward,
                        )
                        .expect("query must not error");
                    let total: usize = result.iter().map(|s| s.entries.len()).sum();
                    if total == 0 {
                        errors += 1;
                    } else if total > limit {
                        wrong += 1;
                    }
                    if forward {
                        let first = result
                            .iter()
                            .flat_map(|s| s.entries.iter())
                            .map(|e| e.timestamp_ns)
                            .next()
                            .unwrap_or(0);
                        let expected_first = 1_700_000_000_000_000_000;
                        if first != expected_first {
                            wrong += 1;
                        }
                    } else {
                        let first = result
                            .iter()
                            .flat_map(|s| s.entries.iter())
                            .map(|e| e.timestamp_ns)
                            .next()
                            .unwrap_or(0);
                        let expected_first = 1_700_000_000_000_000_000 + 49 * 1_000_000_000;
                        if first != expected_first {
                            wrong += 1;
                        }
                    }
                }
                (errors, wrong)
            }));
        }
        let mut total_errors = 0u32;
        let mut total_wrong = 0u32;
        for h in handles {
            let (e, w) = h.join().expect("thread");
            total_errors += e;
            total_wrong += w;
        }
        assert_eq!(
            total_errors, 0,
            "some queries returned empty (race-induced decode failure)"
        );
        assert_eq!(
            total_wrong, 0,
            "some queries returned wrong ordering or count (race-induced corruption)"
        );
    }

    fn tempfile_dir() -> PathBuf {
        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let c = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let mut p = std::env::temp_dir();
        p.push(format!(
            "loggytracy-test-{}-{}-{}",
            std::process::id(),
            c,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&p).unwrap();
        p
    }

    #[cfg(unix)]
    #[test]
    fn cleanup_tmp_rejects_symlinked_root_and_tmp_directory() {
        use std::os::unix::fs::symlink;

        let outside = tempfile_dir();
        let outside_tmp = outside.join(".tmp");
        fs::create_dir_all(&outside_tmp).unwrap();
        let sentinel = outside_tmp.join("sentinel");
        fs::write(&sentinel, b"must survive").unwrap();

        let link_parent = tempfile_dir();
        let linked_root = link_parent.join("parts");
        symlink(&outside, &linked_root).unwrap();
        let error = cleanup_tmp(&linked_root).unwrap_err();
        assert!(error.contains("unsafe parts root"));
        assert!(sentinel.exists());

        let normal_root = tempfile_dir();
        let linked_tmp = normal_root.join(".tmp");
        symlink(&outside_tmp, &linked_tmp).unwrap();
        let error = cleanup_tmp(&normal_root).unwrap_err();
        assert!(error.contains("unsafe tmp directory"));
        assert!(sentinel.exists());
    }

    #[test]
    fn label_eq_empty_matches_missing_label_in_part() {
        // {app=""} matches a missing label. The memtable path already behaves this way,
        // so the part path must conservatively allow it for consistency.
        let tmp = tempfile_dir();
        let mut labels: Labels = BTreeMap::new();
        labels.insert("host".to_string(), "h1".to_string());
        // The app label is missing.
        let rows: Vec<Row> = vec![Row {
            tenant: test_tenant(),
            timestamp_ns: 1_700_000_000_000_000_000,
            labels,
            line: "no app label here".to_string(),
            structured_metadata: vec![],
        }];
        let parts = flush_rows(rows, &tmp, 100).expect("flush");
        let reader = PartReader::open(parts.into_iter().next().unwrap()).expect("open");
        let m = LabelMatcher::new("app".to_string(), MatcherOp::Eq, "".to_string()).unwrap();
        let r = reader
            .query(&test_tenant(), &[m], &[], crate::part::QueryTimeRange::closed(i64::MIN, i64::MAX), 100, true)
            .expect("q");
        let total: usize = r.iter().map(|s| s.entries.len()).sum();
        assert_eq!(
            total, 1,
            "{{app=\"\"}} should match streams without an app label in part"
        );
    }

    #[test]
    fn load_rejects_metadata_checksum_mismatch() {
        let tmp = tempfile_dir();
        let part = flush_rows(make_rows(), &tmp, 100).expect("flush").remove(0);
        let meta_path = part.meta_path();
        let mut meta: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(&meta_path).unwrap()).unwrap();
        meta["stream_labels"]
            .as_array_mut()
            .unwrap()
            .push(serde_json::Value::String("ghost_label".to_string()));
        fs::write(&meta_path, serde_json::to_vec(&meta).unwrap()).unwrap();

        assert!(load_part(&part.dir).is_err());
    }

    /// A metadata layout this build does not know has to say so. Reporting it
    /// as a checksum mismatch would read as corruption and send an operator
    /// looking for a failing disk instead of a version skew.
    #[test]
    fn load_reports_an_unknown_metadata_version_as_such() {
        let tmp = tempfile_dir();
        let part = flush_rows(make_rows(), &tmp, 100).expect("flush").remove(0);
        let meta_path = part.meta_path();
        let mut meta: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(&meta_path).unwrap()).unwrap();
        meta["version"] = serde_json::Value::from(PART_META_VERSION + 1);
        fs::write(&meta_path, serde_json::to_vec(&meta).unwrap()).unwrap();

        let error = load_part(&part.dir).expect_err("a future version must be refused");
        assert!(error.contains("unsupported part metadata version"), "{error}");

        // A part written before the field existed reads as version 0, which is
        // the pre-versioning marker rather than a checksum problem.
        let mut meta: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(&meta_path).unwrap()).unwrap();
        meta.as_object_mut().unwrap().remove("version");
        fs::write(&meta_path, serde_json::to_vec(&meta).unwrap()).unwrap();

        let error = load_part(&part.dir).expect_err("an unversioned part must be refused");
        assert!(error.contains("version 0"), "{error}");
    }

    /// What tenant breadth costs a part.
    ///
    /// Row groups are cut at tenant boundaries, so the number of tenants in a
    /// flush is a *lower bound* on the number of row groups — regardless of
    /// `row_group_size`. Parquet carries per-column metadata for every row
    /// group and this engine carries a bloom filter for every row group, so
    /// that bound is a real cost, and the target workload is many small
    /// tenants. This measures it rather than assuming it.
    ///
    /// The unit is a (tenant, part) pair, not a row: the fixed cost below is
    /// spent per tenant per part it appears in, so it is largest in ratio for
    /// the tenants with the least data. `index_resident_bytes` is called out
    /// separately because the local cache budget does not cover it — that part
    /// stays in memory for as long as the part is open.
    #[test]
    fn tenant_breadth_sets_the_row_group_floor_and_what_that_costs() {
        fn build(tenants: usize, rows_total: usize) -> (u32, u64, u64, u64, u64, u64) {
            let tmp = tempfile_dir();
            let rows: Vec<Row> = (0..rows_total)
                .map(|index| Row {
                    tenant: TenantId::parse(&format!("t{:04}", index % tenants)).unwrap(),
                    timestamp_ns: 1_700_000_000_000_000_000 + index as i64,
                    labels: [("app".to_string(), "fragmentation".to_string())]
                        .into_iter()
                        .collect(),
                    line: format!("row {index} with enough text to compress like a log line"),
                    structured_metadata: vec![],
                })
                .collect();
            let part = flush_rows(rows, &tmp, 8192).expect("flush").remove(0);
            let on_disk = fs::metadata(part.data_path()).unwrap().len();
            let bloom = fs::metadata(part.index_path()).unwrap().len();
            let reader = PartReader::open(part).expect("open");
            (
                reader.meta().row_group_count,
                on_disk,
                bloom,
                reader.meta().meta_bytes,
                reader.index_resident_bytes(),
                reader.meta().materialized_bytes,
            )
        }

        let rows_total = 5_000;
        let (one_groups, one_bytes, one_bloom, one_meta, one_resident, one_materialized) =
            build(1, rows_total);
        let (many_groups, many_bytes, many_bloom, many_meta, many_resident, many_materialized) =
            build(500, rows_total);

        // Same rows, same `row_group_size`; only the tenant breadth differs.
        assert_eq!(one_groups, 1, "one tenant fits one row group");
        assert_eq!(
            many_groups, 500,
            "500 tenants force 500 row groups even though 8192 rows would fit in one"
        );
        assert_eq!(
            one_materialized, many_materialized,
            "the rows themselves are identical in size"
        );

        // The cost is entirely structural. Printed rather than asserted at a
        // threshold: the ratio depends on the parquet writer and the data, and
        // a hard bound here would be a test of zstd, not of this engine.
        println!(
            "tenant fragmentation: 1 tenant = {one_bytes} B / {one_groups} row groups, \
500 tenants = {many_bytes} B / {many_groups} row groups, \
ratio {:.2}x",
            many_bytes as f64 / one_bytes as f64
        );
        println!(
            "  per (tenant, part) pair: parquet {:.0} B, bloom {:.0} B, meta.json {:.0} B, \
resident {:.0} B",
            (many_bytes - one_bytes) as f64 / many_groups as f64,
            (many_bloom - one_bloom) as f64 / many_groups as f64,
            (many_meta - one_meta) as f64 / many_groups as f64,
            (many_resident - one_resident) as f64 / many_groups as f64,
        );
        assert!(
            many_bytes > one_bytes,
            "fragmentation cannot make the file smaller"
        );
        assert!(
            many_resident > one_resident,
            "every extra row group carries its own filters into memory"
        );
    }

    #[test]
    fn bloom_handles_large_trigram_volume_in_single_row_group() {
        // An 8192-row group with dozens of trigrams per line produces several to dozens of times
        // as many unique items as rows. The old implementation (capacity = row count) reached
        // a fill ratio near 99% and produced false positives, while the new implementation
        // (capacity = unique trigram count) accurately prunes absent substrings.
        let tmp = tempfile_dir();
        let mut labels: Labels = BTreeMap::new();
        labels.insert("app".to_string(), "test".to_string());
        let rows: Vec<Row> = (0..8192usize)
            .map(|i| Row {
                tenant: test_tenant(),
                timestamp_ns: 1_700_000_000_000_000_000 + (i as i64) * 1_000_000,
                labels: labels.clone(),
                line: format!(
                    "log line index {} some random words here for trigrams unique fragment {}",
                    i, i
                ),
                structured_metadata: vec![],
            })
            .collect();
        let parts = flush_rows(rows, &tmp, 8192).expect("flush");
        let reader = PartReader::open(parts.into_iter().next().unwrap()).expect("open");
        let f = LineFilter::Contains("zzzzzz-not-present-substr".to_string());
        let r = reader
            .query(&test_tenant(), &[], &[f], crate::part::QueryTimeRange::closed(i64::MIN, i64::MAX), 100, true)
            .expect("q");
        let total: usize = r.iter().map(|s| s.entries.len()).sum();
        assert_eq!(
            total, 0,
            "bloom should prune nonexistent substring even with 8192-row group"
        );
    }

    #[test]
    fn merge_flush_renames_part_with_tombstone_already_present() {
        let tmp = tempfile_dir();
        let parts_root = tmp.join("parts");
        std::fs::create_dir_all(&parts_root).unwrap();
        let old = flush_rows(
            vec![Row {
                tenant: test_tenant(),
                timestamp_ns: 1_700_000_000_000_000_000,
                labels: BTreeMap::new(),
                line: "old".to_string(),
                structured_metadata: vec![],
            }],
            &parts_root,
            100,
        )
        .unwrap()
        .remove(0);

        let merged = flush_rows_with_merge_tombstone(
            vec![Row {
                tenant: test_tenant(),
                timestamp_ns: 1_700_000_000_000_000_000,
                labels: BTreeMap::new(),
                line: "merged".to_string(),
                structured_metadata: vec![],
            }],
            &parts_root,
            100,
            std::slice::from_ref(&old.dir),
        )
        .unwrap();
        let new_dir = &merged[0].dir;
        assert!(new_dir.join(MERGE_TOMBSTONE_FILE).exists());
        assert_eq!(
            read_merge_tombstone(new_dir).unwrap(),
            vec![old.dir.strip_prefix(&parts_root).unwrap().to_path_buf()]
        );
    }

    #[test]
    fn discover_keeps_old_parts_when_tombstoned_replacement_is_corrupt() {
        let tmp = tempfile_dir();
        let parts_root = tmp.join("parts");
        std::fs::create_dir_all(&parts_root).unwrap();
        let old = flush_rows(
            vec![Row {
                tenant: test_tenant(),
                timestamp_ns: 1_700_000_000_000_000_000,
                labels: BTreeMap::new(),
                line: "old".to_string(),
                structured_metadata: vec![],
            }],
            &parts_root,
            100,
        )
        .unwrap()
        .remove(0);
        let merged = flush_rows_with_merge_tombstone(
            vec![Row {
                tenant: test_tenant(),
                timestamp_ns: 1_700_000_000_000_000_000,
                labels: BTreeMap::new(),
                line: "merged".to_string(),
                structured_metadata: vec![],
            }],
            &parts_root,
            100,
            std::slice::from_ref(&old.dir),
        )
        .unwrap();
        let new_dir = &merged[0].dir;
        std::fs::write(new_dir.join(INDEX_FILE), b"corrupt").unwrap();

        let discovered = discover_parts(&parts_root).unwrap();
        assert_eq!(discovered.len(), 1);
        assert_eq!(discovered[0].dir, old.dir);
        assert!(old.dir.exists());
        assert!(new_dir.join(MERGE_TOMBSTONE_FILE).exists());
    }

    #[test]
    fn merge_tombstone_cleanup_during_discover() {
        // If a crash occurs after renaming the new part and recording the tombstone in merge,
        // but before deleting old_dirs, verify that on restart discover_parts finds the tombstone,
        // cleans up old_dirs, and loads only the one new part.
        let tmp = tempfile_dir();
        let parts_root = tmp.join("parts");
        std::fs::create_dir_all(&parts_root).unwrap();

        let mut l1: Labels = BTreeMap::new();
        l1.insert("app".to_string(), "old1".to_string());
        let mut l2: Labels = BTreeMap::new();
        l2.insert("app".to_string(), "old2".to_string());

        let parts1 = flush_rows(
            vec![Row {
                tenant: test_tenant(),
                timestamp_ns: 1_700_000_000_000_000_000,
                labels: l1,
                line: "old1 line".to_string(),
                structured_metadata: vec![],
            }],
            &parts_root,
            100,
        )
        .expect("flush1");
        let parts2 = flush_rows(
            vec![Row {
                tenant: test_tenant(),
                timestamp_ns: 1_700_000_002_000_000_000,
                labels: l2,
                line: "old2 line".to_string(),
                structured_metadata: vec![],
            }],
            &parts_root,
            100,
        )
        .expect("flush2");

        let old_dirs: Vec<PathBuf> = parts1
            .iter()
            .chain(parts2.iter())
            .map(|p| p.dir.clone())
            .collect();

        // Simulated merge: collect rows from two streams and create a new part.
        let mut l3: Labels = BTreeMap::new();
        l3.insert("app".to_string(), "old1".to_string());
        let mut l4: Labels = BTreeMap::new();
        l4.insert("app".to_string(), "old2".to_string());
        let merged_rows = vec![
            Row {
                tenant: test_tenant(),
                timestamp_ns: 1_700_000_000_000_000_000,
                labels: l3,
                line: "old1 line".to_string(),
                structured_metadata: vec![],
            },
            Row {
                tenant: test_tenant(),
                timestamp_ns: 1_700_000_002_000_000_000,
                labels: l4,
                line: "old2 line".to_string(),
                structured_metadata: vec![],
            },
        ];
        let merged_parts = flush_rows(merged_rows, &parts_root, 100).expect("flush merged");

        let new_dir = merged_parts[0].dir.clone();
        write_merge_tombstone(&new_dir, &parts_root, &old_dirs).expect("tombstone write");

        // Simulate a crash: old_dirs remain, and the tombstone is in the new part directory.
        for old_dir in &old_dirs {
            assert!(
                old_dir.exists(),
                "old_dirs should still exist before discover"
            );
        }
        assert!(new_dir.join(MERGE_TOMBSTONE_FILE).exists());

        let discovered = discover_parts(&parts_root).unwrap();
        assert_eq!(
            discovered.len(),
            1,
            "tombstone cleanup should leave only the new part"
        );

        for old_dir in &old_dirs {
            assert!(
                !old_dir.exists(),
                "old part dir {} should be removed by tombstone cleanup",
                old_dir.display()
            );
        }
        assert!(
            !new_dir.join(MERGE_TOMBSTONE_FILE).exists(),
            "tombstone file should be removed during discover"
        );
    }

    #[test]
    fn discover_cleans_transitive_merge_tombstone_chain() {
        let tmp = tempfile_dir();
        let parts_root = tmp.join("parts");
        std::fs::create_dir_all(&parts_root).unwrap();
        let row = |line: &str| Row {
            tenant: test_tenant(),
            timestamp_ns: 1_700_000_000_000_000_000,
            labels: BTreeMap::new(),
            line: line.to_string(),
            structured_metadata: vec![],
        };

        let oldest = flush_rows(vec![row("oldest")], &parts_root, 100)
            .unwrap()
            .remove(0);
        let middle = flush_rows_with_merge_tombstone(
            vec![row("middle")],
            &parts_root,
            100,
            std::slice::from_ref(&oldest.dir),
        )
        .unwrap()
        .remove(0);
        let newest = flush_rows_with_merge_tombstone(
            vec![row("newest")],
            &parts_root,
            100,
            std::slice::from_ref(&middle.dir),
        )
        .unwrap()
        .remove(0);

        let discovered = discover_parts(&parts_root).unwrap();

        assert_eq!(discovered.len(), 1);
        assert_eq!(discovered[0].meta.id, newest.meta.id);
        assert!(!oldest.dir.exists());
        assert!(!middle.dir.exists());
        assert!(newest.dir.exists());
        assert!(!newest.dir.join(MERGE_TOMBSTONE_FILE).exists());
    }

    #[test]
    fn discover_rejects_tombstone_paths_outside_parts_root() {
        let tmp = tempfile_dir();
        let parts_root = tmp.join("parts");
        std::fs::create_dir_all(&parts_root).unwrap();
        let outside = tmp.join("outside");
        std::fs::create_dir_all(&outside).unwrap();
        std::fs::write(outside.join("keep"), b"data").unwrap();
        let replacement = flush_rows(
            vec![Row {
                tenant: test_tenant(),
                timestamp_ns: 1_700_000_000_000_000_000,
                labels: BTreeMap::new(),
                line: "replacement".to_string(),
                structured_metadata: vec![],
            }],
            &parts_root,
            100,
        )
        .unwrap()
        .remove(0);
        std::fs::write(
            replacement.dir.join(MERGE_TOMBSTONE_FILE),
            r#"{"old_dirs":["../../outside"]}"#,
        )
        .unwrap();

        let discovered = discover_parts(&parts_root).unwrap();

        assert!(discovered.is_empty());
        assert!(outside.join("keep").exists());
        assert!(replacement.dir.join(MERGE_TOMBSTONE_FILE).exists());
    }

    #[test]
    fn tombstone_path_validation_rejects_absolute_and_parent_paths() {
        assert!(validate_tombstone_part_path(Path::new("/tmp/part")).is_err());
        assert!(validate_tombstone_part_path(Path::new("partition/../part")).is_err());
        assert!(validate_tombstone_part_path(Path::new("partition/part")).is_ok());
    }

    #[cfg(unix)]
    #[test]
    fn tombstone_resolution_rejects_symlink_escape() {
        let tmp = tempfile_dir();
        let parts_root = tmp.join("parts");
        let marker_dir = tmp.join("marker");
        let outside_partition = tmp.join("outside-partition");
        std::fs::create_dir_all(&parts_root).unwrap();
        std::fs::create_dir_all(&marker_dir).unwrap();
        std::fs::create_dir_all(outside_partition.join("part")).unwrap();
        std::os::unix::fs::symlink(&outside_partition, parts_root.join("escape")).unwrap();
        std::fs::write(
            marker_dir.join(MERGE_TOMBSTONE_FILE),
            r#"{"old_dirs":["escape/part"]}"#,
        )
        .unwrap();

        let result = read_merge_tombstone_dirs(&marker_dir, &parts_root);

        assert!(result.is_err());
        assert!(outside_partition.join("part").exists());
    }

    #[test]
    fn discover_retains_tombstone_when_old_part_deletion_fails() {
        let tmp = tempfile_dir();
        let parts_root = tmp.join("parts");
        std::fs::create_dir_all(&parts_root).unwrap();
        let replacement = flush_rows(
            vec![Row {
                tenant: test_tenant(),
                timestamp_ns: 1_700_000_000_000_000_000,
                labels: BTreeMap::new(),
                line: "replacement".to_string(),
                structured_metadata: vec![],
            }],
            &parts_root,
            100,
        )
        .unwrap()
        .remove(0);
        let partition = replacement.dir.parent().unwrap();
        let undeletable_as_directory = partition.join("old-part");
        std::fs::write(&undeletable_as_directory, b"not a directory").unwrap();
        let relative = undeletable_as_directory
            .strip_prefix(&parts_root)
            .unwrap()
            .to_string_lossy();
        std::fs::write(
            replacement.dir.join(MERGE_TOMBSTONE_FILE),
            format!(r#"{{"old_dirs":["{relative}"]}}"#),
        )
        .unwrap();

        let discovered = discover_parts(&parts_root).unwrap();

        assert_eq!(discovered.len(), 1);
        assert!(undeletable_as_directory.exists());
        assert!(replacement.dir.join(MERGE_TOMBSTONE_FILE).exists());
    }

    fn tenant_row(tenant: &str, line: &str, timestamp_ns: i64) -> Row {
        let mut labels: Labels = BTreeMap::new();
        labels.insert("app".to_string(), format!("{tenant}-app"));
        Row {
            tenant: TenantId::parse(tenant).unwrap(),
            timestamp_ns,
            labels,
            line: line.to_string(),
            structured_metadata: vec![],
        }
    }

    #[test]
    fn a_shared_part_confines_every_read_to_the_querying_tenant() {
        let tmp = tempfile_dir();
        // Interleaved in time and supplied out of tenant order, so the part
        // writer has to do the `(tenant, timestamp)` sort itself.
        let rows = vec![
            tenant_row("globex", "globex second", 2_000),
            tenant_row("acme", "acme first", 1_000),
            tenant_row("globex", "globex first", 500),
            tenant_row("acme", "acme second", 3_000),
        ];
        let part = flush_rows(rows, &tmp, 1).unwrap().remove(0);

        let acme = TenantId::parse("acme").unwrap();
        let globex = TenantId::parse("globex").unwrap();
        let outsider = TenantId::parse("initech").unwrap();

        let acme_segment = part.meta.tenant_segment(&acme).unwrap();
        let globex_segment = part.meta.tenant_segment(&globex).unwrap();
        assert_eq!(acme_segment.row_count, 2);
        assert_eq!(globex_segment.row_count, 2);
        assert_eq!(acme_segment.row_group_end, globex_segment.row_group_start);
        assert!(part.meta.tenant_segment(&outsider).is_none());
        // Per-tenant time bounds, not the part-wide range.
        assert_eq!((acme_segment.min_ts_ns, acme_segment.max_ts_ns), (1_000, 3_000));
        assert_eq!(
            (globex_segment.min_ts_ns, globex_segment.max_ts_ns),
            (500, 2_000)
        );

        let reader = PartReader::open(part).unwrap();
        let lines = |tenant: &TenantId| -> Vec<String> {
            reader
                .query(tenant, &[], &[], crate::part::QueryTimeRange::closed(i64::MIN, i64::MAX), 100, true)
                .unwrap()
                .into_iter()
                .flat_map(|stream| stream.entries)
                .map(|entry| entry.line)
                .collect()
        };
        assert_eq!(lines(&acme), vec!["acme first", "acme second"]);
        assert_eq!(lines(&globex), vec!["globex first", "globex second"]);
        assert!(lines(&outsider).is_empty());

        // The catalog surface is scoped too: acme must not learn that globex
        // exists from label names, values, or series.
        assert_eq!(reader.label_values(&acme, "app"), vec!["acme-app"]);
        assert_eq!(reader.label_values(&globex, "app"), vec!["globex-app"]);
        assert!(reader.label_names(&outsider).is_empty());
        assert!(reader.series(&outsider, &[]).is_empty());
        assert_eq!(reader.series(&acme, &[]).len(), 1);

        // A matcher that only another tenant satisfies must return nothing
        // rather than reaching across the segment boundary.
        let globex_matcher = LabelMatcher::new(
            "app".to_string(),
            MatcherOp::Eq,
            "globex-app".to_string(),
        )
        .unwrap();
        assert!(
            reader
                .query(&acme, &[globex_matcher], &[], crate::part::QueryTimeRange::closed(i64::MIN, i64::MAX), 100, true)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn row_groups_never_straddle_a_tenant_boundary() {
        let rows = vec![
            tenant_row("acme", "one", 1),
            tenant_row("acme", "two", 2),
            tenant_row("acme", "three", 3),
            tenant_row("globex", "four", 4),
        ];
        // A row group size of 2 would put acme's third row and globex's row in
        // one group if the boundary were not tenant-aligned.
        let bounds = row_group_bounds(&rows, 2);
        assert_eq!(bounds, vec![(0, 2), (2, 3), (3, 4)]);
        for (start, end) in bounds {
            let tenants: BTreeSet<_> = rows[start..end].iter().map(|row| &row.tenant).collect();
            assert_eq!(tenants.len(), 1);
        }
    }

    /// A field filter on a stream label prunes through the stream index.
    ///
    /// It is not in the exact-field bloom and never will be, but the index
    /// knows exactly which row groups hold each label value — so this used to
    /// be the one equality that could not prune, for want of asking the other
    /// side.
    #[test]
    fn a_field_filter_on_a_stream_label_prunes_through_the_index() {
        let tmp = tempfile_dir();
        let part = flush_rows(make_rows(), &tmp, 1).unwrap().remove(0);
        let reader = PartReader::open(part).unwrap();
        let range = QueryTimeRange::closed(i64::MIN, i64::MAX);

        // `app` is a stream label: two rows carry "test" and one "other".
        let selected = reader.select_row_groups_with_exact_fields(
            &test_tenant(),
            &[],
            &[],
            &[ExactFieldPredicate::new("app", "other")],
            range,
        );
        assert_eq!(
            selected.len(),
            1,
            "only the row group holding app=other is selected: {selected:?}"
        );

        // A value no row group holds selects nothing at all.
        assert!(
            reader
                .select_row_groups_with_exact_fields(
                    &test_tenant(),
                    &[],
                    &[],
                    &[ExactFieldPredicate::new("app", "absent")],
                    range,
                )
                .is_empty()
        );

        // An empty value means "absent or empty", and absence is not an index
        // entry, so it still scans.
        assert!(
            !reader
                .select_row_groups_with_exact_fields(
                    &test_tenant(),
                    &[],
                    &[],
                    &[ExactFieldPredicate::new("app", "")],
                    range,
                )
                .is_empty(),
            "an empty equality cannot be answered by the index"
        );
    }

    /// At-least-once delivery makes a second, byte-identical copy of an entry a
    /// normal event rather than a defect: a retried push whose first response
    /// was lost, or a WAL suffix replayed after a crash. Two copies of one entry
    /// must not become two log lines.
    #[test]
    fn identical_entries_are_written_once() {
        let root = tempfile_dir();
        let original = make_rows().remove(0);
        let replayed = original.clone();
        let parts = flush_rows(vec![original, replayed], &root, 100).unwrap();
        let rows: u64 = parts.iter().map(|part| part.meta.row_count).sum();
        assert_eq!(rows, 1);
    }

    /// Only a copy in every field is a copy. A shared timestamp is not enough,
    /// and neither is a shared line — dropping either of these would be losing
    /// data, not deduplicating it.
    #[test]
    fn entries_differing_in_any_field_are_all_kept() {
        let root = tempfile_dir();
        let base = make_rows().remove(0);

        let mut different_line = base.clone();
        different_line.line = "second".to_string();

        let mut different_timestamp = base.clone();
        different_timestamp.timestamp_ns += 1;

        let mut different_stream = base.clone();
        different_stream
            .labels
            .insert("pod".to_string(), "other".to_string());

        let mut different_metadata = base.clone();
        different_metadata.structured_metadata = vec![("trace".to_string(), "abc".to_string())];

        let mut different_tenant = base.clone();
        different_tenant.tenant = crate::tenant::TenantId::parse("other-tenant").unwrap();

        let parts = flush_rows(
            vec![
                base,
                different_line,
                different_timestamp,
                different_stream,
                different_metadata,
                different_tenant,
            ],
            &root,
            100,
        )
        .unwrap();
        let rows: u64 = parts.iter().map(|part| part.meta.row_count).sum();
        assert_eq!(rows, 6);
    }

    /// The container is the first thing read, so a file from a build that laid
    /// it out differently has to be refused rather than parsed as sections.
    #[test]
    fn an_index_file_without_the_container_header_is_refused() {
        assert!(split_index(b"BTF4nonsense").is_err());
        assert!(split_index(b"").is_err());
        // Header present, first length prefix truncated.
        let mut truncated = INDEX_MAGIC.to_vec();
        truncated.extend_from_slice(&[0u8; 2]);
        assert!(split_index(&truncated).is_err());
        // Length prefix claims more than follows it.
        let mut lying = INDEX_MAGIC.to_vec();
        lying.extend_from_slice(&99u32.to_le_bytes());
        lying.extend_from_slice(b"short");
        assert!(split_index(&lying).is_err());
    }

    /// Both sections come back exactly as written, and a part opens from them.
    /// Splitting one file into two payloads is the whole change; getting the
    /// boundary wrong would mix the blooms into the stream index silently.
    #[test]
    fn the_index_container_round_trips_both_sections() {
        let tmp = tempfile_dir();
        let part = flush_rows(make_rows(), &tmp, 1).unwrap().remove(0);
        let bytes = fs::read(part.index_path()).unwrap();
        let (bloom, streams) = split_index(&bytes).unwrap();
        assert_eq!(&bloom[..4], BLOOM_MAGIC_V4);
        assert_eq!(&streams[..STREAM_MAGIC.len()], STREAM_MAGIC);
        assert_eq!(
            INDEX_MAGIC.len() + 4 + bloom.len() + 4 + streams.len(),
            bytes.len(),
            "the container holds the two sections and nothing else"
        );
        assert!(PartReader::open(part).is_ok());
    }
