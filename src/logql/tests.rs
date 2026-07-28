
    use super::*;

    fn entry(line: &str, metadata: &[(&str, &str)]) -> LogEntry {
        LogEntry {
            timestamp_ns: 1,
            line: line.to_string(),
            structured_metadata: metadata
                .iter()
                .map(|(name, value)| ((*name).to_string(), (*value).to_string()))
                .collect(),
        }
    }

    #[test]
    fn ordered_pipeline_parses_all_m3_stages() {
        let query = parse(
            r#"{app="x"} |= "ready" | json | status=200 | elapsed >= 100ms | logfmt | user=~"a.+""#,
        )
        .unwrap();
        assert_eq!(query.stages.len(), 6);
        assert!(matches!(query.stages[0], PipelineStage::Line(_)));
        assert!(matches!(query.stages[1], PipelineStage::Json));
        assert!(matches!(query.stages[2], PipelineStage::Field(_)));
        assert!(matches!(query.stages[4], PipelineStage::Logfmt));

        let comparisons =
            parse(r#"{} | a="x" | b!="y" | c=~"x" | d!~"y" | e<1 | f<=2 | g>3 | h>=4"#).unwrap();
        assert_eq!(comparisons.stages.len(), 8);
    }

    #[test]
    fn parser_keywords_can_also_be_field_names() {
        let json = parse(r#"{} | json | json="ok""#).unwrap();
        assert!(json.matches_entry(&entry(r#"{"json":"ok"}"#, &[])));

        let logfmt = parse(r#"{} | logfmt="value""#).unwrap();
        assert!(logfmt.matches_entry(&entry("line", &[("logfmt", "value")])));
    }

    #[test]
    fn json_scalar_extraction_is_ordered_and_metadata_wins_collisions() {
        let query = parse(r#"{} | json | trace_id="metadata" | nested_count=2"#).unwrap();
        assert!(query.matches_entry(&entry(
            r#"{"nested":{"count":2},"trace_id":"line"}"#,
            &[("trace_id", "metadata")],
        )));

        let synthesized =
            parse(r#"{} | json | foo_extracted_2="z""#).expect("synthesized field query");
        assert!(synthesized.exact_field_predicates().is_empty());
        assert!(synthesized.matches_entry(&entry(
            r#"{"foo":"z"}"#,
            &[("foo", "x"), ("foo_extracted", "y")],
        )));

        let sanitized = parse(r#"{} | json | namespace_key="value" | _9code=200"#).unwrap();
        assert!(sanitized.matches_entry(&entry(r#"{"namespace:key":"value","9code":200}"#, &[],)));

        let storage_reserved = parse(r#"{} | json | _msg="parsed""#).unwrap();
        assert!(storage_reserved.matches_entry(&entry(r#"{"_msg":"parsed"}"#, &[],)));

        let structured = parse(r#"{} | structured_metadata="queryable""#).unwrap();
        assert!(structured.matches_entry(&entry("line", &[("structured_metadata", "queryable")],)));
    }

    #[test]
    fn parser_sanitization_collisions_do_not_drop_scalar_values() {
        let json =
            extract_json(r#"{"a-b":"dash","a_b":"underscore","a_b_extracted":"native"}"#).unwrap();
        assert_eq!(json.len(), 3);
        assert!(json.values().any(|value| value == "dash"));
        assert!(json.values().any(|value| value == "underscore"));
        assert!(json.values().any(|value| value == "native"));

        let logfmt = extract_logfmt("a-b=dash a_b=underscore a_b=different").unwrap();
        assert_eq!(logfmt.get("a_b"), Some(&"dash".to_string()));
        assert_eq!(logfmt.get("a_b_extracted"), Some(&"different".to_string()));
        assert_eq!(logfmt.len(), 2);
    }

    #[test]
    fn stream_labels_are_pipeline_fields_and_parser_collisions_are_extracted() {
        let query = parse(r#"{} | json | app="api""#).unwrap();
        let labels: Labels = [("app".to_string(), "api".to_string())]
            .into_iter()
            .collect();
        let mut parsed = entry(r#"{"app":"line"}"#, &[]);

        assert!(query.process_entry_with_labels(&labels, &mut parsed));
        assert_eq!(
            parsed.structured_metadata,
            vec![("app_extracted".to_string(), "line".to_string())]
        );

        let logfmt = parse(r#"{} | logfmt | app="api""#).unwrap();
        let mut parsed = entry("app=line", &[]);
        assert!(logfmt.process_entry_with_labels(&labels, &mut parsed));
        assert_eq!(
            parsed.structured_metadata,
            vec![("app_extracted".to_string(), "line".to_string())]
        );

        let field_only = parse(r#"{} | app="api""#).unwrap();
        assert!(field_only.process_entry_with_labels(&labels, &mut entry("line", &[])));
    }

    #[test]
    fn malformed_parsers_set_a_filterable_error_field() {
        let json = parse(r#"{} | json | __error__="JSONParserErr""#).unwrap();
        let logfmt = parse(r#"{} | logfmt | __error__="LogfmtParserErr""#).unwrap();
        let mut malformed = entry("not json", &[]);
        assert!(json.process_entry(&mut malformed));
        assert_eq!(
            malformed.structured_metadata,
            vec![("__error__".to_string(), "JSONParserErr".to_string())]
        );
        assert!(logfmt.matches_entry(&entry("missing-equals", &[])));

        let mut adjacent = entry(r#"level="error"status=500"#, &[]);
        assert!(logfmt.process_entry(&mut adjacent));
        assert_eq!(
            adjacent.structured_metadata,
            vec![("__error__".to_string(), "LogfmtParserErr".to_string())]
        );
        assert!(
            !parse(r#"{} | logfmt | level="error""#)
                .unwrap()
                .matches_entry(&adjacent),
            "fields parsed before a malformed boundary must not leak"
        );

        let mut bad_escape = entry(r#"value="bad\q""#, &[]);
        assert!(logfmt.process_entry(&mut bad_escape));
        assert_eq!(
            bad_escape.structured_metadata,
            vec![("__error__".to_string(), "LogfmtParserErr".to_string())]
        );
    }

    #[test]
    fn logfmt_extracts_quoted_string_and_duration_scalars() {
        let query =
            parse(r#"{} | logfmt | level="error" | message=~"hello.*" | elapsed<1s"#).unwrap();
        assert!(query.matches_entry(&entry(
            r#"level=error message="hello world" elapsed=250ms"#,
            &[],
        )));
        assert!(query.matches_entry(&entry(
            "level=error\tmessage=\"hello tab\"\telapsed=250ms",
            &[],
        )));
        assert_eq!(
            extract_logfmt(r#"value="quote: \" slash: \\ tab:\t""#)
                .unwrap()
                .get("value")
                .unwrap(),
            "quote: \" slash: \\ tab:\t"
        );
    }

    #[test]
    fn field_regexes_match_whole_values() {
        let exact = parse(r#"{} | level=~"err""#).unwrap();
        let negative = parse(r#"{} | level!~"err""#).unwrap();
        assert!(exact.matches_entry(&entry("line", &[("level", "err")])));
        assert!(!exact.matches_entry(&entry("line", &[("level", "error")])));
        assert!(!negative.matches_entry(&entry("line", &[("level", "err")])));
        assert!(negative.matches_entry(&entry("line", &[("level", "error")])));
    }

    #[test]
    fn parses_metric_tree_and_compound_duration() {
        let QueryExpr::Metric(expr) =
            parse_expr(r#"topk(3, sum by (app) (rate({app=~".+"} | json | ok="yes" [1m30s])))"#)
                .unwrap()
        else {
            panic!("expected metric")
        };
        assert_eq!(expr.lookback_ns(), 90_000_000_000);
        assert_eq!(expr.log_query().exact_field_predicates().len(), 1);
        assert!(expr.log_query().exact_field_predicates()[0].may_be_extracted);

        let QueryExpr::Metric(escaped) =
            parse_expr(r#"count_over_time({app="a\"b"}[5m])"#).unwrap()
        else {
            panic!("expected metric")
        };
        assert_eq!(escaped.log_query().matchers[0].value, "a\"b");
    }

    #[test]
    fn parses_postfix_aggregation_grouping() {
        let QueryExpr::Metric(expr) =
            parse_expr(r#"sum(count_over_time({}[5m])) by (level, __error__)"#).unwrap()
        else {
            panic!("expected metric")
        };
        let MetricExpr::Aggregate { grouping: by, .. } = expr else {
            panic!("expected aggregate")
        };
        assert_eq!(
            by,
            Some(Grouping::By(vec![
                "__error__".to_string(),
                "level".to_string()
            ]))
        );
    }

    #[test]
    fn without_keeps_every_label_it_does_not_name() {
        let QueryExpr::Metric(expr) =
            parse_expr(r#"sum without (pod) (count_over_time({}[5m]))"#).unwrap()
        else {
            panic!("expected metric")
        };
        let MetricExpr::Aggregate { grouping, .. } = expr else {
            panic!("expected aggregate")
        };
        let grouping = grouping.expect("a grouping clause");
        let labels: std::collections::BTreeMap<String, String> = [
            ("app".to_string(), "a".to_string()),
            ("pod".to_string(), "p1".to_string()),
        ]
        .into_iter()
        .collect();
        let key = grouping.key(&labels);
        assert_eq!(key.len(), 1);
        assert_eq!(key["app"], "a");
    }

    #[test]
    fn binary_operators_take_a_scalar_on_either_side() {
        for query in [
            r#"rate({app="a"}[5m]) / 1024"#,
            r#"1024 * rate({app="a"}[5m])"#,
            r#"sum(count_over_time({app="a"}[5m])) > 100"#,
        ] {
            assert!(parse_expr(query).is_ok(), "{query}");
        }
    }

    /// The scalar's side is kept because subtraction and division are not
    /// commutative: `2 - rate(…)` is a different query from `rate(…) - 2`.
    #[test]
    fn a_scalar_on_the_left_stays_on_the_left() {
        let QueryExpr::Metric(MetricExpr::Binary {
            op,
            scalar,
            scalar_on_left,
            ..
        }) = parse_expr(r#"2 - rate({app="a"}[5m])"#).unwrap()
        else {
            panic!("expected a binary metric")
        };
        assert_eq!(op, BinaryOp::Sub);
        assert_eq!(scalar, 2.0);
        assert!(scalar_on_left);
    }

    /// A comparison keeps the sample when it holds and drops the series when it
    /// does not, rather than yielding 1 or 0. That is what makes `> 100` a
    /// filter, and it is Prometheus's and Loki's behaviour.
    #[test]
    fn comparisons_filter_rather_than_indicate() {
        assert!(BinaryOp::Gt.is_comparison());
        assert_eq!(BinaryOp::Gt.apply(150.0, 100.0), Some(150.0));
        assert_eq!(BinaryOp::Gt.apply(50.0, 100.0), None);
        assert!(!BinaryOp::Add.is_comparison());
        assert_eq!(BinaryOp::Add.apply(1.0, 2.0), Some(3.0));
    }

    /// Operators inside brackets, selectors and strings are not operators. The
    /// `-` of `[5m]`, the `>` of a field filter and a `/` in a quoted path all
    /// live inside something.
    #[test]
    fn operators_inside_brackets_and_strings_are_not_split_on() {
        for query in [
            r#"rate({app="a"}[5m])"#,
            r#"count_over_time({app="a"} | logfmt | status > 400 [5m])"#,
            r#"count_over_time({app="a"} |= "a/b" [5m])"#,
            r#"sum by (a) (rate({app="a"}[5m]))"#,
        ] {
            assert!(parse_expr(query).is_ok(), "{query}");
        }
    }

    /// Both sides being selectors would need a scan each, and every read path
    /// here is built around one log query per metric expression. Refused in the
    /// parser rather than half-done in the evaluator.
    #[test]
    fn vector_to_vector_operations_are_refused() {
        let error = parse_expr(r#"rate({app="a"}[5m]) / rate({app="b"}[5m])"#).unwrap_err();
        assert!(error.contains("own scan"), "{error}");
    }

    #[test]
    fn rejects_deep_metric_nesting() {
        let mut query = "count_over_time({}[1s])".to_string();
        for _ in 0..70 {
            query = format!("sum({query})");
        }
        let error = parse_expr(&query).unwrap_err();
        assert!(error.contains("nesting"));
    }

    #[test]
    fn metric_grouping_accepts_parser_fields_named_like_storage_columns() {
        assert!(parse_expr(
            r#"sum by (_msg, timestamp_ns, structured_metadata) (count_over_time({} | json [5m]))"#
        )
        .is_ok());
    }

    #[test]
    fn pipeline_fields_are_retained_for_query_evaluation() {
        let query = parse(r#"{} | json | level="error""#).unwrap();
        let mut parsed = entry(r#"{"level":"error"}"#, &[]);
        assert!(query.process_entry(&mut parsed));
        assert_eq!(
            parsed.structured_metadata,
            vec![("level".to_string(), "error".to_string())]
        );
    }

    #[test]
    fn duration_parsing_is_exact_and_checked() {
        assert_eq!(
            parse_duration_ns("9007199254740993ns").unwrap(),
            9_007_199_254_740_993
        );
        assert!(parse_duration_ns("9223372036854775808ns").is_err());
        assert_eq!(parse_duration_ns("1.5s250ms").unwrap(), 1_750_000_000);
        assert_eq!(parse_duration_ns("1.9ns").unwrap(), 1);
        assert_eq!(parse_duration_ns("0.5ns").unwrap(), 0);
        assert_eq!(parse_duration_ns("0.9ns0.9ns").unwrap(), 0);

        let QueryExpr::Metric(metric) =
            parse_expr("count_over_time({}[9007199254740993ns])").unwrap()
        else {
            panic!("expected metric")
        };
        assert_eq!(metric.lookback_ns(), 9_007_199_254_740_993);
        assert!(parse_expr("count_over_time({}[9223372036854775808ns])").is_err());
        assert!(parse_expr("count_over_time({}[0.5ns])").is_err());

        let filter = parse("{} | elapsed=9007199254740993ns").unwrap();
        assert!(filter.matches_entry(&entry("line", &[("elapsed", "9007199254740993ns")],)));
        assert!(parse("{} | elapsed=9223372036854775808ns").is_err());

        let scientific = parse("{} | count>=1e3").unwrap();
        assert!(scientific.matches_entry(&entry("line", &[("count", "1000")])));
        assert!(parse("{} | count=NaN").is_err());
        assert!(parse("{} | count=inf").is_err());
    }

    #[test]
    fn numeric_field_comparisons_do_not_round_at_two_to_the_fifty_third() {
        let query = parse("{} | value!=9007199254740993").unwrap();
        assert!(query.matches_entry(&entry("line", &[("value", "9007199254740992")],)));
        assert!(!query.matches_entry(&entry("line", &[("value", "9007199254740993")],)));
        assert!(!query.matches_entry(&entry("line", &[("value", "NaN")])));
        assert!(!query.matches_entry(&entry("line", &[("value", "Inf")])));

        let decimal = parse("{} | value=500.0").unwrap();
        assert!(decimal.matches_entry(&entry("line", &[("value", "500")])));
        assert!(parse("{} | value=1e-9223372036854775808").is_err());
    }

    #[test]
    fn quoted_logql_unicode_escape_is_decoded() {
        let query = parse(r#"{} | message="caf\u00e9""#).unwrap();
        assert!(query.matches_entry(&entry("line", &[("message", "café")])));
    }

    #[test]
    fn matcher_regex_value_with_brace() {
        let query = parse(r#"{app=~".{3}"}"#).unwrap();
        assert_eq!(query.matchers[0].value, ".{3}");
    }

    #[test]
    fn rejects_invalid_and_deferred_syntax() {
        assert!(parse_expr(r#"quantile_over_time(0.9, {}[5m])"#).is_err());
        assert!(parse_expr(r#"rate({}[0s])"#).is_err());
    }

    #[test]
    fn unsupported_unicode_stage_returns_error_instead_of_panicking() {
        assert!(parse("{} |文字文字文字文字文字文字文字").is_err());
        assert!(parse("{} 文字文字文字文字文字文字文字").is_err());
        let query = parse(r#"{app="\文"}"#).unwrap();
        assert_eq!(query.matchers[0].value, r#"\文"#);
    }

    #[test]
    fn line_format_rewrites_the_line_from_extracted_fields() {
        let QueryExpr::Logs(query) =
            parse_expr(r#"{app="a"} | logfmt | line_format "{{.status}} {{.path}}""#).unwrap()
        else {
            panic!("expected a log query")
        };
        let mut entry = LogEntry {
            timestamp_ns: 1,
            line: "status=500 path=/checkout user=alice".to_string(),
            structured_metadata: vec![],
        };
        assert!(query.process_entry(&mut entry));
        assert_eq!(entry.line, "500 /checkout");
    }

    /// A filter after a `line_format` sees the formatted line. That is what
    /// makes the two composable in either order, and it is why the stage
    /// rewrites the entry rather than only the output.
    #[test]
    fn a_filter_after_line_format_matches_the_formatted_line() {
        let QueryExpr::Logs(query) =
            parse_expr(r#"{app="a"} | logfmt | line_format "{{.status}}" |= "500""#).unwrap()
        else {
            panic!("expected a log query")
        };
        let mut matching = LogEntry {
            timestamp_ns: 1,
            line: "status=500 path=/a".to_string(),
            structured_metadata: vec![],
        };
        assert!(query.process_entry(&mut matching));

        let mut other = LogEntry {
            timestamp_ns: 1,
            // The original line contains "500", the formatted one does not.
            line: "status=200 latency=500".to_string(),
            structured_metadata: vec![],
        };
        assert!(!query.process_entry(&mut other));
    }

    /// A field the template names but the entry does not have renders empty,
    /// which is what Go's template does with a missing map key.
    #[test]
    fn a_missing_template_field_renders_empty() {
        let QueryExpr::Logs(query) =
            parse_expr(r#"{app="a"} | logfmt | line_format "[{{.absent}}]""#).unwrap()
        else {
            panic!("expected a log query")
        };
        let mut entry = LogEntry {
            timestamp_ns: 1,
            line: "status=200".to_string(),
            structured_metadata: vec![],
        };
        assert!(query.process_entry(&mut entry));
        assert_eq!(entry.line, "[]");
    }

    /// Quoting is what separates a rename from a literal: `new=old` copies a
    /// field, `new="old"` assigns the text.
    #[test]
    fn label_format_distinguishes_a_rename_from_a_template() {
        let QueryExpr::Logs(query) =
            parse_expr(r#"{app="a"} | logfmt | label_format code=status, kind="status""#).unwrap()
        else {
            panic!("expected a log query")
        };
        let mut entry = LogEntry {
            timestamp_ns: 1,
            line: "status=500".to_string(),
            structured_metadata: vec![],
        };
        assert!(query.process_entry(&mut entry));
        let fields: std::collections::BTreeMap<_, _> =
            entry.structured_metadata.iter().cloned().collect();
        assert_eq!(fields["code"], "500", "unquoted copies the field");
        assert_eq!(fields["kind"], "status", "quoted assigns the text");
    }

    /// Assignments read the field set as it was before the stage, so a swap
    /// swaps instead of collapsing. Evaluating them in sequence would make the
    /// result depend on argument order.
    #[test]
    fn label_format_assignments_do_not_see_each_other() {
        let QueryExpr::Logs(query) =
            parse_expr(r#"{app="a"} | logfmt | label_format a=b, b=a"#).unwrap()
        else {
            panic!("expected a log query")
        };
        let mut entry = LogEntry {
            timestamp_ns: 1,
            line: "a=first b=second".to_string(),
            structured_metadata: vec![],
        };
        assert!(query.process_entry(&mut entry));
        let fields: std::collections::BTreeMap<_, _> =
            entry.structured_metadata.iter().cloned().collect();
        assert_eq!(fields["a"], "second");
        assert_eq!(fields["b"], "first");
    }

    /// The template subset refuses what it cannot render rather than
    /// approximating it. A half-supported template produces lines that look
    /// right and are not, which the user cannot see; a parse error they can.
    #[test]
    fn unsupported_template_expressions_are_refused() {
        for query in [
            r#"{app="a"} | line_format "{{ if .x }}y{{ end }}""#,
            r#"{app="a"} | line_format "{{.a""#,
            r#"{app="a"} | line_format "{{ printf \"%s\" .a }}""#,
        ] {
            assert!(parse_expr(query).is_err(), "{query}");
        }
    }

    /// A `label_format` can synthesize any name, so an equality after it says
    /// nothing about what is stored and must not prune a row group.
    #[test]
    fn a_predicate_after_label_format_does_not_prune() {
        let QueryExpr::Logs(query) =
            parse_expr(r#"{app="a"} | logfmt | label_format code=status | code="500""#).unwrap()
        else {
            panic!("expected a log query")
        };
        assert!(
            query.exact_field_predicates().is_empty(),
            "a synthesized name is not an index term"
        );
    }

    #[test]
    fn unwrap_and_the_value_functions_parse() {
        for query in [
            r#"sum_over_time({app="a"} | logfmt | unwrap latency [5m])"#,
            r#"avg_over_time({app="a"} | logfmt | unwrap duration(took) [5m])"#,
            r#"quantile_over_time(0.99, {app="a"} | logfmt | unwrap latency [5m])"#,
            r#"max_over_time({app="a"} | logfmt | unwrap latency [1h])"#,
        ] {
            assert!(parse_expr(query).is_ok(), "{query}");
        }
    }

    /// A function over values with no values named is a query the user got
    /// wrong. Answering it with an empty result reads as "no data" rather than
    /// "no question", so it is refused.
    #[test]
    fn a_value_function_without_unwrap_is_refused() {
        assert!(parse_expr(r#"sum_over_time({app="a"}[5m])"#).is_err());
        assert!(parse_expr(r#"quantile_over_time(0.9, {app="a"}[5m])"#).is_err());
        // And the converse: counting functions take no unwrap.
        assert!(parse_expr(r#"count_over_time({app="a"} | logfmt | unwrap x [5m])"#).is_err());
    }

    #[test]
    fn quantiles_outside_zero_to_one_are_refused() {
        assert!(parse_expr(r#"quantile_over_time(1.5, {a="b"} | unwrap x [5m])"#).is_err());
        assert!(parse_expr(r#"quantile_over_time(-0.1, {a="b"} | unwrap x [5m])"#).is_err());
        assert!(parse_expr(r#"quantile_over_time(nope, {a="b"} | unwrap x [5m])"#).is_err());
    }

    /// Only a bare field and `duration(field)` convert. Anything else errors
    /// rather than silently yielding nothing for every entry.
    #[test]
    fn unsupported_unwrap_conversions_are_refused() {
        assert!(parse_expr(r#"sum_over_time({a="b"} | unwrap bytes(size) [5m])"#).is_err());
        assert!(parse_expr(r#"sum_over_time({a="b"} | unwrap foo(x) [5m])"#).is_err());
    }

    /// The unwrap belongs to the range function, not to the pipeline, so a log
    /// query can never carry one — which is what makes the "needs an unwrap"
    /// check possible at parse time.
    #[test]
    fn unwrap_is_not_a_log_pipeline_stage() {
        assert!(parse(r#"{app="a"} | unwrap latency"#).is_err());
    }

    #[test]
    fn a_duration_unwrap_yields_seconds() {
        let unwrap = Unwrap {
            field: "took".to_string(),
            conversion: UnwrapConversion::Duration,
        };
        let fields: std::collections::BTreeMap<String, String> =
            [("took".to_string(), "1500ms".to_string())]
                .into_iter()
                .collect();
        assert_eq!(unwrap.value(&fields), Some(1.5));
    }

    /// A field that is absent or does not convert drops the entry rather than
    /// contributing zero. Zero is a value someone will plot, and a parse
    /// failure is not a measurement of zero.
    #[test]
    fn an_unconvertible_unwrap_drops_the_entry() {
        let unwrap = Unwrap {
            field: "latency".to_string(),
            conversion: UnwrapConversion::None,
        };
        let absent = std::collections::BTreeMap::new();
        assert_eq!(unwrap.value(&absent), None);
        let unparseable: std::collections::BTreeMap<String, String> =
            [("latency".to_string(), "fast".to_string())]
                .into_iter()
                .collect();
        assert_eq!(unwrap.value(&unparseable), None);
        let infinite: std::collections::BTreeMap<String, String> =
            [("latency".to_string(), "inf".to_string())]
                .into_iter()
                .collect();
        assert_eq!(unwrap.value(&infinite), None, "infinity is not a sample");
    }

    /// The scan range is cut from the lookback, so an offset that did not
    /// widen it would select parts that do not contain the window being asked
    /// about — the query would read as empty rather than as wrong.
    #[test]
    fn an_offset_widens_the_lookback() {
        let QueryExpr::Metric(plain) = parse_expr(r#"rate({a="b"}[5m])"#).unwrap() else {
            panic!("expected metric")
        };
        let QueryExpr::Metric(offset) = parse_expr(r#"rate({a="b"}[5m] offset 1h)"#).unwrap()
        else {
            panic!("expected metric")
        };
        assert_eq!(plain.lookback_ns(), 5 * 60 * 1_000_000_000);
        assert_eq!(
            offset.lookback_ns(),
            (5 * 60 + 60 * 60) * 1_000_000_000,
            "the offset has to be inside the scan range"
        );
    }

    /// `offset` is only the keyword when it follows the range bracket. A field
    /// or label of that name is not a modifier.
    #[test]
    fn the_word_offset_elsewhere_is_not_a_modifier() {
        assert!(parse_expr(r#"count_over_time({app="offset"}[5m])"#).is_ok());
        assert!(parse_expr(r#"count_over_time({a="b"} | logfmt | offset="1" [5m])"#).is_ok());
        assert!(parse_expr(r#"count_over_time({a="b"}[5m] offset)"#).is_err());
        assert!(parse_expr(r#"count_over_time({a="b"}[5m] offset -1h)"#).is_err());
    }
