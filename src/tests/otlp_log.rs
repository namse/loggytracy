    use super::*;
    use opentelemetry_proto::tonic::common::v1::InstrumentationScope;
    use opentelemetry_proto::tonic::logs::v1::{ResourceLogs, ScopeLogs};
    use opentelemetry_proto::tonic::resource::v1::Resource;

    fn string_value(value: &str) -> AnyValue {
        AnyValue {
            value: Some(any_value::Value::StringValue(value.to_string())),
        }
    }

    fn attribute(key: &str, value: AnyValue) -> KeyValue {
        KeyValue {
            key: key.to_string(),
            value: Some(value),
            ..Default::default()
        }
    }

    fn request(resource: Vec<KeyValue>, records: Vec<LogRecord>) -> ExportLogsServiceRequest {
        ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: Some(Resource {
                    attributes: resource,
                    dropped_attributes_count: 0,
                    entity_refs: Vec::new(),
                }),
                scope_logs: vec![ScopeLogs {
                    scope: Some(InstrumentationScope {
                        name: "checkout".to_string(),
                        ..Default::default()
                    }),
                    log_records: records,
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        }
    }

    fn record(time_unix_nano: u64, body: &str) -> LogRecord {
        LogRecord {
            time_unix_nano,
            observed_time_unix_nano: 0,
            severity_number: 9,
            severity_text: "INFO".to_string(),
            body: Some(string_value(body)),
            ..Default::default()
        }
    }

    /// The split between labels and structured metadata is the cardinality
    /// decision. An OTLP resource carries dozens of attributes and several of
    /// them are unique per process, so promoting all of them would make every
    /// pod its own stream — and streams are what the plan sells a bounded
    /// number of.
    #[test]
    fn only_the_promoted_resource_attributes_become_labels() {
        let normalized = normalize_request(&request(
            vec![
                attribute("service.name", string_value("checkout")),
                attribute("k8s.namespace.name", string_value("prod")),
                // Unique per process: a label here is a new stream per restart.
                attribute("process.pid", string_value("41213")),
                attribute("telemetry.sdk.version", string_value("1.28.0")),
            ],
            vec![record(1_700_000_000_000_000_000, "order placed")],
        ))
        .unwrap();

        assert_eq!(normalized.len(), 1, "one resource is one stream");
        let (labels, entries) = &normalized[0];
        assert_eq!(
            labels.keys().collect::<Vec<_>>(),
            vec!["k8s_namespace_name", "service_name"],
            "dots become underscores because LogQL matches Prometheus-shaped names"
        );
        assert_eq!(labels["service_name"], "checkout");

        let metadata: BTreeMap<&str, &str> = entries[0]
            .structured_metadata
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()))
            .collect();
        assert_eq!(metadata["process.pid"], "41213");
        assert_eq!(metadata["telemetry.sdk.version"], "1.28.0");
        assert_eq!(metadata["severity_text"], "INFO");
        assert_eq!(metadata["severity_number"], "9");
        assert_eq!(metadata["scope_name"], "checkout");
    }

    /// Records sharing a label set are one stream. An export normally carries
    /// many records from one resource, and a stream per record would defeat
    /// the grouping the storage layer is built on.
    #[test]
    fn records_from_one_resource_are_grouped_into_one_stream() {
        let normalized = normalize_request(&request(
            vec![attribute("service.name", string_value("checkout"))],
            vec![
                record(1_700_000_000_000_000_000, "first"),
                record(1_700_000_001_000_000_000, "second"),
                record(1_700_000_002_000_000_000, "third"),
            ],
        ))
        .unwrap();

        assert_eq!(normalized.len(), 1);
        assert_eq!(normalized[0].1.len(), 3);
        assert_eq!(normalized[0].1[2].line, "third");
    }

    /// `time_unix_nano` is optional in OTLP and a collector leaves it at zero
    /// when the source had none. Falling back to the observed time is the
    /// difference between a usable record and one stamped at the epoch.
    #[test]
    fn a_record_without_a_timestamp_falls_back_to_when_it_was_observed() {
        let mut without_time = record(0, "no timestamp at the source");
        without_time.observed_time_unix_nano = 1_700_000_005_000_000_000;
        let normalized = normalize_request(&request(
            vec![attribute("service.name", string_value("checkout"))],
            vec![without_time],
        ))
        .unwrap();
        assert_eq!(normalized[0].1[0].timestamp_ns, 1_700_000_005_000_000_000);
    }

    /// Trace correlation is the reason logs and traces live in one engine, so
    /// the ids have to arrive in the form the trace side already speaks.
    #[test]
    fn trace_and_span_ids_arrive_as_hex_like_everywhere_else() {
        let mut correlated = record(1_700_000_000_000_000_000, "handled");
        correlated.trace_id = vec![0x0a; 16];
        correlated.span_id = vec![0xff, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07];
        let normalized = normalize_request(&request(Vec::new(), vec![correlated])).unwrap();
        let metadata: BTreeMap<&str, &str> = normalized[0].1[0]
            .structured_metadata
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()))
            .collect();
        assert_eq!(metadata["trace_id"], "0a".repeat(16));
        assert_eq!(metadata["span_id"], "ff01020304050607");
    }

    /// A non-scalar body is rendered as JSON rather than dropped, so the
    /// parser filters that already exist for structured lines can reach into
    /// it. Dropping it would lose the record's only content.
    #[test]
    fn a_composite_body_is_kept_as_json() {
        let mut structured = record(1_700_000_000_000_000_000, "");
        structured.body = Some(AnyValue {
            value: Some(any_value::Value::KvlistValue(
                opentelemetry_proto::tonic::common::v1::KeyValueList {
                    values: vec![attribute("event", string_value("checkout.completed"))],
                },
            )),
        });
        let normalized = normalize_request(&request(Vec::new(), vec![structured])).unwrap();
        let line = &normalized[0].1[0].line;
        assert!(line.contains("checkout.completed"), "{line}");
    }

    #[test]
    fn an_export_with_no_records_is_rejected_rather_than_silently_accepted() {
        let error = normalize_request(&ExportLogsServiceRequest::default())
            .err()
            .expect("an export with no records is an error");
        assert_eq!(error, OtlpLogError::EmptyRequest);
    }

    /// The journal stores every log record as a Loki `PushRequest` whatever
    /// protocol it arrived on, so replay has one decoder. That only holds if
    /// the label rendering round-trips — a value with a quote in it would
    /// otherwise write a record the WAL cannot read back.
    #[test]
    fn normalized_labels_round_trip_through_the_journal_encoding() {
        let awkward = normalize_request(&request(
            vec![attribute(
                "service.name",
                string_value(r#"say "hi"\ then a newline
"#),
            )],
            vec![record(1_700_000_000_000_000_000, "line")],
        ))
        .unwrap();

        let encoded = crate::proto::encode_push_request(&awkward);
        let decoded = <crate::proto::PushRequest as prost::Message>::decode(encoded.as_slice())
            .expect("the journal encoding decodes");
        let labels = crate::proto::parse_labels(&decoded.streams[0].labels)
            .expect("and its labels parse back");
        assert_eq!(labels, awkward[0].0);
        assert_eq!(
            decoded.streams[0].entries[0].timestamp_ns().unwrap(),
            1_700_000_000_000_000_000
        );
    }
