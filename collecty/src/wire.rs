use std::io::{Read, Write};

use crate::signal::Signal;

pub const ZSTD_LEVEL: i32 = 3;

pub const RECORD_HEADER_BYTES: usize = 5;

pub fn compress_record(signal: Signal, payload: &[u8], level: i32) -> std::io::Result<Vec<u8>> {
    let mut header = [0u8; RECORD_HEADER_BYTES];
    header[0] = signal.tag();
    header[1..5].copy_from_slice(&(payload.len() as u32).to_le_bytes());

    let mut encoder = zstd::stream::write::Encoder::new(
        Vec::with_capacity(RECORD_HEADER_BYTES + payload.len() / 2),
        level,
    )?;
    encoder.write_all(&header)?;
    encoder.write_all(payload)?;
    encoder.finish()
}

pub fn split_records(plain: &[u8]) -> Result<Vec<(Signal, &[u8])>, String> {
    let mut records = Vec::new();
    let mut at = 0;
    while at < plain.len() {
        let Some(header) = plain.get(at..at + RECORD_HEADER_BYTES) else {
            return Err(format!(
                "a record header needs {RECORD_HEADER_BYTES} bytes and {} are left",
                plain.len() - at
            ));
        };
        let Some(signal) = Signal::from_tag(header[0]) else {
            return Err(format!("{} is not a signal tag", header[0]));
        };
        let len = u32::from_le_bytes(header[1..5].try_into().expect("four bytes")) as usize;
        at += RECORD_HEADER_BYTES;
        let Some(payload) = plain.get(at..at + len) else {
            return Err(format!(
                "a record claims {len} bytes and {} are left",
                plain.len() - at
            ));
        };
        at += len;
        records.push((signal, payload));
    }
    Ok(records)
}

pub fn decompress_concatenated(
    frames: &[u8],
    expected_plain_len: usize,
) -> std::io::Result<Vec<u8>> {
    let mut plain = Vec::with_capacity(expected_plain_len);
    zstd::stream::read::Decoder::new(frames)?.read_to_end(&mut plain)?;
    Ok(plain)
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::collector::logs::v1::{
        ExportLogsServiceRequest, ExportLogsServiceResponse,
    };
    use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
    use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
    use opentelemetry_proto::tonic::common::v1::{
        AnyValue, InstrumentationScope, KeyValue, any_value,
    };
    use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
    use opentelemetry_proto::tonic::metrics::v1::ResourceMetrics;
    use opentelemetry_proto::tonic::resource::v1::Resource;
    use opentelemetry_proto::tonic::trace::v1::ResourceSpans;
    use prost::Message;

    fn request_with(service: &str, body: &str) -> ExportLogsServiceRequest {
        ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: Some(Resource {
                    attributes: vec![KeyValue {
                        key: "service.name".to_string(),
                        value: Some(AnyValue {
                            value: Some(any_value::Value::StringValue(service.to_string())),
                        }),
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
                scope_logs: vec![ScopeLogs {
                    scope: Some(InstrumentationScope::default()),
                    log_records: vec![LogRecord {
                        time_unix_nano: 1,
                        body: Some(AnyValue {
                            value: Some(any_value::Value::StringValue(body.to_string())),
                        }),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }

    #[test]
    fn concatenated_frames_decompress_to_the_concatenated_records() {
        let first = b"the first request's bytes, long enough that zstd emits a real frame";
        let second = b"the second request's bytes, also long enough to be worth compressing";

        let mut frames = compress_record(Signal::Logs, first, ZSTD_LEVEL).unwrap();
        frames.extend_from_slice(&compress_record(Signal::Traces, second, ZSTD_LEVEL).unwrap());

        let plain = decompress_concatenated(&frames, frames.len() * 4).unwrap();
        let records = split_records(&plain).unwrap();

        assert_eq!(
            records,
            vec![
                (Signal::Logs, first.as_slice()),
                (Signal::Traces, second.as_slice())
            ]
        );
    }

    #[test]
    fn concatenated_export_requests_decode_as_one_merged_request() {
        let first = request_with("api", "first line");
        let second = request_with("worker", "second line");

        let mut bytes = first.encode_to_vec();
        bytes.extend_from_slice(&second.encode_to_vec());

        let merged = ExportLogsServiceRequest::decode(bytes.as_slice()).unwrap();

        assert_eq!(merged.resource_logs.len(), 2);
        assert_eq!(merged.resource_logs[0], first.resource_logs[0]);
        assert_eq!(merged.resource_logs[1], second.resource_logs[0]);
    }

    #[test]
    fn a_batch_survives_both_layers_at_once() {
        let requests: Vec<_> = (0..8)
            .map(|index| request_with(&format!("service-{index}"), &format!("line {index}")))
            .collect();

        let mut frames = Vec::new();
        let mut plain_len = 0;
        for request in &requests {
            let encoded = request.encode_to_vec();
            plain_len += RECORD_HEADER_BYTES + encoded.len();
            frames.extend_from_slice(&compress_record(Signal::Logs, &encoded, ZSTD_LEVEL).unwrap());
        }

        let plain = decompress_concatenated(&frames, plain_len).unwrap();
        assert_eq!(plain.len(), plain_len);
        let mut merged_bytes = Vec::new();
        for (signal, payload) in split_records(&plain).unwrap() {
            assert_eq!(signal, Signal::Logs);
            merged_bytes.extend_from_slice(payload);
        }
        let merged = ExportLogsServiceRequest::decode(merged_bytes.as_slice()).unwrap();

        assert_eq!(merged.resource_logs.len(), requests.len());
        for (index, request) in requests.iter().enumerate() {
            assert_eq!(merged.resource_logs[index], request.resource_logs[0]);
        }
    }

    #[test]
    fn a_record_that_claims_more_than_it_carries_is_refused() {
        let mut plain = Vec::new();
        plain.push(Signal::Logs.tag());
        plain.extend_from_slice(&64u32.to_le_bytes());
        plain.extend_from_slice(b"only eight");

        let error = split_records(&plain).unwrap_err();
        assert!(error.contains("claims 64 bytes"), "{error}");
    }

    #[test]
    fn a_record_under_an_unknown_tag_is_refused() {
        let mut plain = Vec::new();
        plain.push(9);
        plain.extend_from_slice(&0u32.to_le_bytes());

        let error = split_records(&plain).unwrap_err();
        assert!(error.contains("not a signal tag"), "{error}");
    }

    #[test]
    fn traces_and_metrics_carry_the_same_single_repeated_field() {
        let mut spans = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans::default()],
        }
        .encode_to_vec();
        spans.extend_from_slice(
            &ExportTraceServiceRequest {
                resource_spans: vec![ResourceSpans::default()],
            }
            .encode_to_vec(),
        );
        assert_eq!(
            ExportTraceServiceRequest::decode(spans.as_slice())
                .unwrap()
                .resource_spans
                .len(),
            2
        );

        let mut metrics = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics::default()],
        }
        .encode_to_vec();
        metrics.extend_from_slice(
            &ExportMetricsServiceRequest {
                resource_metrics: vec![ResourceMetrics::default()],
            }
            .encode_to_vec(),
        );
        assert_eq!(
            ExportMetricsServiceRequest::decode(metrics.as_slice())
                .unwrap()
                .resource_metrics
                .len(),
            2
        );
    }

    #[test]
    fn an_empty_body_is_a_successful_export_response() {
        let response = ExportLogsServiceResponse::decode([].as_slice()).unwrap();
        assert!(response.partial_success.is_none());
        assert!(response.encode_to_vec().is_empty());
    }
}
