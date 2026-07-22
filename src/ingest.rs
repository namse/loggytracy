use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use prost::Message;

use crate::proto::{self, PushRequest};
use crate::memtable::LogEntry;
use crate::AppState;

pub async fn push(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<StatusCode, (StatusCode, String)> {
    let content_type = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    let decompressed = if content_type.contains("application/json") {
        return Err((
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "JSON push not supported in M0, use protobuf+snappy".to_string(),
        ));
    } else {
        snap::raw::Decoder::new()
            .decompress_vec(&body)
            .map_err(|e| (StatusCode::BAD_REQUEST, format!("snappy decompress failed: {}", e)))?
    };

    let push_req = PushRequest::decode(decompressed.as_slice())
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("protobuf decode failed: {}", e)))?;

    let mut parsed: Vec<(std::collections::BTreeMap<String, String>, Vec<LogEntry>)> =
        Vec::with_capacity(push_req.streams.len());
    for stream in &push_req.streams {
        let labels = proto::parse_labels(&stream.labels)
            .map_err(|e| (StatusCode::BAD_REQUEST, format!("invalid labels '{}': {}", stream.labels, e)))?;
        let entries: Vec<LogEntry> = stream
            .entries
            .iter()
            .map(|e| LogEntry {
                timestamp_ns: e.timestamp_ns(),
                line: e.line.clone(),
                structured_metadata: e
                    .structured_metadata
                    .iter()
                    .map(|lp| (lp.name.clone(), lp.value.clone()))
                    .collect(),
            })
            .collect();
        parsed.push((labels, entries));
    }

    state
        .journal
        .append(decompressed)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("journal write failed: {}", e)))?;

    for (labels, entries) in parsed {
        state.memtable.insert(labels, entries);
    }

    Ok(StatusCode::NO_CONTENT)
}
