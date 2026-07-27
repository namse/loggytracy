#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PushRequest {
    #[prost(message, repeated, tag = "1")]
    pub streams: Vec<StreamAdapter>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StreamAdapter {
    #[prost(string, tag = "1")]
    pub labels: String,
    #[prost(message, repeated, tag = "2")]
    pub entries: Vec<EntryAdapter>,
    #[prost(uint64, tag = "3")]
    pub hash: u64,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LabelPairAdapter {
    #[prost(string, tag = "1")]
    pub name: String,
    #[prost(string, tag = "2")]
    pub value: String,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EntryAdapter {
    #[prost(message, optional, tag = "1")]
    pub timestamp: Option<::prost_types::Timestamp>,
    #[prost(string, tag = "2")]
    pub line: String,
    #[prost(message, repeated, tag = "3")]
    pub structured_metadata: Vec<LabelPairAdapter>,
}

impl EntryAdapter {
    pub fn timestamp_ns(&self) -> Result<i64, String> {
        match &self.timestamp {
            Some(ts) => {
                if !(0..1_000_000_000).contains(&ts.nanos) {
                    return Err(format!(
                        "timestamp nanos must be between 0 and 999999999, got {}",
                        ts.nanos
                    ));
                }
                let nanos = i128::from(ts.seconds) * 1_000_000_000 + i128::from(ts.nanos);
                i64::try_from(nanos)
                    .map_err(|_| "timestamp is outside the supported nanosecond range".to_string())
            }
            None => Ok(0),
        }
    }
}

pub fn validate_label_name(name: &str) -> Result<(), String> {
    validate_field_name(name)?;
    // Stream labels are persisted as Parquet columns, so these storage column
    // names remain reserved even though LogQL pipeline fields may use them.
    match name {
        "_msg" | "_tenant" | "timestamp_ns" | "structured_metadata" => Err(format!(
            "invalid label name '{}': reserved column name",
            name
        )),
        _ => Ok(()),
    }
}

pub fn validate_field_name(name: &str) -> Result<(), String> {
    let bytes = name.as_bytes();
    if bytes.is_empty() {
        return Err("empty label name".to_string());
    }
    let first = bytes[0];
    if !(first.is_ascii_alphabetic() || first == b'_') {
        return Err(format!(
            "invalid label name '{}': must match [a-zA-Z_][a-zA-Z0-9_]*",
            name
        ));
    }
    for &b in &bytes[1..] {
        if !(b.is_ascii_alphanumeric() || b == b'_') {
            return Err(format!(
                "invalid label name '{}': must match [a-zA-Z_][a-zA-Z0-9_]*",
                name
            ));
        }
    }
    Ok(())
}

/// Encode normalized streams as the `PushRequest` the journal stores.
pub fn encode_push_request(
    streams: &[(
        std::collections::BTreeMap<String, String>,
        Vec<crate::memtable::LogEntry>,
    )],
) -> Vec<u8> {
    use prost::Message;

    let request = PushRequest {
        streams: streams
            .iter()
            .map(|(labels, entries)| StreamAdapter {
                labels: format_labels(labels),
                entries: entries
                    .iter()
                    .map(|entry| EntryAdapter {
                        timestamp: Some(::prost_types::Timestamp {
                            seconds: entry.timestamp_ns.div_euclid(1_000_000_000),
                            nanos: entry.timestamp_ns.rem_euclid(1_000_000_000) as i32,
                        }),
                        line: entry.line.clone(),
                        structured_metadata: entry
                            .structured_metadata
                            .iter()
                            .map(|(name, value)| LabelPairAdapter {
                                name: name.clone(),
                                value: value.clone(),
                            })
                            .collect(),
                    })
                    .collect(),
                hash: 0,
            })
            .collect(),
    };
    request.encode_to_vec()
}

/// Render a label set back into the wire form `parse_labels` reads.
///
/// The journal stores a log record as a Loki `PushRequest` whatever protocol
/// it arrived on, so replay has exactly one decoder. That makes this the
/// inverse of `parse_labels` and it has to escape everything that function
/// unescapes — a label value with a quote in it would otherwise produce a
/// record that replays as a parse error, which is a WAL that cannot be read.
pub fn format_labels(labels: &std::collections::BTreeMap<String, String>) -> String {
    let mut out = String::from("{");
    for (index, (name, value)) in labels.iter().enumerate() {
        if index > 0 {
            out.push(',');
        }
        out.push_str(name);
        out.push_str("=\"");
        for character in value.chars() {
            match character {
                '"' => out.push_str("\\\""),
                '\\' => out.push_str("\\\\"),
                '\n' => out.push_str("\\n"),
                '\t' => out.push_str("\\t"),
                '\r' => out.push_str("\\r"),
                other => out.push(other),
            }
        }
        out.push('"');
    }
    out.push('}');
    out
}

pub fn parse_labels(s: &str) -> Result<std::collections::BTreeMap<String, String>, String> {
    let s = s.trim();
    let s = s
        .strip_prefix('{')
        .ok_or_else(|| "labels must start with '{'".to_string())?;
    let s = s
        .strip_suffix('}')
        .ok_or_else(|| "labels must end with '}'".to_string())?;
    let mut map = std::collections::BTreeMap::new();
    let mut chars = s.chars().peekable();
    loop {
        while chars.peek().is_some_and(|c| c.is_whitespace() || *c == ',') {
            chars.next();
        }
        if chars.peek().is_none() {
            break;
        }
        let mut name = String::new();
        while let Some(&c) = chars.peek() {
            if c == '=' {
                break;
            }
            name.push(c);
            chars.next();
        }
        let name = name.trim().to_string();
        if name.is_empty() {
            return Err("empty label name".to_string());
        }
        validate_label_name(&name)?;
        match chars.peek() {
            Some(&'=') => {
                chars.next();
            }
            _ => return Err(format!("expected '=' after label name '{}'", name)),
        }
        while chars.peek().is_some_and(|c| c.is_whitespace()) {
            chars.next();
        }
        match chars.next() {
            Some('"') => {}
            _ => return Err(format!("expected '\"' to start value for label '{}'", name)),
        }
        let mut value = String::new();
        loop {
            match chars.next() {
                Some('\\') => match chars.next() {
                    Some('n') => value.push('\n'),
                    Some('t') => value.push('\t'),
                    Some('r') => value.push('\r'),
                    Some('"') => value.push('"'),
                    Some('\\') => value.push('\\'),
                    Some(c) => {
                        value.push('\\');
                        value.push(c);
                    }
                    None => return Err("unterminated escape in label value".to_string()),
                },
                Some('"') => break,
                Some(c) => value.push(c),
                None => return Err(format!("unterminated value for label '{}'", name)),
            }
        }
        if map.insert(name.clone(), value).is_some() {
            return Err(format!("duplicate label name '{}'", name));
        }
        while chars.peek().is_some_and(|c| c.is_whitespace()) {
            chars.next();
        }
        match chars.peek() {
            None | Some(&',') => {}
            Some(&c) => return Err(format!("expected ',' after label value, found '{}'", c)),
        }
    }
    Ok(map)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_valid_labels() {
        let m = parse_labels(r#"{app="test-app", host="local"}"#).expect("parse");
        assert_eq!(m.len(), 2);
        assert_eq!(m.get("app").map(|s| s.as_str()), Some("test-app"));
        assert_eq!(m.get("host").map(|s| s.as_str()), Some("local"));
    }

    #[test]
    fn rejects_unquoted_value() {
        assert!(parse_labels(r#"{app=plain}"#).is_err());
    }

    #[test]
    fn rejects_missing_comma() {
        assert!(parse_labels(r#"{app="a" host="b"}"#).is_err());
    }

    #[test]
    fn rejects_duplicate_label_name() {
        assert!(parse_labels(r#"{app="a", app="b"}"#).is_err());
    }

    #[test]
    fn rejects_unterminated_value() {
        assert!(parse_labels(r#"{app="unterminated}"#).is_err());
    }

    #[test]
    fn rejects_missing_braces() {
        assert!(parse_labels(r#"app="a""#).is_err());
    }

    #[test]
    fn decodes_escapes() {
        let m = parse_labels(r#"{msg="a\"b\\c"}"#).expect("parse");
        assert_eq!(m.get("msg").map(|s| s.as_str()), Some(r#"a"b\c"#));
    }

    #[test]
    fn rejects_invalid_label_name_starting_digit() {
        assert!(parse_labels(r#"{1app="x"}"#).is_err());
    }

    #[test]
    fn rejects_invalid_label_name_with_dash() {
        assert!(parse_labels(r#"{app-name="x"}"#).is_err());
    }

    #[test]
    fn accepts_label_name_with_underscore() {
        assert!(parse_labels(r#"{_app="x", app_2="y"}"#).is_ok());
    }

    #[test]
    fn rejects_reserved_column_names() {
        assert!(validate_label_name("_msg").is_err());
        assert!(validate_label_name("_tenant").is_err());
        assert!(validate_label_name("timestamp_ns").is_err());
        assert!(validate_label_name("structured_metadata").is_err());
    }

    #[test]
    fn parse_labels_rejects_reserved_names() {
        assert!(parse_labels(r#"{_msg="hi"}"#).is_err());
        assert!(parse_labels(r#"{timestamp_ns="123"}"#).is_err());
        assert!(parse_labels(r#"{structured_metadata="x"}"#).is_err());
    }

    #[test]
    fn timestamp_ns_accepts_i64_boundaries() {
        let max = EntryAdapter {
            timestamp: Some(prost_types::Timestamp {
                seconds: 9_223_372_036,
                nanos: 854_775_807,
            }),
            line: String::new(),
            structured_metadata: Vec::new(),
        };
        assert_eq!(max.timestamp_ns().unwrap(), i64::MAX);

        let min = EntryAdapter {
            timestamp: Some(prost_types::Timestamp {
                seconds: -9_223_372_037,
                nanos: 145_224_192,
            }),
            line: String::new(),
            structured_metadata: Vec::new(),
        };
        assert_eq!(min.timestamp_ns().unwrap(), i64::MIN);
    }

    #[test]
    fn timestamp_ns_rejects_overflow_and_invalid_nanos() {
        let overflow = EntryAdapter {
            timestamp: Some(prost_types::Timestamp {
                seconds: 9_223_372_037,
                nanos: 0,
            }),
            line: String::new(),
            structured_metadata: Vec::new(),
        };
        assert!(overflow.timestamp_ns().is_err());

        let invalid_nanos = EntryAdapter {
            timestamp: Some(prost_types::Timestamp {
                seconds: 0,
                nanos: -1,
            }),
            line: String::new(),
            structured_metadata: Vec::new(),
        };
        assert!(invalid_nanos.timestamp_ns().is_err());
    }
}
