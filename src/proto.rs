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
    pub fn timestamp_ns(&self) -> i64 {
        match &self.timestamp {
            Some(ts) => ts.seconds * 1_000_000_000 + ts.nanos as i64,
            None => 0,
        }
    }
}

pub fn validate_label_name(name: &str) -> Result<(), String> {
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
        map.insert(name, value);
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
}
