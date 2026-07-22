use crate::memtable::Labels;
use regex::Regex;

#[derive(Debug, Clone, PartialEq)]
pub enum MatcherOp {
    Eq,
    Neq,
    Re,
    NRe,
}

#[derive(Debug, Clone)]
pub struct LabelMatcher {
    pub name: String,
    pub op: MatcherOp,
    pub value: String,
    regex: Option<Regex>,
}

impl LabelMatcher {
    pub fn new(name: String, op: MatcherOp, value: String) -> Result<Self, String> {
        let regex = match op {
            MatcherOp::Re | MatcherOp::NRe => Some(
                Regex::new(&format!("^(?:{})$", value))
                    .map_err(|e| format!("invalid regular expression '{}': {}", value, e))?,
            ),
            MatcherOp::Eq | MatcherOp::Neq => None,
        };
        Ok(Self {
            name,
            op,
            value,
            regex,
        })
    }

    pub fn matches(&self, labels: &Labels) -> bool {
        let actual = labels.get(&self.name).map(|s| s.as_str()).unwrap_or("");
        match self.op {
            MatcherOp::Eq => actual == self.value,
            MatcherOp::Neq => actual != self.value,
            MatcherOp::Re => self
                .regex
                .as_ref()
                .map(|re| re.is_match(actual))
                .unwrap_or(false),
            MatcherOp::NRe => self
                .regex
                .as_ref()
                .map(|re| !re.is_match(actual))
                .unwrap_or(true),
        }
    }
}

#[derive(Debug, Clone)]
pub enum LineFilter {
    Contains(String),
    NotContains(String),
    Regex(Regex),
    NotRegex(Regex),
}

impl LineFilter {
    pub fn matches(&self, line: &str) -> bool {
        match self {
            LineFilter::Contains(s) => line.contains(s.as_str()),
            LineFilter::NotContains(s) => !line.contains(s.as_str()),
            LineFilter::Regex(re) => re.is_match(line),
            LineFilter::NotRegex(re) => !re.is_match(line),
        }
    }
}

#[derive(Debug)]
pub struct LogQuery {
    pub matchers: Vec<LabelMatcher>,
    pub line_filters: Vec<LineFilter>,
}

pub fn parse(input: &str) -> Result<LogQuery, String> {
    let input = input.trim();
    let (matchers_str, rest) = if input.starts_with('{') {
        let end = find_matchers_close(input)
            .ok_or("closing '}' not found in label matcher")?;
        (&input[1..end], &input[end + 1..])
    } else {
        return Err("query must start with '{' label matchers".to_string());
    };

    let matchers = parse_matchers(matchers_str)?;
    let line_filters = parse_line_filters(rest)?;

    Ok(LogQuery {
        matchers,
        line_filters,
    })
}

fn find_matchers_close(input: &str) -> Option<usize> {
    let bytes = input.as_bytes();
    let mut in_quotes = false;
    let mut i = 1;
    while i < bytes.len() {
        match bytes[i] {
            b'\\' if in_quotes => {
                i += 2;
                continue;
            }
            b'"' => in_quotes = !in_quotes,
            b'}' if !in_quotes => return Some(i),
            _ => {}
        }
        i += 1;
    }
    None
}

fn parse_matchers(s: &str) -> Result<Vec<LabelMatcher>, String> {
    let s = s.trim();
    if s.is_empty() {
        return Ok(Vec::new());
    }

    let mut matchers = Vec::new();
    let mut pos = 0;
    let bytes = s.as_bytes();

    while pos < bytes.len() {
        while pos < bytes.len() && (bytes[pos].is_ascii_whitespace() || bytes[pos] == b',') {
            pos += 1;
        }
        if pos >= bytes.len() {
            break;
        }

        let name_start = pos;
        while pos < bytes.len() && bytes[pos] != b'=' && bytes[pos] != b'!' && !bytes[pos].is_ascii_whitespace() {
            pos += 1;
        }
        let name = s[name_start..pos].to_string();
        if name.is_empty() {
            return Err("empty label name in matcher".to_string());
        }
        crate::proto::validate_label_name(&name)?;

        while pos < bytes.len() && bytes[pos].is_ascii_whitespace() {
            pos += 1;
        }

        let op = if pos < bytes.len() && bytes[pos] == b'=' {
            pos += 1;
            if pos < bytes.len() && bytes[pos] == b'~' {
                pos += 1;
                MatcherOp::Re
            } else {
                MatcherOp::Eq
            }
        } else if pos < bytes.len() && bytes[pos] == b'!' {
            pos += 1;
            if pos < bytes.len() && bytes[pos] == b'=' {
                pos += 1;
                MatcherOp::Neq
            } else if pos < bytes.len() && bytes[pos] == b'~' {
                pos += 1;
                MatcherOp::NRe
            } else {
                return Err("expected '=' or '~' after '!'".to_string());
            }
        } else {
            return Err(format!("expected operator after label name '{}'", name));
        };

        while pos < bytes.len() && bytes[pos].is_ascii_whitespace() {
            pos += 1;
        }

        if pos >= bytes.len() || bytes[pos] != b'"' {
            return Err("expected '\"' to start matcher value".to_string());
        }
        pos += 1;

        let (value, new_pos) = parse_quoted_at(s, pos)?;
        pos = new_pos;

        matchers.push(LabelMatcher::new(name, op, value)?);
    }

    Ok(matchers)
}

fn parse_line_filters(s: &str) -> Result<Vec<LineFilter>, String> {
    let s = s.trim();
    if s.is_empty() {
        return Ok(Vec::new());
    }

    let mut filters = Vec::new();
    let mut pos = 0;
    let bytes = s.as_bytes();

    while pos < bytes.len() {
        while pos < bytes.len() && bytes[pos].is_ascii_whitespace() {
            pos += 1;
        }
        if pos >= bytes.len() {
            break;
        }

        let (op_len, op): (usize, MatcherOp) =
            if s[pos..].starts_with("|=") {
                (2, MatcherOp::Eq)
            } else if s[pos..].starts_with("!=") {
                (2, MatcherOp::Neq)
            } else if s[pos..].starts_with("|~") {
                (2, MatcherOp::Re)
            } else if s[pos..].starts_with("!~") {
                (2, MatcherOp::NRe)
            } else if s[pos..].starts_with('|') {
                return Err(format!(
                    "unsupported LogQL stage: '{}...' (M0 supports only |=, !=, |~, !~ line filters)",
                    &s[pos..s.len().min(pos + 20)]
                ));
            } else {
                return Err(format!(
                    "unexpected token in line filter position: '{}...'",
                    &s[pos..s.len().min(pos + 20)]
                ));
            };

        pos += op_len;
        while pos < bytes.len() && bytes[pos].is_ascii_whitespace() {
            pos += 1;
        }

        if pos >= bytes.len() || (bytes[pos] != b'"' && bytes[pos] != b'`') {
            return Err("expected quoted string after line filter operator".to_string());
        }

        let quote = bytes[pos];
        pos += 1;

        let value = if quote == b'"' {
            let (value, new_pos) = parse_quoted_at(s, pos)?;
            pos = new_pos;
            value
        } else {
            let start = pos;
            while pos < bytes.len() && bytes[pos] != b'`' {
                pos += 1;
            }
            if pos >= bytes.len() {
                return Err("unterminated backtick-quoted string".to_string());
            }
            let value = s[start..pos].to_string();
            pos += 1;
            value
        };

        filters.push(build_line_filter(op, value)?);
    }

    Ok(filters)
}

fn build_line_filter(op: MatcherOp, value: String) -> Result<LineFilter, String> {
    match op {
        MatcherOp::Eq => Ok(LineFilter::Contains(value)),
        MatcherOp::Neq => Ok(LineFilter::NotContains(value)),
        MatcherOp::Re => Ok(LineFilter::Regex(
            Regex::new(&value)
                .map_err(|e| format!("invalid regular expression '{}': {}", value, e))?,
        )),
        MatcherOp::NRe => Ok(LineFilter::NotRegex(
            Regex::new(&value)
                .map_err(|e| format!("invalid regular expression '{}': {}", value, e))?,
        )),
    }
}

fn parse_quoted_at(s: &str, start: usize) -> Result<(String, usize), String> {
    let bytes = s.as_bytes();
    let mut pos = start;
    let mut value = String::new();
    loop {
        if pos >= bytes.len() {
            return Err("unterminated quoted string".to_string());
        }
        match bytes[pos] {
            b'\\' => {
                pos += 1;
                if pos >= bytes.len() {
                    return Err("unterminated escape sequence".to_string());
                }
                match bytes[pos] {
                    b'n' => {
                        value.push('\n');
                        pos += 1;
                    }
                    b't' => {
                        value.push('\t');
                        pos += 1;
                    }
                    b'r' => {
                        value.push('\r');
                        pos += 1;
                    }
                    b'"' => {
                        value.push('"');
                        pos += 1;
                    }
                    b'\\' => {
                        value.push('\\');
                        pos += 1;
                    }
                    _ => {
                        value.push('\\');
                        let c = s[pos..].chars().next().unwrap();
                        value.push(c);
                        pos += c.len_utf8();
                    }
                }
            }
            b'"' => {
                pos += 1;
                break;
            }
            _ => {
                let c = s[pos..].chars().next().unwrap();
                value.push(c);
                pos += c.len_utf8();
            }
        }
    }
    Ok((value, pos))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matcher_regex_value_with_brace() {
        let q = parse(r#"{app=~".{3}"}"#).expect("parse");
        assert_eq!(q.matchers.len(), 1);
        assert_eq!(q.matchers[0].value, ".{3}");
        assert!(q.matchers[0].matches(
            &std::iter::once(("app".to_string(), "abc".to_string())).collect()
        ));
    }

    #[test]
    fn matcher_value_with_brace() {
        let q = parse(r#"{path="/x}y"}"#).expect("parse");
        assert_eq!(q.matchers[0].value, "/x}y");
    }

    #[test]
    fn matcher_value_with_escaped_quote_then_brace() {
        let q = parse(r#"{a="\"}"}"#).expect("parse");
        assert_eq!(q.matchers[0].value, "\"}");
    }

    #[test]
    fn matchers_followed_by_line_filter() {
        let q = parse(r#"{app="x"} |= "err" != "debug""#).expect("parse");
        assert_eq!(q.matchers.len(), 1);
        assert_eq!(q.line_filters.len(), 2);
    }

    #[test]
    fn unterminated_matchers_block() {
        assert!(parse(r#"{app=~".{3""#).is_err());
    }

    #[test]
    fn unterminated_line_filter_double_quote() {
        assert!(parse(r#"{app="x"} |= "err"#).is_err());
    }

    #[test]
    fn unterminated_line_filter_backtick() {
        assert!(parse(r#"{app="x"} |= `err"#).is_err());
    }

    #[test]
    fn rejects_invalid_matcher_label_name() {
        assert!(parse(r#"{1app="x"}"#).is_err());
        assert!(parse(r#"{app-name="x"}"#).is_err());
    }
}
