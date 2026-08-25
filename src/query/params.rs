/// The first-party API's shared parameter grammar (`docs/QUERY_API.md`).
///
/// One hand-written parser instead of a serde extractor, for two reasons that
/// are the same reason: axum's `Query<T>` cannot collect repeated keys, and a
/// refusal here must teach — name the offending input, list what the endpoint
/// accepts, show a correct form — which is the contract that makes the API
/// usable by an agent that only sees the error text.
pub(crate) const LOGS_PARAMS: &[&str] = &[
    "start",
    "end",
    "attr",
    "contains",
    "not_contains",
    "regex",
    "not_regex",
    "parse",
    "limit",
    "direction",
];

pub(crate) const HISTOGRAM_PARAMS: &[&str] = &[
    "start",
    "end",
    "attr",
    "contains",
    "not_contains",
    "regex",
    "not_regex",
    "parse",
    "bucket",
];

/// The first-party routes, listed by the router fallback so one wrong request
/// teaches the whole surface. Grows with each endpoint that lands.
pub(crate) const ROUTES: &[&str] = &[
    "/loggytracy/api/v1/logs",
    "/loggytracy/api/v1/logs/histogram",
];

#[derive(Debug)]
pub(crate) struct FilterParams {
    pub start_ns: Option<i64>,
    pub end_ns: Option<i64>,
    pub query: logql::LogQuery,
    pub limit: Option<usize>,
    pub forward: bool,
    pub bucket_ns: Option<i64>,
}

pub(crate) fn parse_filter_params(
    raw: &str,
    now_ns: i64,
    allowed: &'static [&'static str],
) -> Result<FilterParams, String> {
    let mut start_ns = None;
    let mut end_ns = None;
    let mut limit = None;
    let mut direction: Option<String> = None;
    let mut bucket_ns = None;
    let mut attrs: Vec<(String, logql::MatcherOp, String)> = Vec::new();
    let mut line_filters: Vec<logql::LineFilter> = Vec::new();
    let mut parse_stages: Vec<logql::PipelineStage> = Vec::new();

    for (key, value) in url::form_urlencoded::parse(raw.as_bytes()) {
        let key = key.as_ref();
        let value = value.into_owned();
        if !allowed.contains(&key) {
            return Err(format!(
                "unknown parameter '{key}': this endpoint accepts {} — see docs/QUERY_API.md",
                allowed.join(", ")
            ));
        }
        match key {
            "start" => set_once("start", &mut start_ns, parse_time_or_relative_ns(&value, now_ns)?)?,
            "end" => set_once("end", &mut end_ns, parse_time_or_relative_ns(&value, now_ns)?)?,
            "limit" => set_once(
                "limit",
                &mut limit,
                value.parse::<usize>().map_err(|_| {
                    format!("invalid limit '{value}': expected a non-negative integer")
                })?,
            )?,
            "direction" => set_once("direction", &mut direction, value)?,
            "bucket" => {
                let parsed = logql::parse_duration_ns(&value)
                    .map_err(|error| format!("invalid bucket '{value}': {error}"))?;
                if parsed <= 0 {
                    return Err(format!(
                        "invalid bucket '{value}': the width must be positive, like bucket=30s"
                    ));
                }
                set_once("bucket", &mut bucket_ns, parsed)?
            }
            "attr" => attrs.push(split_attr(&value)?),
            "contains" => line_filters.push(logql::LineFilter::Contains(value)),
            "not_contains" => line_filters.push(logql::LineFilter::NotContains(value)),
            "regex" => line_filters.push(logql::LineFilter::Regex(line_regex("regex", &value)?)),
            "not_regex" => {
                line_filters.push(logql::LineFilter::NotRegex(line_regex("not_regex", &value)?))
            }
            "parse" => {
                let stage = match value.as_str() {
                    "json" => logql::PipelineStage::Json,
                    "logfmt" => logql::PipelineStage::Logfmt,
                    other => {
                        return Err(format!(
                            "invalid parse value '{other}': expected json or logfmt"
                        ));
                    }
                };
                if parse_stages
                    .iter()
                    .any(|existing| std::mem::discriminant(existing) == std::mem::discriminant(&stage))
                {
                    return Err(format!("parse={value} was given more than once"));
                }
                parse_stages.push(stage);
            }
            _ => unreachable!("every allowed parameter is matched above"),
        }
    }

    let forward = parse_direction(&direction)?;
    let query = build_log_query(attrs, line_filters, parse_stages)?;
    Ok(FilterParams {
        start_ns,
        end_ns,
        query,
        limit,
        forward,
        bucket_ns,
    })
}

fn set_once<T>(name: &str, slot: &mut Option<T>, value: T) -> Result<(), String> {
    if slot.is_some() {
        return Err(format!("parameter '{name}' was given more than once"));
    }
    *slot = Some(value);
    Ok(())
}

/// `-1h` is one hour before now; `-123` stays a negative epoch. The unit
/// suffix is what separates them, so the two never collide.
pub(crate) fn parse_time_or_relative_ns(input: &str, now_ns: i64) -> Result<i64, String> {
    let trimmed = input.trim();
    if let Some(rest) = trimmed.strip_prefix('-')
        && rest.bytes().any(|byte| byte.is_ascii_alphabetic())
        && !rest.contains(['e', 'E'])
    {
        let duration = logql::parse_duration_ns(rest)
            .map_err(|error| format!("invalid relative time '{trimmed}': {error}"))?;
        return Ok(now_ns.saturating_sub(duration));
    }
    parse_time_ns(trimmed)
}

/// The key ends at the first operator: `!=`, `!~`, `=~`, or `=`, longest match
/// at that position. Operators are ASCII, so splitting there is always a char
/// boundary however non-ASCII the key or value.
fn split_attr(input: &str) -> Result<(String, logql::MatcherOp, String), String> {
    let bytes = input.as_bytes();
    for at in 0..bytes.len() {
        let (op, op_len) = match bytes[at] {
            b'!' if bytes.get(at + 1) == Some(&b'=') => (logql::MatcherOp::Neq, 2),
            b'!' if bytes.get(at + 1) == Some(&b'~') => (logql::MatcherOp::NRe, 2),
            b'=' if bytes.get(at + 1) == Some(&b'~') => (logql::MatcherOp::Re, 2),
            b'=' => (logql::MatcherOp::Eq, 1),
            _ => continue,
        };
        if at == 0 {
            return Err(format!(
                "attr filter '{input}' has an empty key: write attr=key=value"
            ));
        }
        return Ok((
            input[..at].to_string(),
            op,
            input[at + op_len..].to_string(),
        ));
    }
    Err(format!(
        "attr filter '{input}' has no operator: write attr=key=value (also !=, =~, !~)"
    ))
}

fn line_regex(param: &str, value: &str) -> Result<regex::Regex, String> {
    regex::Regex::new(value)
        .map_err(|error| format!("invalid {param} regex '{value}': {error}"))
}

/// Without `parse=`, `attr` filters are selector matchers — a statement about
/// the row's pushed attributes, and the position exact-field pruning reads.
/// With `parse=`, they become field filters after the parse stages, so they
/// see pushed attributes and extracted fields alike (pushed attributes shadow
/// same-named extractions — `merge_extracted`'s rule, stated in QUERY_API.md).
fn build_log_query(
    attrs: Vec<(String, logql::MatcherOp, String)>,
    line_filters: Vec<logql::LineFilter>,
    parse_stages: Vec<logql::PipelineStage>,
) -> Result<logql::LogQuery, String> {
    if parse_stages.is_empty() {
        let matchers = attrs
            .into_iter()
            .map(|(name, op, value)| logql::LabelMatcher::new(name, op, value))
            .collect::<Result<Vec<_>, _>>()?;
        return Ok(logql::LogQuery {
            matchers,
            line_filters,
            stages: Vec::new(),
        });
    }
    let mut stages = parse_stages;
    for (name, op, value) in attrs {
        let (op, value) = match op {
            logql::MatcherOp::Eq => (logql::FieldOp::Eq, logql::FieldValue::String(value)),
            logql::MatcherOp::Neq => (logql::FieldOp::Neq, logql::FieldValue::String(value)),
            logql::MatcherOp::Re => (
                logql::FieldOp::Regex,
                logql::FieldValue::Regex(anchored_regex(&value)?),
            ),
            logql::MatcherOp::NRe => (
                logql::FieldOp::NotRegex,
                logql::FieldValue::Regex(anchored_regex(&value)?),
            ),
        };
        stages.push(logql::PipelineStage::Field(logql::FieldFilter {
            name,
            op,
            value,
        }));
    }
    Ok(logql::LogQuery {
        matchers: Vec::new(),
        line_filters,
        stages,
    })
}

/// Anchored like a selector matcher's regex, so `attr=k=~v` means the same
/// thing with and without `parse=`.
fn anchored_regex(value: &str) -> Result<regex::Regex, String> {
    regex::Regex::new(&format!("^(?:{value})$"))
        .map_err(|error| format!("invalid regular expression '{value}': {error}"))
}
