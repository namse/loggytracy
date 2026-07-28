pub fn parse(input: &str) -> Result<LogQuery, String> {
    match parse_expr(input)? {
        QueryExpr::Logs(query) => Ok(query),
        QueryExpr::Metric(_) => Err("expected a log stream expression".to_string()),
    }
}

pub fn parse_expr(input: &str) -> Result<QueryExpr, String> {
    let input = input.trim();
    if input.starts_with('{') {
        return parse_log_query(input).map(QueryExpr::Logs);
    }
    parse_metric_expr(input, 0).map(QueryExpr::Metric)
}

fn parse_metric_expr(input: &str, depth: usize) -> Result<MetricExpr, String> {
    if depth >= MAX_METRIC_AST_DEPTH {
        return Err(format!(
            "metric expression nesting exceeds the maximum of {MAX_METRIC_AST_DEPTH}"
        ));
    }
    let input = input.trim();
    for (name, op) in [
        ("sum", AggregateOp::Sum),
        ("avg", AggregateOp::Avg),
        ("min", AggregateOp::Min),
        ("max", AggregateOp::Max),
    ] {
        if let Some(mut rest) = strip_word(input, name) {
            rest = rest.trim_start();
            let (by, inside) = if let Some(after_by) = strip_word(rest, "by") {
                let (grouping, tail) = take_parenthesized(after_by.trim_start())?;
                let (inside, tail) = take_parenthesized(tail.trim_start())?;
                if !tail.trim().is_empty() {
                    return Err(format!("unexpected input after {name} aggregation"));
                }
                (Some(parse_grouping(grouping)?), inside)
            } else {
                let (inside, tail) = take_parenthesized(rest)?;
                let tail = tail.trim_start();
                if let Some(after_by) = strip_word(tail, "by") {
                    let (grouping, tail) = take_parenthesized(after_by.trim_start())?;
                    if !tail.trim().is_empty() {
                        return Err(format!("unexpected input after {name} aggregation"));
                    }
                    (Some(parse_grouping(grouping)?), inside)
                } else if !tail.is_empty() {
                    return Err(format!("unexpected input after {name} aggregation"));
                } else {
                    (None, inside)
                }
            };
            return Ok(MetricExpr::Aggregate {
                op,
                by,
                expr: Box::new(parse_metric_expr(inside, depth + 1)?),
            });
        }
    }

    if let Some(rest) = strip_word(input, "topk") {
        let (inside, tail) = take_parenthesized(rest.trim_start())?;
        if !tail.trim().is_empty() {
            return Err("unexpected input after topk".to_string());
        }
        let comma = find_top_level_comma(inside).ok_or("topk expects k and an expression")?;
        let k: usize = inside[..comma]
            .trim()
            .parse()
            .map_err(|_| "topk k must be a positive integer".to_string())?;
        if k == 0 {
            return Err("topk k must be greater than zero".to_string());
        }
        return Ok(MetricExpr::TopK {
            k,
            expr: Box::new(parse_metric_expr(&inside[comma + 1..], depth + 1)?),
        });
    }

    for (name, function) in [
        ("rate", RangeFunction::Rate),
        ("count_over_time", RangeFunction::CountOverTime),
        ("bytes_over_time", RangeFunction::BytesOverTime),
    ] {
        if let Some(rest) = strip_word(input, name) {
            let (inside, tail) = take_parenthesized(rest.trim_start())?;
            if !tail.trim().is_empty() {
                return Err(format!("unexpected input after {name}"));
            }
            let close = inside
                .trim_end()
                .strip_suffix(']')
                .ok_or_else(|| format!("{name} expects a log expression followed by [range]"))?;
            let open = find_range_open(close)
                .ok_or_else(|| format!("{name} expects a log expression followed by [range]"))?;
            let range_ns = parse_duration_ns(close[open + 1..].trim())?;
            if range_ns <= 0 {
                return Err("metric range must be greater than zero".to_string());
            }
            return Ok(MetricExpr::Range {
                function,
                query: parse_log_query(close[..open].trim())?,
                range_ns,
            });
        }
    }

    Err("unsupported LogQL metric expression".to_string())
}

fn strip_word<'a>(input: &'a str, word: &str) -> Option<&'a str> {
    input.strip_prefix(word).filter(|rest| {
        rest.chars()
            .next()
            .is_none_or(|c| c.is_ascii_whitespace() || c == '(')
    })
}

fn take_parenthesized(input: &str) -> Result<(&str, &str), String> {
    if !input.starts_with('(') {
        return Err("expected '('".to_string());
    }
    let bytes = input.as_bytes();
    let mut depth = 0usize;
    let mut quote = None;
    let mut escaped = false;
    for (index, byte) in bytes.iter().copied().enumerate() {
        if escaped {
            escaped = false;
            continue;
        }
        if quote.is_some() && byte == b'\\' {
            escaped = true;
            continue;
        }
        if byte == b'"' || byte == b'`' {
            if quote == Some(byte) {
                quote = None;
            } else if quote.is_none() {
                quote = Some(byte);
            }
            continue;
        }
        if quote.is_some() {
            continue;
        }
        match byte {
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    return Ok((&input[1..index], &input[index + 1..]));
                }
            }
            _ => {}
        }
        if depth > MAX_METRIC_AST_DEPTH {
            return Err(format!(
                "metric expression nesting exceeds the maximum of {MAX_METRIC_AST_DEPTH}"
            ));
        }
    }
    Err("unterminated parenthesized expression".to_string())
}

fn find_top_level_comma(input: &str) -> Option<usize> {
    let mut depth = 0usize;
    let mut quote = None;
    let mut escaped = false;
    for (index, byte) in input.bytes().enumerate() {
        if escaped {
            escaped = false;
            continue;
        }
        if quote == Some(b'"') && byte == b'\\' {
            escaped = true;
            continue;
        }
        match byte {
            b'"' | b'`' if quote == Some(byte) => quote = None,
            b'"' | b'`' if quote.is_none() => quote = Some(byte),
            b'(' if quote.is_none() => depth += 1,
            b')' if quote.is_none() => depth = depth.saturating_sub(1),
            b',' if quote.is_none() && depth == 0 => return Some(index),
            _ => {}
        }
    }
    None
}

fn find_range_open(input: &str) -> Option<usize> {
    let mut quote = None;
    let mut candidate = None;
    let mut escaped = false;
    for (index, byte) in input.bytes().enumerate() {
        if escaped {
            escaped = false;
            continue;
        }
        if quote == Some(b'"') && byte == b'\\' {
            escaped = true;
            continue;
        }
        match byte {
            b'"' | b'`' if quote == Some(byte) => quote = None,
            b'"' | b'`' if quote.is_none() => quote = Some(byte),
            b'[' if quote.is_none() => candidate = Some(index),
            _ => {}
        }
    }
    candidate
}

fn parse_grouping(input: &str) -> Result<Vec<String>, String> {
    let mut labels = Vec::new();
    for label in input.split(',') {
        let label = label.trim();
        if label.is_empty() {
            return Err("empty label in by grouping".to_string());
        }
        crate::proto::validate_field_name(label)?;
        labels.push(label.to_string());
    }
    labels.sort();
    labels.dedup();
    Ok(labels)
}

fn parse_log_query(input: &str) -> Result<LogQuery, String> {
    let input = input.trim();
    if !input.starts_with('{') {
        return Err("query must start with '{' label matchers".to_string());
    }
    let end = find_matchers_close(input).ok_or("closing '}' not found in label matcher")?;
    let matchers = parse_matchers(&input[1..end])?;
    let stages = parse_pipeline(&input[end + 1..])?;
    let line_filters = stages
        .iter()
        .filter_map(|stage| match stage {
            PipelineStage::Line(filter) => Some(filter.clone()),
            _ => None,
        })
        .collect();
    Ok(LogQuery {
        matchers,
        line_filters,
        stages,
    })
}

fn find_matchers_close(input: &str) -> Option<usize> {
    let bytes = input.as_bytes();
    let mut in_quotes = false;
    let mut index = 1;
    while index < bytes.len() {
        match bytes[index] {
            b'\\' if in_quotes => index += 2,
            b'"' => {
                in_quotes = !in_quotes;
                index += 1;
            }
            b'}' if !in_quotes => return Some(index),
            _ => index += 1,
        }
    }
    None
}

fn parse_matchers(input: &str) -> Result<Vec<LabelMatcher>, String> {
    let input = input.trim();
    if input.is_empty() {
        return Ok(Vec::new());
    }
    let mut matchers = Vec::new();
    let mut pos = 0;
    while pos < input.len() {
        skip_space(input, &mut pos);
        let name = parse_identifier(input, &mut pos)?;
        crate::proto::validate_label_name(&name)?;
        skip_space(input, &mut pos);
        let op = parse_matcher_op(input, &mut pos)?;
        skip_space(input, &mut pos);
        let value = parse_quoted(input, &mut pos)?;
        matchers.push(LabelMatcher::new(name, op, value)?);
        skip_space(input, &mut pos);
        if pos == input.len() {
            break;
        }
        if input.as_bytes()[pos] != b',' {
            return Err("expected ',' between label matchers".to_string());
        }
        pos += 1;
        skip_space(input, &mut pos);
        if pos == input.len() || input.as_bytes()[pos] == b',' {
            return Err("trailing or repeated ',' in label matchers".to_string());
        }
    }
    Ok(matchers)
}

fn parse_pipeline(input: &str) -> Result<Vec<PipelineStage>, String> {
    let mut stages = Vec::new();
    let mut pos = 0usize;
    while pos < input.len() {
        skip_space(input, &mut pos);
        if pos == input.len() {
            break;
        }
        let rest = &input[pos..];
        if rest.starts_with("|=")
            || rest.starts_with("!=")
            || rest.starts_with("|~")
            || rest.starts_with("!~")
        {
            let op = &rest[..2];
            pos += 2;
            skip_space(input, &mut pos);
            let value = parse_string_literal(input, &mut pos)?;
            stages.push(PipelineStage::Line(build_line_filter(op, value)?));
            continue;
        }
        if input.as_bytes()[pos] != b'|' {
            return Err(format!(
                "unexpected token in pipeline: '{}...'",
                input_preview(input, pos)
            ));
        }
        pos += 1;
        skip_space(input, &mut pos);
        if consume_word(input, &mut pos, "json") {
            stages.push(PipelineStage::Json);
            continue;
        }
        if consume_word(input, &mut pos, "logfmt") {
            stages.push(PipelineStage::Logfmt);
            continue;
        }
        if consume_word(input, &mut pos, "line_format") {
            skip_space(input, &mut pos);
            let source = parse_string_literal(input, &mut pos)?;
            stages.push(PipelineStage::LineFormat(Template::parse(&source)?));
            continue;
        }
        if consume_word(input, &mut pos, "label_format") {
            stages.push(PipelineStage::LabelFormat(parse_label_formats(
                input, &mut pos,
            )?));
            continue;
        }
        let preview = input_preview(input, pos);
        let filter = parse_field_filter(input, &mut pos)
            .map_err(|error| format!("unsupported LogQL stage '{preview}...': {error}"))?;
        stages.push(PipelineStage::Field(filter));
    }
    Ok(stages)
}

/// `label_format a=b, c="{{.d}}"` — one or more comma-separated assignments.
///
/// An unquoted right-hand side is a rename and a quoted one is a template,
/// which is Loki's distinction. Getting it from the quoting rather than from
/// the content matters: `new=old` and `new="old"` mean different things — the
/// first copies a field, the second assigns the literal text `old`.
fn parse_label_formats(input: &str, pos: &mut usize) -> Result<Vec<LabelFormat>, String> {
    let mut formats = Vec::new();
    loop {
        skip_space(input, pos);
        let name = parse_identifier(input, pos)?;
        validate_field_name(&name)?;
        skip_space(input, pos);
        if input.as_bytes().get(*pos) != Some(&b'=') {
            return Err(format!("expected '=' after label_format name '{name}'"));
        }
        *pos += 1;
        skip_space(input, pos);
        let source = if input
            .as_bytes()
            .get(*pos)
            .is_some_and(|byte| *byte == b'"' || *byte == b'`')
        {
            LabelFormatSource::Template(Template::parse(&parse_string_literal(input, pos)?)?)
        } else {
            let source = parse_identifier(input, pos)?;
            validate_field_name(&source)?;
            LabelFormatSource::Rename(source)
        };
        formats.push(LabelFormat { name, source });
        skip_space(input, pos);
        if input.as_bytes().get(*pos) == Some(&b',') {
            *pos += 1;
            continue;
        }
        break;
    }
    if formats.is_empty() {
        return Err("label_format expects at least one assignment".to_string());
    }
    Ok(formats)
}

fn parse_field_filter(input: &str, pos: &mut usize) -> Result<FieldFilter, String> {
    let name = parse_identifier(input, pos)?;
    validate_field_name(&name)?;
    skip_space(input, pos);
    let (op, op_len) = [
        ("=~", FieldOp::Regex),
        ("!~", FieldOp::NotRegex),
        ("<=", FieldOp::Lte),
        (">=", FieldOp::Gte),
        ("!=", FieldOp::Neq),
        ("=", FieldOp::Eq),
        ("<", FieldOp::Lt),
        (">", FieldOp::Gt),
    ]
    .into_iter()
    .find(|(token, _)| input[*pos..].starts_with(token))
    .map(|(token, op)| (op, token.len()))
    .ok_or("expected field comparison operator")?;
    *pos += op_len;
    skip_space(input, pos);
    let value = if input
        .as_bytes()
        .get(*pos)
        .is_some_and(|b| *b == b'"' || *b == b'`')
    {
        let value = parse_string_literal(input, pos)?;
        if matches!(op, FieldOp::Regex | FieldOp::NotRegex) {
            FieldValue::Regex(
                Regex::new(&format!("^(?:{value})$"))
                    .map_err(|error| format!("invalid regular expression '{value}': {error}"))?,
            )
        } else if matches!(op, FieldOp::Eq | FieldOp::Neq) {
            FieldValue::String(value)
        } else {
            return Err(
                "ordered field comparisons require a numeric or duration value".to_string(),
            );
        }
    } else {
        let start = *pos;
        while *pos < input.len()
            && !input.as_bytes()[*pos].is_ascii_whitespace()
            && input.as_bytes()[*pos] != b'|'
            && input.as_bytes()[*pos] != b')'
        {
            *pos += 1;
        }
        let raw = &input[start..*pos];
        if raw.is_empty() {
            return Err("expected field comparison value".to_string());
        }
        if matches!(op, FieldOp::Regex | FieldOp::NotRegex) {
            return Err("regular expression field values must be quoted".to_string());
        }
        match Decimal::parse(raw) {
            Ok(number) => FieldValue::Number(number),
            Err(_) => FieldValue::Duration(parse_duration_ns(raw)?),
        }
    };
    Ok(FieldFilter { name, op, value })
}

fn validate_field_name(name: &str) -> Result<(), String> {
    let mut bytes = name.bytes();
    let Some(first) = bytes.next() else {
        return Err("empty field name".to_string());
    };
    if !(first.is_ascii_alphabetic() || first == b'_')
        || !bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    {
        return Err(format!(
            "invalid field name '{name}': must match [a-zA-Z_][a-zA-Z0-9_]*"
        ));
    }
    Ok(())
}

fn parse_identifier(input: &str, pos: &mut usize) -> Result<String, String> {
    let start = *pos;
    while *pos < input.len() {
        let byte = input.as_bytes()[*pos];
        if byte.is_ascii_alphanumeric() || byte == b'_' || byte == b':' {
            *pos += 1;
        } else {
            break;
        }
    }
    if start == *pos {
        Err("expected identifier".to_string())
    } else {
        Ok(input[start..*pos].to_string())
    }
}

fn parse_matcher_op(input: &str, pos: &mut usize) -> Result<MatcherOp, String> {
    for (token, op) in [
        ("=~", MatcherOp::Re),
        ("!~", MatcherOp::NRe),
        ("!=", MatcherOp::Neq),
        ("=", MatcherOp::Eq),
    ] {
        if input[*pos..].starts_with(token) {
            *pos += token.len();
            return Ok(op);
        }
    }
    Err("expected label matcher operator".to_string())
}

fn consume_word(input: &str, pos: &mut usize, word: &str) -> bool {
    if !input[*pos..].starts_with(word) {
        return false;
    }
    let end = *pos + word.len();
    if input[end..]
        .chars()
        .next()
        .is_some_and(|c| c.is_ascii_alphanumeric() || c == '_')
    {
        return false;
    }
    let mut lookahead = end;
    while input
        .as_bytes()
        .get(lookahead)
        .is_some_and(u8::is_ascii_whitespace)
    {
        lookahead += 1;
    }
    if ["=~", "!~", "<=", ">=", "!=", "=", "<", ">"]
        .iter()
        .any(|operator| input[lookahead..].starts_with(operator))
    {
        return false;
    }
    *pos = end;
    true
}

fn build_line_filter(op: &str, value: String) -> Result<LineFilter, String> {
    match op {
        "|=" => Ok(LineFilter::Contains(value)),
        "!=" => Ok(LineFilter::NotContains(value)),
        "|~" => Ok(LineFilter::Regex(Regex::new(&value).map_err(|error| {
            format!("invalid regular expression '{value}': {error}")
        })?)),
        "!~" => Ok(LineFilter::NotRegex(Regex::new(&value).map_err(
            |error| format!("invalid regular expression '{value}': {error}"),
        )?)),
        _ => Err("invalid line filter operator".to_string()),
    }
}

fn parse_string_literal(input: &str, pos: &mut usize) -> Result<String, String> {
    match input.as_bytes().get(*pos) {
        Some(b'"') => parse_quoted(input, pos),
        Some(b'`') => {
            *pos += 1;
            let start = *pos;
            while *pos < input.len() && input.as_bytes()[*pos] != b'`' {
                *pos += 1;
            }
            if *pos == input.len() {
                return Err("unterminated backtick-quoted string".to_string());
            }
            let value = input[start..*pos].to_string();
            *pos += 1;
            Ok(value)
        }
        _ => Err("expected quoted string".to_string()),
    }
}

fn parse_quoted(input: &str, pos: &mut usize) -> Result<String, String> {
    if input.as_bytes().get(*pos) != Some(&b'"') {
        return Err("expected quoted string".to_string());
    }
    *pos += 1;
    let mut value = String::new();
    while *pos < input.len() {
        match input.as_bytes()[*pos] {
            b'"' => {
                *pos += 1;
                return Ok(value);
            }
            b'\\' => {
                *pos += 1;
                let escaped = input[*pos..]
                    .chars()
                    .next()
                    .ok_or("unterminated escape sequence")?;
                match escaped {
                    'n' => value.push('\n'),
                    't' => value.push('\t'),
                    'r' => value.push('\r'),
                    '"' => value.push('"'),
                    '\\' => value.push('\\'),
                    'u' => {
                        let start = (*pos).checked_add(1).ok_or("invalid unicode escape")?;
                        let end = start.checked_add(4).ok_or("invalid unicode escape")?;
                        let encoded = input.get(start..end).ok_or("invalid unicode escape")?;
                        let codepoint = u32::from_str_radix(encoded, 16)
                            .map_err(|_| "invalid unicode escape")?;
                        value.push(char::from_u32(codepoint).ok_or("invalid unicode escape")?);
                        *pos = end;
                        continue;
                    }
                    other => {
                        value.push('\\');
                        value.push(other);
                    }
                }
                *pos += escaped.len_utf8();
            }
            _ => {
                let ch = input[*pos..].chars().next().unwrap();
                value.push(ch);
                *pos += ch.len_utf8();
            }
        }
    }
    Err("unterminated quoted string".to_string())
}

fn skip_space(input: &str, pos: &mut usize) {
    while *pos < input.len() && input.as_bytes()[*pos].is_ascii_whitespace() {
        *pos += 1;
    }
}

fn input_preview(input: &str, start: usize) -> String {
    input[start..].chars().take(20).collect()
}


