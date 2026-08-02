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
    // Split on a top-level binary operator before anything else, so the
    // operands are parsed as complete expressions rather than the operator
    // being mistaken for part of one.
    if let Some(binary) = parse_binary(input, depth)? {
        return Ok(binary);
    }
    for (name, op) in [
        ("sum", AggregateOp::Sum),
        ("avg", AggregateOp::Avg),
        ("min", AggregateOp::Min),
        ("max", AggregateOp::Max),
    ] {
        if let Some(mut rest) = strip_word(input, name) {
            rest = rest.trim_start();
            let (grouping, inside) = parse_aggregate_grouping(rest, name)?;
            return Ok(MetricExpr::Aggregate {
                op,
                grouping,
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

    // A subquery is a range function whose operand is `(<expr>[<range>:<step>])`
    // rather than a log selector, so it is recognised before the log-selector
    // forms below try and fail to parse the inner expression as one.
    if let Some(subquery) = parse_subquery(input, depth)? {
        return Ok(subquery);
    }

    for (name, function) in [
        ("rate", RangeFunction::Rate),
        ("count_over_time", RangeFunction::CountOverTime),
        ("bytes_over_time", RangeFunction::BytesOverTime),
        ("sum_over_time", RangeFunction::SumOverTime),
        ("avg_over_time", RangeFunction::AvgOverTime),
        ("min_over_time", RangeFunction::MinOverTime),
        ("max_over_time", RangeFunction::MaxOverTime),
        ("quantile_over_time", RangeFunction::QuantileOverTime),
    ] {
        if let Some(rest) = strip_word(input, name) {
            let (inside, tail) = take_parenthesized(rest.trim_start())?;
            if !tail.trim().is_empty() {
                return Err(format!("unexpected input after {name}"));
            }
            // `quantile_over_time(0.99, {…}[5m])` puts φ first. Split on the
            // comma before the selector so a comma inside the selector or a
            // pipeline does not confuse it.
            let (quantile, inside) = if function == RangeFunction::QuantileOverTime {
                let comma = inside
                    .find(',')
                    .ok_or("quantile_over_time expects a quantile followed by a log expression")?;
                let raw = inside[..comma].trim();
                let quantile: f64 = raw
                    .parse()
                    .map_err(|_| format!("invalid quantile '{raw}'"))?;
                if !(0.0..=1.0).contains(&quantile) {
                    return Err(format!("quantile {quantile} must be between 0 and 1"));
                }
                (Some(quantile), &inside[comma + 1..])
            } else {
                (None, inside)
            };
            // `[5m] offset 1h` — peeled before the bracket, since the offset
            // sits outside it.
            let (inside, offset_ns) = match split_offset(inside)? {
                Some((head, offset_ns)) => (head, offset_ns),
                None => (inside, 0),
            };
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
            let (query, unwrap) = parse_log_query_with_unwrap(close[..open].trim())?;
            // Refused rather than answered with nothing: a function over
            // values with no values named is a query the user got wrong, and
            // an empty result reads as "no data" instead of "no question".
            if function.needs_unwrap() && unwrap.is_none() {
                return Err(format!("{name} requires an '| unwrap <field>' stage"));
            }
            if !function.needs_unwrap() && unwrap.is_some() {
                return Err(format!("{name} does not take an 'unwrap' stage"));
            }
            return Ok(MetricExpr::Range {
                function,
                query,
                range_ns,
                unwrap,
                quantile,
                offset_ns,
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
        crate::label_name::validate_field_name(label)?;
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
        crate::label_name::validate_label_name(&name)?;
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

/// Splits a trailing `| unwrap …` off a log expression before parsing the rest
/// as a pipeline.
///
/// `unwrap` is not a pipeline stage: it selects the value a range function
/// aggregates and belongs to that function, not to the filtering. Keeping it
/// out of `PipelineStage` means a log query can never carry one, which is what
/// makes "this function needs an unwrap" checkable at parse time.
/// `by (…)` or `without (…)`, before or after the aggregated expression.
fn parse_aggregate_grouping<'a>(
    rest: &'a str,
    name: &str,
) -> Result<(Option<Grouping>, &'a str), String> {
    let rest = rest.trim_start();
    for (keyword, build) in [
        ("by", Grouping::By as fn(Vec<String>) -> Grouping),
        ("without", Grouping::Without as fn(Vec<String>) -> Grouping),
    ] {
        if let Some(after) = strip_word(rest, keyword) {
            let (names, tail) = take_parenthesized(after.trim_start())?;
            let (inside, tail) = take_parenthesized(tail.trim_start())?;
            if !tail.trim().is_empty() {
                return Err(format!("unexpected input after {name} aggregation"));
            }
            return Ok((Some(build(parse_grouping(names)?)), inside));
        }
    }
    let (inside, tail) = take_parenthesized(rest)?;
    let tail = tail.trim_start();
    for (keyword, build) in [
        ("by", Grouping::By as fn(Vec<String>) -> Grouping),
        ("without", Grouping::Without as fn(Vec<String>) -> Grouping),
    ] {
        if let Some(after) = strip_word(tail, keyword) {
            let (names, tail) = take_parenthesized(after.trim_start())?;
            if !tail.trim().is_empty() {
                return Err(format!("unexpected input after {name} aggregation"));
            }
            return Ok((Some(build(parse_grouping(names)?)), inside));
        }
    }
    if !tail.is_empty() {
        return Err(format!("unexpected input after {name} aggregation"));
    }
    Ok((None, inside))
}

/// Binary operators, lowest precedence first so the split lands on the operator
/// that binds least tightly — the same reason a recursive-descent parser peels
/// `+` before `*`.
const BINARY_OPERATORS: [(&str, BinaryOp); 11] = [
    ("==", BinaryOp::Eq),
    ("!=", BinaryOp::Neq),
    (">=", BinaryOp::Gte),
    ("<=", BinaryOp::Lte),
    (">", BinaryOp::Gt),
    ("<", BinaryOp::Lt),
    ("+", BinaryOp::Add),
    ("-", BinaryOp::Sub),
    ("*", BinaryOp::Mul),
    ("/", BinaryOp::Div),
    ("%", BinaryOp::Mod),
];

/// Splits a trailing `offset <duration>` off a range expression.
/// `<function>(<inner>[<range>:<step>] offset <d>)`.
fn parse_subquery(input: &str, depth: usize) -> Result<Option<MetricExpr>, String> {
    for (name, function) in [
        ("rate", RangeFunction::Rate),
        ("count_over_time", RangeFunction::CountOverTime),
        ("sum_over_time", RangeFunction::SumOverTime),
        ("avg_over_time", RangeFunction::AvgOverTime),
        ("min_over_time", RangeFunction::MinOverTime),
        ("max_over_time", RangeFunction::MaxOverTime),
        ("quantile_over_time", RangeFunction::QuantileOverTime),
    ] {
        let Some(rest) = strip_word(input, name) else {
            continue;
        };
        let (inside, tail) = take_parenthesized(rest.trim_start())?;
        if !tail.trim().is_empty() {
            return Err(format!("unexpected input after {name}"));
        }
        let (quantile, inside) = if function == RangeFunction::QuantileOverTime {
            let Some(comma) = inside.find(',') else {
                return Ok(None);
            };
            let raw = inside[..comma].trim();
            let Ok(quantile) = raw.parse::<f64>() else {
                return Ok(None);
            };
            (Some(quantile), &inside[comma + 1..])
        } else {
            (None, inside)
        };
        let (inside, offset_ns) = match split_offset(inside)? {
            Some((head, offset_ns)) => (head, offset_ns),
            None => (inside, 0),
        };
        let Some(close) = inside.trim_end().strip_suffix(']') else {
            return Ok(None);
        };
        let Some(open) = find_range_open(close) else {
            return Ok(None);
        };
        // The colon is what distinguishes `[5m:1m]` from `[5m]`. Without one
        // this is an ordinary range function over a log selector, which the
        // caller handles.
        let Some(colon) = close[open + 1..].find(':') else {
            return Ok(None);
        };
        let range_ns = parse_duration_ns(close[open + 1..open + 1 + colon].trim())?;
        let step = close[open + 2 + colon..].trim();
        // `[5m:]` means "the default step", which this does not guess at: a
        // step chosen for the user silently changes how many samples the outer
        // window aggregates.
        if step.is_empty() {
            return Err(format!(
                "{name} subquery requires an explicit step, as in [5m:1m]"
            ));
        }
        let step_ns = parse_duration_ns(step)?;
        if range_ns <= 0 || step_ns <= 0 {
            return Err("subquery range and step must be greater than zero".to_string());
        }
        if let Some(quantile) = quantile
            && !(0.0..=1.0).contains(&quantile)
        {
            return Err(format!("quantile {quantile} must be between 0 and 1"));
        }
        let inner = parse_metric_expr(close[..open].trim(), depth + 1)?;
        return Ok(Some(MetricExpr::Subquery {
            function,
            quantile,
            inner: Box::new(inner),
            range_ns,
            step_ns,
            offset_ns,
        }));
    }
    Ok(None)
}

fn split_offset(input: &str) -> Result<Option<(&str, i64)>, String> {
    let trimmed = input.trim_end();
    // Searched from the right: the keyword can only be the last thing, and a
    // label or line filter could contain the word anywhere else.
    let Some(position) = trimmed.rfind("offset") else {
        return Ok(None);
    };
    // Must follow the closing bracket of the range, or it is part of something
    // else — a field named `offset`, for instance.
    let head = &trimmed[..position];
    if !head.trim_end().ends_with(']') {
        return Ok(None);
    }
    let after = &trimmed[position + "offset".len()..];
    if !after.starts_with(char::is_whitespace) {
        return Ok(None);
    }
    let offset_ns = parse_duration_ns(after.trim())?;
    if offset_ns < 0 {
        return Err("offset must not be negative".to_string());
    }
    Ok(Some((head, offset_ns)))
}

fn parse_binary(input: &str, depth: usize) -> Result<Option<MetricExpr>, String> {
    let Some((position, token, op)) = find_top_level_binary(input) else {
        return Ok(None);
    };
    let left = input[..position].trim();
    let right = input[position + token.len()..].trim();
    if left.is_empty() || right.is_empty() {
        return Err(format!("binary operator '{token}' is missing an operand"));
    }
    let left_scalar = left.parse::<f64>().ok();
    let right_scalar = right.parse::<f64>().ok();
    match (left_scalar, right_scalar) {
        (None, Some(scalar)) => Ok(Some(MetricExpr::Binary {
            op,
            expr: Box::new(parse_metric_expr(left, depth + 1)?),
            scalar,
            scalar_on_left: false,
        })),
        (Some(scalar), None) => Ok(Some(MetricExpr::Binary {
            op,
            expr: Box::new(parse_metric_expr(right, depth + 1)?),
            scalar,
            scalar_on_left: true,
        })),
        (Some(_), Some(_)) => Err(
            "a binary operation between two scalars is not a metric query".to_string(),
        ),
        (None, None) => Err(format!(
            "binary operations between two metric expressions are not supported: '{token}' has a \
selector on both sides, and each side would need its own scan"
        )),
    }
}

/// The rightmost lowest-precedence operator outside brackets, quotes and
/// selectors.
///
/// Rightmost so that `a - b - c` associates left, which is what subtraction
/// requires. Skipping bracketed regions is what keeps the `-` in `[5m]`, the
/// `>` in a field filter and the `/` in a quoted path from being read as
/// operators.
fn find_top_level_binary(input: &str) -> Option<(usize, &'static str, BinaryOp)> {
    let bytes = input.as_bytes();
    let mut found: Option<(usize, &'static str, BinaryOp)> = None;
    let mut depth = 0i32;
    let mut index = 0usize;
    let mut in_string = false;
    let mut quote = b'"';
    while index < bytes.len() {
        let byte = bytes[index];
        if in_string {
            if byte == b'\\' && quote == b'"' {
                index += 2;
                continue;
            }
            if byte == quote {
                in_string = false;
            }
            index += 1;
            continue;
        }
        match byte {
            b'"' | b'`' => {
                in_string = true;
                quote = byte;
            }
            b'(' | b'[' | b'{' => depth += 1,
            b')' | b']' | b'}' => depth -= 1,
            _ => {
                if depth == 0
                    && let Some((token, op)) = BINARY_OPERATORS
                        .iter()
                        .find(|(token, _)| input[index..].starts_with(token))
                {
                    // `!=` and `=~` inside a bare selector never reach here
                    // because a selector is braced, but a comparison operator
                    // is two bytes and its first byte can also start another,
                    // so the longest match wins by table order.
                    found = Some((index, token, *op));
                    index += token.len();
                    continue;
                }
            }
        }
        index += 1;
    }
    found
}

fn parse_log_query_with_unwrap(input: &str) -> Result<(LogQuery, Option<Unwrap>), String> {
    let Some(position) = find_unwrap_stage(input) else {
        return Ok((parse_log_query(input)?, None));
    };
    let argument = input[position..].trim_start();
    let argument = argument
        .strip_prefix('|')
        .map(str::trim_start)
        .and_then(|rest| strip_word(rest, "unwrap"))
        .ok_or("malformed unwrap stage")?
        .trim();
    let unwrap = parse_unwrap(argument)?;
    Ok((parse_log_query(input[..position].trim_end())?, Some(unwrap)))
}

/// The last `| unwrap` in the expression, since anything after it would be a
/// stage the unwrap cannot see.
fn find_unwrap_stage(input: &str) -> Option<usize> {
    let bytes = input.as_bytes();
    let mut position = None;
    let mut index = 0usize;
    let mut in_string = false;
    let mut quote = b'"';
    while index < bytes.len() {
        let byte = bytes[index];
        if in_string {
            if byte == b'\\' && quote == b'"' {
                index += 2;
                continue;
            }
            if byte == quote {
                in_string = false;
            }
            index += 1;
            continue;
        }
        if byte == b'"' || byte == b'`' {
            in_string = true;
            quote = byte;
            index += 1;
            continue;
        }
        if byte == b'|' {
            let rest = input[index + 1..].trim_start();
            if strip_word(rest, "unwrap").is_some() {
                position = Some(index);
            }
        }
        index += 1;
    }
    position
}

fn parse_unwrap(argument: &str) -> Result<Unwrap, String> {
    if let Some(rest) = strip_word(argument, "duration") {
        let rest = rest.trim_start();
        let inner = rest
            .strip_prefix('(')
            .and_then(|rest| rest.strip_suffix(')'))
            .ok_or("unwrap duration expects a parenthesized field name")?;
        let field = inner.trim().to_string();
        validate_field_name(&field)?;
        return Ok(Unwrap {
            field,
            conversion: UnwrapConversion::Duration,
        });
    }
    if argument.contains('(') {
        return Err(format!(
            "unsupported unwrap conversion in '{argument}': only a bare field name and \
duration(field) are supported"
        ));
    }
    let field = argument.trim().to_string();
    validate_field_name(&field)?;
    Ok(Unwrap {
        field,
        conversion: UnwrapConversion::None,
    })
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


