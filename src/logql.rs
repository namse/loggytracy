use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};

use regex::Regex;

use crate::memtable::{Labels, LogEntry};
use crate::part::ExactFieldPredicate;

pub const PARSER_ERROR_FIELD: &str = "__error__";

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
                    .map_err(|e| format!("invalid regular expression '{value}': {e}"))?,
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
        let actual = labels.get(&self.name).map(String::as_str).unwrap_or("");
        match self.op {
            MatcherOp::Eq => actual == self.value,
            MatcherOp::Neq => actual != self.value,
            MatcherOp::Re => self.regex.as_ref().is_some_and(|re| re.is_match(actual)),
            MatcherOp::NRe => self.regex.as_ref().is_none_or(|re| !re.is_match(actual)),
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
            Self::Contains(s) => line.contains(s),
            Self::NotContains(s) => !line.contains(s),
            Self::Regex(re) => re.is_match(line),
            Self::NotRegex(re) => !re.is_match(line),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldOp {
    Eq,
    Neq,
    Regex,
    NotRegex,
    Lt,
    Lte,
    Gt,
    Gte,
}

#[derive(Debug, Clone)]
pub enum FieldValue {
    String(String),
    Regex(Regex),
    Number(Decimal),
    Duration(i64),
}

/// An exact, bounded decimal representation for field comparisons.
///
/// `f64` is intentionally not used here: values above 2^53 must retain their
/// distinct integer representations, and NaN/Inf must never participate in a
/// field predicate. The coefficient is bounded to i128 and the scale is
/// bounded to keep malformed input from causing unbounded allocations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Decimal {
    coefficient: i128,
    scale: i32,
}

const MAX_DECIMAL_SCALE: i64 = 1_024;
const MAX_METRIC_AST_DEPTH: usize = 64;

impl Decimal {
    fn parse(input: &str) -> Result<Self, ()> {
        let input = input.trim();
        if input.is_empty() {
            return Err(());
        }

        let (mantissa, exponent) = match input.find(['e', 'E']) {
            Some(index) => {
                if input[index + 1..].contains(['e', 'E']) {
                    return Err(());
                }
                let exponent = input[index + 1..].parse::<i64>().map_err(|_| ())?;
                (&input[..index], exponent)
            }
            None => (input, 0),
        };
        let (negative, mantissa) = if let Some(value) = mantissa.strip_prefix('-') {
            (true, value)
        } else if let Some(value) = mantissa.strip_prefix('+') {
            (false, value)
        } else {
            (false, mantissa)
        };

        let (integer, fraction) = match mantissa.split_once('.') {
            Some((integer, fraction)) if !fraction.contains('.') => (integer, fraction),
            Some(_) => return Err(()),
            None => (mantissa, ""),
        };
        if integer.is_empty() && fraction.is_empty() {
            return Err(());
        }
        if !integer.bytes().all(|byte| byte.is_ascii_digit())
            || !fraction.bytes().all(|byte| byte.is_ascii_digit())
        {
            return Err(());
        }

        let digits = format!("{integer}{fraction}");
        let digits = digits.trim_start_matches('0');
        if digits.is_empty() {
            return Ok(Self {
                coefficient: 0,
                scale: 0,
            });
        }
        let magnitude = digits.parse::<u128>().map_err(|_| ())?;
        let mut coefficient = if negative {
            if magnitude == i128::MAX as u128 + 1 {
                i128::MIN
            } else {
                -i128::try_from(magnitude).map_err(|_| ())?
            }
        } else {
            i128::try_from(magnitude).map_err(|_| ())?
        };
        let mut scale = i64::try_from(fraction.len())
            .map_err(|_| ())?
            .checked_sub(exponent)
            .ok_or(())?;
        if !(-MAX_DECIMAL_SCALE..=MAX_DECIMAL_SCALE).contains(&scale) {
            return Err(());
        }
        while coefficient % 10 == 0 {
            coefficient /= 10;
            scale = scale.checked_sub(1).ok_or(())?;
        }
        if !(-MAX_DECIMAL_SCALE..=MAX_DECIMAL_SCALE).contains(&scale) {
            return Err(());
        }
        Ok(Self {
            coefficient,
            scale: i32::try_from(scale).map_err(|_| ())?,
        })
    }

    fn canonical_string(&self) -> String {
        if self.coefficient == 0 {
            return "0".to_string();
        }
        let negative = self.coefficient < 0;
        let digits = self.coefficient.unsigned_abs().to_string();
        let mut output = if self.scale <= 0 {
            format!(
                "{}{}",
                digits,
                "0".repeat(self.scale.unsigned_abs() as usize)
            )
        } else if self.scale as usize >= digits.len() {
            format!(
                "0.{}{}",
                "0".repeat(self.scale as usize - digits.len()),
                digits
            )
        } else {
            let split = digits.len() - self.scale as usize;
            format!("{}.{}", &digits[..split], &digits[split..])
        };
        if negative {
            output.insert(0, '-');
        }
        output
    }

    fn cmp(&self, other: &Self) -> Ordering {
        if self.coefficient == 0 || other.coefficient == 0 {
            return self.coefficient.cmp(&other.coefficient);
        }
        let self_negative = self.coefficient < 0;
        let other_negative = other.coefficient < 0;
        if self_negative != other_negative {
            return self_negative.cmp(&other_negative).reverse();
        }

        let self_abs = self.coefficient.unsigned_abs();
        let other_abs = other.coefficient.unsigned_abs();
        let self_digits = self_abs.to_string();
        let other_digits = other_abs.to_string();
        let self_magnitude = self_digits.len() as i64 - i64::from(self.scale);
        let other_magnitude = other_digits.len() as i64 - i64::from(other.scale);
        let absolute_order = self_magnitude.cmp(&other_magnitude).then_with(|| {
            let length = self_digits.len().max(other_digits.len());
            (0..length)
                .map(|index| {
                    self_digits
                        .as_bytes()
                        .get(index)
                        .copied()
                        .unwrap_or(b'0')
                        .cmp(&other_digits.as_bytes().get(index).copied().unwrap_or(b'0'))
                })
                .find(|order| *order != Ordering::Equal)
                .unwrap_or(Ordering::Equal)
        });
        if self_negative {
            absolute_order.reverse()
        } else {
            absolute_order
        }
    }
}

/// Convert a decimal Unix timestamp expressed in seconds to exact nanoseconds.
/// Fractions finer than one nanosecond are rejected instead of being rounded by
/// an intermediate `f64`.
pub(crate) fn decimal_seconds_to_ns(input: &str) -> Result<i64, ()> {
    let decimal = Decimal::parse(input)?;
    let shift = 9i32 - decimal.scale;
    let nanoseconds = if shift >= 0 {
        let multiplier = 10_i128
            .checked_pow(u32::try_from(shift).map_err(|_| ())?)
            .ok_or(())?;
        decimal.coefficient.checked_mul(multiplier).ok_or(())?
    } else {
        let divisor = 10_i128.checked_pow(shift.unsigned_abs()).ok_or(())?;
        if decimal.coefficient % divisor != 0 {
            return Err(());
        }
        decimal.coefficient / divisor
    };
    i64::try_from(nanoseconds).map_err(|_| ())
}

#[derive(Debug, Clone)]
pub struct FieldFilter {
    pub name: String,
    pub op: FieldOp,
    pub value: FieldValue,
}

impl FieldFilter {
    fn matches(&self, fields: &BTreeMap<String, String>) -> bool {
        let actual = fields.get(&self.name).map(String::as_str).unwrap_or("");
        match (&self.op, &self.value) {
            (FieldOp::Eq, FieldValue::String(expected)) => actual == expected,
            (FieldOp::Neq, FieldValue::String(expected)) => actual != expected,
            (FieldOp::Regex, FieldValue::Regex(re)) => re.is_match(actual),
            (FieldOp::NotRegex, FieldValue::Regex(re)) => !re.is_match(actual),
            (op, FieldValue::Number(expected)) => Decimal::parse(actual)
                .ok()
                .is_some_and(|actual| compare_decimal(&actual, expected, *op)),
            (op, FieldValue::Duration(expected)) => parse_duration_ns(actual)
                .ok()
                .is_some_and(|actual| compare_ordered(actual, *expected, *op)),
            _ => false,
        }
    }
}

fn compare_decimal(actual: &Decimal, expected: &Decimal, op: FieldOp) -> bool {
    let ordering = actual.cmp(expected);
    match op {
        FieldOp::Eq => ordering == Ordering::Equal,
        FieldOp::Neq => ordering != Ordering::Equal,
        FieldOp::Lt => ordering == Ordering::Less,
        FieldOp::Lte => ordering != Ordering::Greater,
        FieldOp::Gt => ordering == Ordering::Greater,
        FieldOp::Gte => ordering != Ordering::Less,
        FieldOp::Regex | FieldOp::NotRegex => false,
    }
}

fn compare_ordered<T: PartialOrd + PartialEq>(actual: T, expected: T, op: FieldOp) -> bool {
    match op {
        FieldOp::Eq => actual == expected,
        FieldOp::Neq => actual != expected,
        FieldOp::Lt => actual < expected,
        FieldOp::Lte => actual <= expected,
        FieldOp::Gt => actual > expected,
        FieldOp::Gte => actual >= expected,
        FieldOp::Regex | FieldOp::NotRegex => false,
    }
}

#[derive(Debug, Clone)]
pub enum PipelineStage {
    Line(LineFilter),
    Json,
    Logfmt,
    Field(FieldFilter),
}

#[derive(Debug, Clone)]
pub struct LogQuery {
    pub matchers: Vec<LabelMatcher>,
    pub line_filters: Vec<LineFilter>,
    pub stages: Vec<PipelineStage>,
}

impl LogQuery {
    pub fn exact_field_predicates(&self) -> Vec<ExactFieldPredicate> {
        let mut parser_seen = false;
        let mut predicates = Vec::new();
        for stage in &self.stages {
            match stage {
                PipelineStage::Json | PipelineStage::Logfmt => parser_seen = true,
                PipelineStage::Field(FieldFilter {
                    name,
                    op: FieldOp::Eq,
                    value,
                }) if name != PARSER_ERROR_FIELD
                    && (!parser_seen || !may_be_synthesized_extracted_name(name)) =>
                {
                    let (value, canonical) = match value {
                        FieldValue::String(value) => (value.clone(), false),
                        FieldValue::Number(value) => (value.canonical_string(), true),
                        FieldValue::Duration(value) => (value.to_string(), true),
                        FieldValue::Regex(_) => continue,
                    };
                    predicates.push(if canonical {
                        ExactFieldPredicate::new_canonical_with_extraction(
                            name.clone(),
                            value,
                            parser_seen,
                        )
                    } else {
                        ExactFieldPredicate::new_with_extraction(name.clone(), value, parser_seen)
                    });
                }
                PipelineStage::Line(_) | PipelineStage::Field(_) => {}
            }
        }
        predicates
    }

    #[cfg(test)]
    pub fn matches_entry(&self, entry: &LogEntry) -> bool {
        self.process_entry(&mut entry.clone())
    }

    /// Evaluates the ordered pipeline and preserves the fields visible to the
    /// pipeline on the returned query-local entry. This includes fields
    /// extracted by json/logfmt and the synthetic `__error__` field.
    #[cfg(test)]
    pub fn process_entry(&self, entry: &mut LogEntry) -> bool {
        self.process_entry_with_labels(&BTreeMap::new(), entry)
    }

    /// Evaluates the pipeline with stream labels available as its initial
    /// field set. Labels are canonical for colliding names, so a parser
    /// extraction with the same name is retained as `<name>_extracted`.
    /// Labels themselves are not retained as query-local structured metadata.
    #[cfg(test)]
    pub fn process_entry_with_labels(&self, labels: &Labels, entry: &mut LogEntry) -> bool {
        self.process_entry_with_labels_cancellable(labels, entry, None)
            .unwrap_or(false)
    }

    pub fn process_entry_with_labels_cancellable(
        &self,
        labels: &Labels,
        entry: &mut LogEntry,
        cancellation: Option<&AtomicBool>,
    ) -> Result<bool, String> {
        let mut fields: BTreeMap<String, String> = labels.clone();
        let mut next_extracted_suffix = BTreeMap::new();
        for name in fields.keys() {
            observe_extracted_name(&mut next_extracted_suffix, name);
        }
        for (name, value) in &entry.structured_metadata {
            // A stream label is the stable value visible to the pipeline.
            // Structured metadata with another name remains queryable.
            fields.entry(name.clone()).or_insert_with(|| value.clone());
            observe_extracted_name(&mut next_extracted_suffix, name);
        }
        for stage in &self.stages {
            if cancellation.is_some_and(|flag| flag.load(AtomicOrdering::Acquire)) {
                return Err("query timed out".to_string());
            }
            match stage {
                PipelineStage::Line(filter) => {
                    if !filter.matches(&entry.line) {
                        return Ok(false);
                    }
                }
                PipelineStage::Json => match extract_json_cancellable(&entry.line, cancellation) {
                    Ok(extracted) => {
                        merge_extracted(&mut fields, &mut next_extracted_suffix, extracted)
                    }
                    Err(ExtractError::Parse) => {
                        set_parser_error(entry, &mut fields, "JSONParserErr");
                    }
                    Err(ExtractError::Cancelled) => return Err("query timed out".to_string()),
                },
                PipelineStage::Logfmt => {
                    match extract_logfmt_cancellable(&entry.line, cancellation) {
                        Ok(extracted) => {
                            merge_extracted(&mut fields, &mut next_extracted_suffix, extracted)
                        }
                        Err(ExtractError::Parse) => {
                            set_parser_error(entry, &mut fields, "LogfmtParserErr");
                        }
                        Err(ExtractError::Cancelled) => return Err("query timed out".to_string()),
                    }
                }
                PipelineStage::Field(filter) => {
                    if !filter.matches(&fields) {
                        return Ok(false);
                    }
                }
            }
        }
        // `entry` is a query-local clone, so retaining the evaluated fields
        // here lets metric evaluation use the same label set that filtering
        // used without changing the persisted log entry.
        entry.structured_metadata = fields
            .into_iter()
            .filter(|(name, _)| !labels.contains_key(name))
            .collect();
        Ok(true)
    }
}

fn may_be_synthesized_extracted_name(name: &str) -> bool {
    if name.ends_with("_extracted") {
        return true;
    }
    name.rsplit_once("_extracted_").is_some_and(|(_, suffix)| {
        !suffix.is_empty() && suffix.bytes().all(|byte| byte.is_ascii_digit())
    })
}

fn set_parser_error(entry: &mut LogEntry, fields: &mut BTreeMap<String, String>, error: &str) {
    fields.insert(PARSER_ERROR_FIELD.to_string(), error.to_string());
    entry
        .structured_metadata
        .retain(|(name, _)| name != PARSER_ERROR_FIELD);
    entry
        .structured_metadata
        .push((PARSER_ERROR_FIELD.to_string(), error.to_string()));
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RangeFunction {
    Rate,
    CountOverTime,
    BytesOverTime,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateOp {
    Sum,
    Avg,
    Min,
    Max,
}

#[derive(Debug, Clone)]
pub enum MetricExpr {
    Range {
        function: RangeFunction,
        query: LogQuery,
        range_ns: i64,
    },
    Aggregate {
        op: AggregateOp,
        by: Option<Vec<String>>,
        expr: Box<MetricExpr>,
    },
    TopK {
        k: usize,
        expr: Box<MetricExpr>,
    },
}

impl MetricExpr {
    pub fn log_query(&self) -> &LogQuery {
        match self {
            Self::Range { query, .. } => query,
            Self::Aggregate { expr, .. } | Self::TopK { expr, .. } => expr.log_query(),
        }
    }

    pub fn lookback_ns(&self) -> i64 {
        match self {
            Self::Range { range_ns, .. } => *range_ns,
            Self::Aggregate { expr, .. } | Self::TopK { expr, .. } => expr.lookback_ns(),
        }
    }

    /// Structured fields become metric labels only when a grouping clause
    /// needs them. This keeps ordinary range functions at stream-label
    /// cardinality while preserving `sum by (field)` semantics.
    pub fn grouping_fields(&self) -> BTreeSet<String> {
        match self {
            Self::Range { .. } => BTreeSet::new(),
            Self::Aggregate { by, expr, .. } => {
                let mut fields = expr.grouping_fields();
                if let Some(names) = by {
                    fields.extend(names.iter().cloned());
                }
                fields
            }
            Self::TopK { expr, .. } => expr.grouping_fields(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum QueryExpr {
    Logs(LogQuery),
    Metric(MetricExpr),
}

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
        let preview = input_preview(input, pos);
        let filter = parse_field_filter(input, &mut pos)
            .map_err(|error| format!("unsupported LogQL stage '{preview}...': {error}"))?;
        stages.push(PipelineStage::Field(filter));
    }
    Ok(stages)
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

pub fn parse_duration_ns(input: &str) -> Result<i64, String> {
    let input = input.trim();
    if input.is_empty() {
        return Err("empty duration".to_string());
    }
    let mut total = 0i64;
    let mut pos = 0usize;
    while pos < input.len() {
        let number_start = pos;
        while pos < input.len()
            && (input.as_bytes()[pos].is_ascii_digit() || input.as_bytes()[pos] == b'.')
        {
            pos += 1;
        }
        if number_start == pos {
            return Err(format!("invalid duration '{input}'"));
        }
        let number = &input[number_start..pos];
        let (unit, multiplier) = [
            ("ns", 1u128),
            ("us", 1_000u128),
            ("µs", 1_000u128),
            ("ms", 1_000_000u128),
            ("s", 1_000_000_000u128),
            ("m", 60_000_000_000u128),
            ("h", 3_600_000_000_000u128),
            ("d", 86_400_000_000_000u128),
            ("w", 604_800_000_000_000u128),
        ]
        .into_iter()
        .find(|(unit, _)| input[pos..].starts_with(unit))
        .ok_or_else(|| format!("invalid duration unit in '{input}'"))?;
        pos += unit.len();
        let component = decimal_duration_component_ns(number, multiplier, input)?;
        total = total
            .checked_add(component)
            .ok_or_else(|| format!("duration '{input}' is out of range"))?;
    }
    Ok(total)
}

fn decimal_duration_component_ns(
    number: &str,
    multiplier: u128,
    full_input: &str,
) -> Result<i64, String> {
    let (integer, fraction) = match number.split_once('.') {
        Some((integer, fraction)) if !fraction.contains('.') => (integer, fraction),
        Some(_) => return Err(format!("invalid duration '{full_input}'")),
        None => (number, ""),
    };
    if integer.is_empty() && fraction.is_empty() {
        return Err(format!("invalid duration '{full_input}'"));
    }
    if !integer.bytes().all(|byte| byte.is_ascii_digit())
        || !fraction.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(format!("invalid duration '{full_input}'"));
    }

    let significant_fraction = fraction.trim_end_matches('0');
    let scale = u32::try_from(significant_fraction.len())
        .map_err(|_| format!("duration '{full_input}' is out of range"))?;
    let divisor = 10u128
        .checked_pow(scale)
        .ok_or_else(|| format!("duration '{full_input}' is out of range"))?;
    let integer_value = if integer.is_empty() {
        0
    } else {
        integer
            .parse::<u128>()
            .map_err(|_| format!("duration '{full_input}' is out of range"))?
    };
    let fraction_value = if significant_fraction.is_empty() {
        0
    } else {
        significant_fraction
            .parse::<u128>()
            .map_err(|_| format!("duration '{full_input}' is out of range"))?
    };
    let decimal_numerator = integer_value
        .checked_mul(divisor)
        .and_then(|value| value.checked_add(fraction_value))
        .ok_or_else(|| format!("duration '{full_input}' is out of range"))?;
    let scaled = decimal_numerator
        .checked_mul(multiplier)
        .ok_or_else(|| format!("duration '{full_input}' is out of range"))?;
    let truncated = scaled / divisor;
    i64::try_from(truncated).map_err(|_| format!("duration '{full_input}' is out of range"))
}

fn merge_extracted(
    fields: &mut BTreeMap<String, String>,
    next_suffix: &mut BTreeMap<String, usize>,
    extracted: BTreeMap<String, String>,
) {
    for (name, value) in extracted {
        insert_extracted_with_counter(fields, next_suffix, name, value);
    }
}

/// Insert one parser-produced field without losing a value when sanitization
/// or a previous parser maps two names to the same LogQL identifier.
fn insert_extracted_with_counter(
    fields: &mut BTreeMap<String, String>,
    next_suffix: &mut BTreeMap<String, usize>,
    name: String,
    value: String,
) {
    if let std::collections::btree_map::Entry::Vacant(entry) = fields.entry(name.clone()) {
        entry.insert(value);
        observe_extracted_name(next_suffix, &name);
        return;
    }

    let base = format!("{name}_extracted");
    let suffix = next_suffix.entry(base.clone()).or_insert(2);
    let mut candidate = base.clone();
    while fields.contains_key(&candidate) {
        candidate = format!("{base}_{}", *suffix);
        *suffix = (*suffix).saturating_add(1);
    }
    fields.insert(candidate, value);
}

fn observe_extracted_name(next_suffix: &mut BTreeMap<String, usize>, name: &str) {
    if let Some((base, suffix)) = name.rsplit_once("_extracted_")
        && let Ok(suffix) = suffix.parse::<usize>()
    {
        let base = format!("{base}_extracted");
        let next = suffix.saturating_add(1).max(2);
        next_suffix
            .entry(base)
            .and_modify(|current| *current = (*current).max(next))
            .or_insert(next);
    } else if name.ends_with("_extracted") {
        next_suffix.entry(name.to_string()).or_insert(2);
    }
}

fn extract_json(line: &str) -> Result<BTreeMap<String, String>, ()> {
    extract_json_cancellable(line, None).map_err(|_| ())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExtractError {
    Parse,
    Cancelled,
}

fn extract_json_cancellable(
    line: &str,
    cancellation: Option<&AtomicBool>,
) -> Result<BTreeMap<String, String>, ExtractError> {
    if cancellation.is_some_and(|flag| flag.load(AtomicOrdering::Acquire)) {
        return Err(ExtractError::Cancelled);
    }
    // serde_json's parser is not cancellation-aware. Check between chunks so
    // a cancelled query does not spend time entering a large parse, and check
    // again before returning the materialized object.
    for chunk in line.as_bytes().chunks(4096) {
        let _ = chunk;
        if cancellation.is_some_and(|flag| flag.load(AtomicOrdering::Acquire)) {
            return Err(ExtractError::Cancelled);
        }
    }
    let value: serde_json::Value = serde_json::from_str(line).map_err(|_| ExtractError::Parse)?;
    if cancellation.is_some_and(|flag| flag.load(AtomicOrdering::Acquire)) {
        return Err(ExtractError::Cancelled);
    }
    let serde_json::Value::Object(object) = value else {
        return Err(ExtractError::Parse);
    };
    let mut fields = BTreeMap::new();
    let mut next_suffix: BTreeMap<String, usize> = BTreeMap::new();
    flatten_json("", &object, &mut fields, &mut next_suffix, cancellation)?;
    Ok(fields)
}

fn flatten_json(
    prefix: &str,
    object: &serde_json::Map<String, serde_json::Value>,
    fields: &mut BTreeMap<String, String>,
    next_suffix: &mut BTreeMap<String, usize>,
    cancellation: Option<&AtomicBool>,
) -> Result<(), ExtractError> {
    let mut entries: Vec<_> = object.iter().collect();
    entries.sort_by(|left, right| left.0.cmp(right.0));
    for (name, value) in entries {
        if cancellation.is_some_and(|flag| flag.load(AtomicOrdering::Acquire)) {
            return Err(ExtractError::Cancelled);
        }
        let name = sanitize_field_name(name);
        let path = if prefix.is_empty() {
            name
        } else {
            format!("{prefix}_{name}")
        };
        match value {
            serde_json::Value::String(value) => {
                insert_extracted_with_counter(fields, next_suffix, path, value.clone());
            }
            serde_json::Value::Bool(value) => {
                insert_extracted_with_counter(fields, next_suffix, path, value.to_string());
            }
            serde_json::Value::Number(value) => {
                insert_extracted_with_counter(fields, next_suffix, path, value.to_string());
            }
            serde_json::Value::Object(nested) => {
                flatten_json(&path, nested, fields, next_suffix, cancellation)?;
            }
            serde_json::Value::Null | serde_json::Value::Array(_) => {}
        }
    }
    Ok(())
}

fn sanitize_field_name(name: &str) -> String {
    let mut output = String::new();
    for (index, ch) in name.chars().enumerate() {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            if index == 0 && ch.is_ascii_digit() {
                output.push('_');
            }
            output.push(ch);
        } else {
            output.push('_');
        }
    }
    if output.is_empty() {
        "_".to_string()
    } else {
        output
    }
}

fn extract_logfmt(line: &str) -> Result<BTreeMap<String, String>, ()> {
    extract_logfmt_cancellable(line, None).map_err(|_| ())
}

fn extract_logfmt_cancellable(
    line: &str,
    cancellation: Option<&AtomicBool>,
) -> Result<BTreeMap<String, String>, ExtractError> {
    let mut fields = BTreeMap::new();
    let mut source_keys: BTreeMap<String, String> = BTreeMap::new();
    let mut next_suffix: BTreeMap<String, usize> = BTreeMap::new();
    let mut pos = 0usize;
    while pos < line.len() {
        if cancellation.is_some_and(|flag| flag.load(AtomicOrdering::Acquire)) {
            return Err(ExtractError::Cancelled);
        }
        skip_space(line, &mut pos);
        if pos == line.len() {
            break;
        }
        let key_start = pos;
        while pos < line.len()
            && !line.as_bytes()[pos].is_ascii_whitespace()
            && line.as_bytes()[pos] != b'='
        {
            if cancellation.is_some_and(|flag| flag.load(AtomicOrdering::Acquire)) {
                return Err(ExtractError::Cancelled);
            }
            pos += 1;
        }
        if key_start == pos || line.as_bytes().get(pos) != Some(&b'=') {
            return Err(ExtractError::Parse);
        }
        let raw_key = line[key_start..pos].to_string();
        let key = sanitize_field_name(&raw_key);
        pos += 1;
        let value = if line.as_bytes().get(pos) == Some(&b'"') {
            if cancellation.is_some_and(|flag| flag.load(AtomicOrdering::Acquire)) {
                return Err(ExtractError::Cancelled);
            }
            let value = match parse_logfmt_quoted(line, &mut pos, cancellation) {
                Ok(value) => value,
                Err(()) if cancellation.is_some_and(|flag| flag.load(AtomicOrdering::Acquire)) => {
                    return Err(ExtractError::Cancelled);
                }
                Err(()) => return Err(ExtractError::Parse),
            };
            if pos < line.len() && !line.as_bytes()[pos].is_ascii_whitespace() {
                return Err(ExtractError::Parse);
            }
            value
        } else {
            let start = pos;
            while pos < line.len() && !line.as_bytes()[pos].is_ascii_whitespace() {
                if cancellation.is_some_and(|flag| flag.load(AtomicOrdering::Acquire)) {
                    return Err(ExtractError::Cancelled);
                }
                pos += 1;
            }
            line[start..pos].to_string()
        };
        // Repeated occurrences of the same logfmt key retain the usual
        // last-value-wins behavior. Distinct raw keys that sanitize to the
        // same identifier are preserved under a deterministic suffix.
        if let Some(storage_key) = source_keys.get(&raw_key) {
            fields.insert(storage_key.clone(), value);
        } else {
            let mut storage_key = key;
            if fields.contains_key(&storage_key) {
                let base = format!("{storage_key}_extracted");
                let suffix = next_suffix.entry(base.clone()).or_insert(2);
                if fields.contains_key(&base) {
                    storage_key = format!("{base}_{}", *suffix);
                    *suffix = (*suffix).saturating_add(1);
                } else {
                    storage_key = base;
                }
            }
            let inserted_key = storage_key.clone();
            source_keys.insert(raw_key, inserted_key.clone());
            fields.insert(inserted_key.clone(), value);
            observe_extracted_name(&mut next_suffix, &inserted_key);
        }
    }
    Ok(fields)
}

fn parse_logfmt_quoted(
    input: &str,
    pos: &mut usize,
    cancellation: Option<&AtomicBool>,
) -> Result<String, ()> {
    if input.as_bytes().get(*pos) != Some(&b'"') {
        return Err(());
    }
    *pos += 1;
    let mut value = String::new();
    while *pos < input.len() {
        if cancellation.is_some_and(|flag| flag.load(AtomicOrdering::Acquire)) {
            return Err(());
        }
        match input.as_bytes()[*pos] {
            b'"' => {
                *pos += 1;
                return Ok(value);
            }
            b'\\' => {
                *pos += 1;
                let escape = *input.as_bytes().get(*pos).ok_or(())?;
                match escape {
                    b'a' => {
                        value.push('\u{7}');
                        *pos += 1;
                    }
                    b'b' => {
                        value.push('\u{8}');
                        *pos += 1;
                    }
                    b'f' => {
                        value.push('\u{c}');
                        *pos += 1;
                    }
                    b'n' => {
                        value.push('\n');
                        *pos += 1;
                    }
                    b'r' => {
                        value.push('\r');
                        *pos += 1;
                    }
                    b't' => {
                        value.push('\t');
                        *pos += 1;
                    }
                    b'v' => {
                        value.push('\u{b}');
                        *pos += 1;
                    }
                    b'"' => {
                        value.push('"');
                        *pos += 1;
                    }
                    b'\\' => {
                        value.push('\\');
                        *pos += 1;
                    }
                    b'x' => {
                        *pos += 1;
                        let codepoint = parse_logfmt_escape_digits(input, pos, 2, 16)?;
                        if codepoint > u8::MAX as u32 {
                            return Err(());
                        }
                        value.push(char::from_u32(codepoint).ok_or(())?);
                    }
                    b'u' => {
                        *pos += 1;
                        let codepoint = parse_logfmt_escape_digits(input, pos, 4, 16)?;
                        value.push(char::from_u32(codepoint).ok_or(())?);
                    }
                    b'U' => {
                        *pos += 1;
                        let codepoint = parse_logfmt_escape_digits(input, pos, 8, 16)?;
                        value.push(char::from_u32(codepoint).ok_or(())?);
                    }
                    b'0'..=b'7' => {
                        let codepoint = parse_logfmt_escape_digits(input, pos, 3, 8)?;
                        if codepoint > u8::MAX as u32 {
                            return Err(());
                        }
                        value.push(char::from_u32(codepoint).ok_or(())?);
                    }
                    _ => return Err(()),
                }
            }
            _ => {
                let ch = input[*pos..].chars().next().ok_or(())?;
                value.push(ch);
                *pos += ch.len_utf8();
            }
        }
    }
    Err(())
}

fn parse_logfmt_escape_digits(
    input: &str,
    pos: &mut usize,
    digits: usize,
    radix: u32,
) -> Result<u32, ()> {
    let end = pos.checked_add(digits).ok_or(())?;
    let encoded = input.get(*pos..end).ok_or(())?;
    if !encoded.is_ascii() {
        return Err(());
    }
    let value = u32::from_str_radix(encoded, radix).map_err(|_| ())?;
    *pos = end;
    Ok(value)
}

/// Values represented in the BTF2 exact-field bloom. The union is deliberate:
/// it permits conservative push-down after either parser without making the
/// immutable part format depend on a query's particular pipeline ordering.
pub(crate) fn indexed_parser_fields(line: &str) -> BTreeMap<String, Vec<String>> {
    let mut indexed: BTreeMap<String, Vec<String>> = BTreeMap::new();
    // Avoid invoking both parsers for ordinary plain-text lines. Parser error
    // fields are intentionally not indexed: exact_field_predicates excludes
    // __error__, so indexing those values only adds work and memory.
    if line.trim_start().starts_with('{')
        && let Ok(fields) = extract_json(line)
    {
        for (name, value) in fields {
            indexed.entry(name).or_default().push(value);
        }
    }
    if line.contains('=')
        && let Ok(fields) = extract_logfmt(line)
    {
        for (name, value) in fields {
            indexed.entry(name).or_default().push(value);
        }
    }
    indexed
}

/// Returns the representations that the exact-field bloom stores for one
/// scalar value. The raw representation preserves string equality, while the
/// canonical numeric and duration forms make typed equality prune safely.
pub(crate) fn canonical_index_values(value: &str) -> Vec<String> {
    let mut values = vec![value.to_string()];
    if let Ok(number) = Decimal::parse(value) {
        values.push(number.canonical_string());
    }
    if let Ok(duration) = parse_duration_ns(value) {
        values.push(duration.to_string());
    }
    values.sort();
    values.dedup();
    values
}

#[cfg(test)]
mod tests {
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
        let MetricExpr::Aggregate { by, .. } = expr else {
            panic!("expected aggregate")
        };
        assert_eq!(by, Some(vec!["__error__".to_string(), "level".to_string()]));
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
        assert!(parse(r#"{app="x"} | line_format "{{.app}}""#).is_err());
        assert!(parse_expr(r#"quantile_over_time(0.9, {}[5m])"#).is_err());
        assert!(parse_expr(r#"rate({}[0s])"#).is_err());
    }

    #[test]
    fn unsupported_unicode_stage_returns_error_instead_of_panicking() {
        assert!(parse("{} |가가가가가가가").is_err());
        assert!(parse("{} 가가가가가가가").is_err());
        let query = parse(r#"{app="\가"}"#).unwrap();
        assert_eq!(query.matchers[0].value, r#"\가"#);
    }
}
