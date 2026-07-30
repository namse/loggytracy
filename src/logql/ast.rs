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

pub enum PipelineStage {
    Line(LineFilter),
    Json,
    Logfmt,
    Field(FieldFilter),
    /// `| line_format "{{.status}} {{.path}}"` — rewrite the line from fields.
    LineFormat(Template),
    /// `| label_format new=old, other="{{.field}}"` — rename or synthesize
    /// fields. Loki calls them labels; at this point in the pipeline they are
    /// the same map the field filters read.
    LabelFormat(Vec<LabelFormat>),
}

#[derive(Debug, Clone)]
pub struct LabelFormat {
    pub name: String,
    pub source: LabelFormatSource,
}

#[derive(Debug, Clone)]
pub enum LabelFormatSource {
    /// `new=old`, an unquoted identifier: a rename.
    Rename(String),
    /// `new="..."`, a quoted template.
    Template(Template),
}

/// The subset of Go's text/template that Loki's format stages use in practice:
/// literal text and `{{.field}}` substitutions.
///
/// Deliberately a subset, and one that refuses rather than approximates.
/// Supporting a fragment of a template language while silently mis-rendering
/// the rest would produce log lines that look right and are not, which is worse
/// than a query that will not parse — the user can see the second one.
#[derive(Debug, Clone)]
pub struct Template {
    parts: Vec<TemplatePart>,
}

#[derive(Debug, Clone)]
enum TemplatePart {
    Literal(String),
    Field(String),
}

impl Template {
    pub fn parse(source: &str) -> Result<Self, String> {
        let mut parts = Vec::new();
        let mut rest = source;
        while let Some(open) = rest.find("{{") {
            if open > 0 {
                parts.push(TemplatePart::Literal(rest[..open].to_string()));
            }
            let after = &rest[open + 2..];
            let close = after.find("}}").ok_or_else(|| {
                "unterminated '{{' in a format template".to_string()
            })?;
            let expression = after[..close].trim();
            let field = expression.strip_prefix('.').ok_or_else(|| {
                format!(
                    "unsupported template expression '{{{{{expression}}}}}': only field \
substitutions like {{{{.name}}}} are supported"
                )
            })?;
            if field.is_empty() || !field.chars().all(|c| c.is_alphanumeric() || c == '_') {
                return Err(format!(
                    "unsupported template field '.{field}': names may contain letters, digits \
and underscores"
                ));
            }
            parts.push(TemplatePart::Field(field.to_string()));
            rest = &after[close + 2..];
        }
        if !rest.is_empty() {
            parts.push(TemplatePart::Literal(rest.to_string()));
        }
        Ok(Self { parts })
    }

    /// Render against the pipeline's current fields. A field the template names
    /// but the entry does not have renders empty, which is what Go's template
    /// does with a missing map key and what Loki therefore does.
    pub fn render(&self, fields: &BTreeMap<String, String>) -> String {
        let mut out = String::new();
        for part in &self.parts {
            match part {
                TemplatePart::Literal(text) => out.push_str(text),
                TemplatePart::Field(name) => {
                    if let Some(value) = fields.get(name) {
                        out.push_str(value);
                    }
                }
            }
        }
        out
    }

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
                // A `label_format` can synthesize any name, so a later
                // equality on that name is not a statement about what is stored
                // and must not prune a row group. Everything already collected
                // stays: those predicates were read before this stage could
                // rewrite anything.
                PipelineStage::LabelFormat(_) => return predicates,
                // `line_format` rewrites the line, not the fields, so the
                // exact-field index is unaffected by it.
                PipelineStage::LineFormat(_) => {}
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
    /// An unchanged label is not retained as query-local structured metadata;
    /// one a `label_format` rewrote is.
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
        // The names a parser stage may not extract under, because structured
        // metadata outranks an extraction of the same name on Loki. See
        // `merge_extracted`. A name that is *also* a stream label is not in
        // here: there the extraction survives as `<name>_extracted`.
        let mut shadowed_by_metadata: BTreeSet<String> = BTreeSet::new();
        for (name, value) in &entry.structured_metadata {
            // A stream label is the stable value visible to the pipeline.
            // Structured metadata with another name remains queryable.
            if !labels.contains_key(name) {
                shadowed_by_metadata.insert(name.clone());
            }
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
                        merge_extracted(
                            &mut fields,
                            &mut next_extracted_suffix,
                            &shadowed_by_metadata,
                            extracted,
                        )
                    }
                    Err(ExtractError::Parse) => {
                        set_parser_error(entry, &mut fields, "JSONParserErr");
                    }
                    Err(ExtractError::Cancelled) => return Err("query timed out".to_string()),
                },
                PipelineStage::Logfmt => {
                    match extract_logfmt_cancellable(&entry.line, cancellation) {
                        Ok(extracted) => {
                            merge_extracted(
                            &mut fields,
                            &mut next_extracted_suffix,
                            &shadowed_by_metadata,
                            extracted,
                        )
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
                // Rewrites the line the *later* stages see, not the stored one.
                // A `|=` after a `line_format` matches the formatted text,
                // which is what makes the two composable in either order.
                PipelineStage::LineFormat(template) => {
                    entry.line = template.render(&fields);
                }
                PipelineStage::LabelFormat(formats) => {
                    // Every assignment reads the field set as it was before
                    // this stage, so `a=b, b=a` swaps rather than collapsing.
                    // Loki evaluates a label_format the same way, and the
                    // alternative makes the result depend on argument order.
                    let before = fields.clone();
                    for format in formats {
                        let value = match &format.source {
                            LabelFormatSource::Rename(source) => {
                                before.get(source).cloned().unwrap_or_default()
                            }
                            LabelFormatSource::Template(template) => template.render(&before),
                        };
                        observe_extracted_name(&mut next_extracted_suffix, &format.name);
                        fields.insert(format.name.clone(), value);
                    }
                }
            }
        }
        // `entry` is a query-local clone, so retaining the evaluated fields
        // here lets metric evaluation use the same label set that filtering
        // used without changing the persisted log entry.
        //
        // A stream label is dropped only while it still *equals* the stream
        // label — carrying an identical copy would say nothing. A
        // `label_format` that rewrote one is kept, because the row's label set
        // genuinely changed and the response has to show the new value: Loki
        // 3.3.2 answers `{app="probe"} | label_format level="rewritten"` with
        // `level="rewritten"`, not with the stored value.
        entry.structured_metadata = fields
            .into_iter()
            .filter(|(name, value)| labels.get(name) != Some(value))
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
    /// Functions over unwrapped sample values rather than over entry counts.
    /// Each requires an `unwrap` stage, because there is no value to aggregate
    /// without one.
    SumOverTime,
    AvgOverTime,
    MinOverTime,
    MaxOverTime,
    QuantileOverTime,
}

impl RangeFunction {
    /// Whether this function aggregates unwrapped values. The parser uses it to
    /// reject a query that asks for one without an `unwrap`, rather than
    /// silently returning nothing.
    pub fn needs_unwrap(self) -> bool {
        matches!(
            self,
            Self::SumOverTime
                | Self::AvgOverTime
                | Self::MinOverTime
                | Self::MaxOverTime
                | Self::QuantileOverTime
        )
    }
}

/// `| unwrap latency` or `| unwrap duration(latency)` — how a field becomes a
/// number.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unwrap {
    pub field: String,
    pub conversion: UnwrapConversion,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnwrapConversion {
    /// The field is already a number.
    None,
    /// `duration(field)` — parsed as a duration and yielded in seconds, which
    /// is the unit Loki reports.
    Duration,
}

impl Unwrap {
    /// The sample value for one entry, or `None` when the field is absent or
    /// does not convert.
    ///
    /// A non-convertible entry is dropped from the sample set rather than
    /// counted as zero. Zero is a value someone will plot, and a field that
    /// failed to parse is not a measurement of zero.
    pub fn value(&self, fields: &BTreeMap<String, String>) -> Option<f64> {
        self.convert(fields.get(&self.field)?)
    }

    /// The same conversion applied to a value the caller already resolved.
    ///
    /// The metric path names exactly one field, so it resolves that field
    /// directly rather than materializing the whole field set per row.
    pub fn convert(&self, raw: &str) -> Option<f64> {
        match self.conversion {
            UnwrapConversion::None => raw.parse::<f64>().ok().filter(|value| value.is_finite()),
            UnwrapConversion::Duration => crate::logql::parse_duration_ns(raw)
                .ok()
                .map(|nanos| nanos as f64 / 1_000_000_000.0),
        }
    }
}

/// `by (a, b)` keeps those labels; `without (a, b)` keeps everything else.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Grouping {
    By(Vec<String>),
    Without(Vec<String>),
}

impl Grouping {
    /// The label set one series is grouped under.
    pub fn key(&self, labels: &Labels) -> Labels {
        match self {
            Self::By(names) => names
                .iter()
                .filter_map(|name| labels.get(name).map(|value| (name.clone(), value.clone())))
                .collect(),
            Self::Without(names) => labels
                .iter()
                .filter(|(name, _)| !names.contains(name))
                .map(|(name, value)| (name.clone(), value.clone()))
                .collect(),
        }
    }

    pub fn names(&self) -> &[String] {
        match self {
            Self::By(names) | Self::Without(names) => names,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Eq,
    Neq,
    Gt,
    Gte,
    Lt,
    Lte,
}

impl BinaryOp {
    /// Comparisons filter rather than compute: Prometheus and Loki keep the
    /// left sample when the comparison holds and drop the series when it does
    /// not, instead of yielding 1 or 0.
    #[cfg(test)]
    pub fn is_comparison(self) -> bool {
        matches!(
            self,
            Self::Eq | Self::Neq | Self::Gt | Self::Gte | Self::Lt | Self::Lte
        )
    }

    pub fn apply(self, left: f64, right: f64) -> Option<f64> {
        Some(match self {
            Self::Add => left + right,
            Self::Sub => left - right,
            Self::Mul => left * right,
            Self::Div => left / right,
            Self::Mod => left % right,
            Self::Eq => return (left == right).then_some(left),
            Self::Neq => return (left != right).then_some(left),
            Self::Gt => return (left > right).then_some(left),
            Self::Gte => return (left >= right).then_some(left),
            Self::Lt => return (left < right).then_some(left),
            Self::Lte => return (left <= right).then_some(left),
        })
    }
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
        /// Present exactly when `function.needs_unwrap()`.
        unwrap: Option<Unwrap>,
        /// The φ of `quantile_over_time(φ, ...)`, in `[0, 1]`.
        quantile: Option<f64>,
        /// `[5m] offset 1h` — shift the window back by this much.
        ///
        /// Held here rather than folded into the evaluation time because the
        /// scan range has to cover the shifted window: a query offset by an
        /// hour reads an hour further back, and the planner needs to know that
        /// before it selects parts.
        offset_ns: i64,
    },
    Aggregate {
        op: AggregateOp,
        grouping: Option<Grouping>,
        expr: Box<MetricExpr>,
    },
    /// `<aggregation>(<inner>[<range>:<step>])` — an aggregation over the
    /// values another metric expression produces at a fixed step.
    ///
    /// Held as its own node rather than desugared, because the inner
    /// expression's evaluation points are not the outer query's: the inner one
    /// is evaluated on the subquery's own step grid inside each outer window.
    Subquery {
        function: RangeFunction,
        quantile: Option<f64>,
        inner: Box<MetricExpr>,
        range_ns: i64,
        step_ns: i64,
        offset_ns: i64,
    },
    /// `expr op scalar` or `scalar op expr`.
    ///
    /// Vector-to-vector operations are not here. Both sides would be separate
    /// selectors needing separate scans, and every read path in this engine is
    /// built around one log query per metric expression — `log_query()` would
    /// stop being answerable. That is a planner change, not a parser one, so
    /// the parser refuses it rather than the evaluator half-doing it.
    Binary {
        op: BinaryOp,
        expr: Box<MetricExpr>,
        scalar: f64,
        /// `2 - rate(…)` is not `rate(…) - 2`, so which side the scalar was on
        /// has to survive parsing.
        scalar_on_left: bool,
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
            Self::Aggregate { expr, .. }
            | Self::TopK { expr, .. }
            | Self::Binary { expr, .. } => expr.log_query(),
            Self::Subquery { inner, .. } => inner.log_query(),
        }
    }

    /// How far back a query reads, counting any offset. The scan range is cut
    /// from this, so an offset that is not included here would select parts
    /// that do not contain the window being asked about.
    pub fn lookback_ns(&self) -> i64 {
        match self {
            Self::Range {
                range_ns,
                offset_ns,
                ..
            } => range_ns.saturating_add(*offset_ns),
            Self::Aggregate { expr, .. }
            | Self::TopK { expr, .. }
            | Self::Binary { expr, .. } => expr.lookback_ns(),
            // The scan has to cover the subquery's whole window plus whatever
            // the inner expression looks back from its earliest point inside
            // it, or the oldest inner evaluations read from parts that were
            // never selected.
            Self::Subquery {
                inner,
                range_ns,
                offset_ns,
                ..
            } => range_ns
                .saturating_add(*offset_ns)
                .saturating_add(inner.lookback_ns()),
        }
    }

    /// Structured fields become metric labels only when a grouping clause
    /// needs them. This keeps ordinary range functions at stream-label
    /// cardinality while preserving `sum by (field)` semantics.
    pub fn grouping_fields(&self) -> BTreeSet<String> {
        match self {
            Self::Range { .. } => BTreeSet::new(),
            Self::Aggregate { grouping, expr, .. } => {
                let mut fields = expr.grouping_fields();
                if let Some(grouping) = grouping {
                    // `without` names labels to drop, but the evaluator still
                    // has to see them to drop them, so both forms pull their
                    // names into the projected field set.
                    fields.extend(grouping.names().iter().cloned());
                }
                fields
            }
            Self::TopK { expr, .. } | Self::Binary { expr, .. } => expr.grouping_fields(),
            Self::Subquery { inner, .. } => inner.grouping_fields(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum QueryExpr {
    Logs(LogQuery),
    Metric(MetricExpr),
}

