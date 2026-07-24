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

