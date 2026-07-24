fn collect_tags(spans: &[TraceSpan]) -> BTreeSet<String> {
    let mut tags = BTreeSet::new();
    for span in spans {
        tags.insert("name".to_string());
        if span.service_name().is_some() {
            tags.insert("service.name".to_string());
        }
        for attribute in &span.span.attributes {
            tags.insert(attribute.key.clone());
        }
        if let Some(resource) = &span.resource {
            for attribute in &resource.attributes {
                tags.insert(attribute.key.clone());
            }
        }
    }
    tags
}

fn collect_scoped_tags(spans: &[TraceSpan]) -> (BTreeSet<String>, BTreeSet<String>) {
    let mut resource_tags = BTreeSet::new();
    let mut span_tags = BTreeSet::new();
    for span in spans {
        span_tags.insert("name".to_string());
        for attribute in &span.span.attributes {
            span_tags.insert(attribute.key.clone());
        }
        if let Some(resource) = &span.resource {
            for attribute in &resource.attributes {
                resource_tags.insert(attribute.key.clone());
            }
        }
    }
    (resource_tags, span_tags)
}

fn parse_tags(raw: Option<&str>) -> Result<Vec<(String, String)>, String> {
    let Some(raw) = raw else {
        return Ok(Vec::new());
    };
    let raw = raw.trim().trim_start_matches('{').trim_end_matches('}');
    if raw.is_empty() {
        return Ok(Vec::new());
    }
    split_tag_tokens(raw)?
        .into_iter()
        .map(|token| {
            let (name, value) = token
                .split_once('=')
                .ok_or_else(|| format!("invalid tag filter {token:?}"))?;
            let name = name.trim().to_string();
            let value = value.trim().trim_matches('"').to_string();
            if name.is_empty() {
                return Err("tag name must not be empty".to_string());
            }
            Ok((name, value))
        })
        .collect()
}

fn split_tag_tokens(raw: &str) -> Result<Vec<String>, String> {
    let mut tokens = Vec::new();
    let mut token = String::new();
    let mut quoted = false;
    let mut escaped = false;
    for character in raw.chars() {
        if character == '"' && !escaped {
            quoted = !quoted;
        }
        if (character == ',' || character.is_whitespace()) && !quoted {
            if !token.is_empty() {
                tokens.push(std::mem::take(&mut token));
            }
        } else {
            token.push(character);
        }
        escaped = character == '\\' && !escaped;
        if character != '\\' {
            escaped = false;
        }
    }
    if quoted {
        return Err("unterminated quoted tag value".to_string());
    }
    if !token.is_empty() {
        tokens.push(token);
    }
    Ok(tokens)
}

fn tag_matches(span: &TraceSpan, name: &str, expected: &str) -> bool {
    if name == "duration" {
        if span.tag_value(name).as_deref() == Some(expected) {
            return true;
        }
        return parse_duration_ns(expected)
            .ok()
            .and_then(|duration| u64::try_from(duration).ok())
            .is_some_and(|duration| span.duration_ns() == duration);
    }
    span.tag_value(name).is_some_and(|actual| {
        actual == expected || (name == "status" && actual.eq_ignore_ascii_case(expected))
    })
}

fn parse_duration_ns(value: &str) -> Result<i64, String> {
    let value = value.trim();
    if let Ok(seconds) = value.parse::<f64>()
        && seconds.is_finite()
        && seconds >= 0.0
    {
        return (seconds * 1_000_000_000.0)
            .round()
            .to_string()
            .parse::<i64>()
            .map_err(|_| format!("duration {value:?} is out of range"));
    }
    crate::logql::parse_duration_ns(value)
}


