/// The first-party autocomplete endpoints (`docs/QUERY_API.md`): attribute
/// keys in a window, and a key's values, both bounded — keys come from the
/// memtable and the part metadata census, values from the newest
/// `METADATA_SAMPLE_ROWS` rows in the window rather than from a catalog.
/// Rare or old values may not appear over a long range; the doc says so.
pub async fn logs_attributes(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    RawQuery(raw): RawQuery,
) -> Result<axum::response::Response, ApiError> {
    let tenant = crate::tenant::from_headers(&headers, &state.config, &state.tenant_policy)
        .map_err(ApiError::from_tenant)?;
    let params = attribute_window_params(&state, raw.as_deref().unwrap_or(""), ATTRIBUTE_KEYS_PARAMS)?;
    let Some(guard) = MetadataGuard::acquire_window(&state, &tenant, params.window)
        .await
        .map_err(|(status, message)| ApiError(status, message))?
    else {
        return Ok(ndjson_response(String::new(), 0, 0));
    };

    let mut names = std::collections::BTreeSet::new();
    for name in state.memtable.label_names(&tenant, guard.window) {
        names.insert(name);
    }
    guard
        .check_deadline()
        .map_err(|(status, message)| ApiError(status, message))?;
    for name in state.parts.metadata_key_names(&tenant, guard.window) {
        names.insert(name);
    }

    let mut body = String::new();
    for name in names {
        body.push_str(
            &serde_json::to_string(&serde_json::json!({ "key": name }))
                .expect("a key serializes infallibly"),
        );
        body.push('\n');
    }
    Ok(ndjson_response(body, 0, 0))
}

pub async fn logs_attribute_values(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(key): Path<String>,
    RawQuery(raw): RawQuery,
) -> Result<axum::response::Response, ApiError> {
    let tenant = crate::tenant::from_headers(&headers, &state.config, &state.tenant_policy)
        .map_err(ApiError::from_tenant)?;
    let params =
        attribute_window_params(&state, raw.as_deref().unwrap_or(""), ATTRIBUTE_VALUES_PARAMS)?;
    let Some(guard) = MetadataGuard::acquire_window(&state, &tenant, params.window)
        .await
        .map_err(|(status, message)| ApiError(status, message))?
    else {
        return Ok(ndjson_response(String::new(), 0, 0));
    };

    let mut values = std::collections::BTreeSet::new();
    if params.matchers.is_empty() {
        for value in state.memtable.label_values(&tenant, &key, guard.window) {
            values.insert(value);
        }
        guard
            .check_deadline()
            .map_err(|(status, message)| ApiError(status, message))?;
        for metadata in state
            .parts
            .sample_metadata(&tenant, guard.window, METADATA_SAMPLE_ROWS)
            .map_err(|error| ApiError(StatusCode::INTERNAL_SERVER_ERROR, error))?
        {
            for (name, value) in metadata {
                if name == key {
                    values.insert(value);
                }
            }
        }
    } else {
        // A dropdown built without the other filters offers values that
        // belong to other rows and return nothing when clicked, so the
        // matchers are evaluated against each sampled row's own metadata.
        for labels in state
            .memtable
            .series(&tenant, &params.matchers, guard.window)
        {
            if let Some(value) = labels.get(&key) {
                values.insert(value.clone());
            }
        }
        guard
            .check_deadline()
            .map_err(|(status, message)| ApiError(status, message))?;
        for metadata in state
            .parts
            .sample_metadata(&tenant, guard.window, METADATA_SAMPLE_ROWS)
            .map_err(|error| ApiError(StatusCode::INTERNAL_SERVER_ERROR, error))?
        {
            let set: crate::memtable::Labels = metadata.into_iter().collect();
            if params.matchers.iter().all(|matcher| matcher.matches(&set))
                && let Some(value) = set.get(&key)
            {
                values.insert(value.clone());
            }
        }
    }

    let mut body = String::new();
    for value in values {
        body.push_str(
            &serde_json::to_string(&serde_json::json!({ "value": value }))
                .expect("a value serializes infallibly"),
        );
        body.push('\n');
    }
    Ok(ndjson_response(body, 0, 0))
}

struct AttributeParams {
    window: part::MetadataWindow,
    matchers: Vec<logql::LabelMatcher>,
}

fn attribute_window_params(
    state: &Arc<AppState>,
    raw: &str,
    allowed: &'static [&'static str],
) -> Result<AttributeParams, ApiError> {
    let now_ns = state.clock.now_ns();
    let params = parse_filter_params(raw, now_ns, allowed).map_err(ApiError::bad_request)?;
    let end_ns = params.end_ns.unwrap_or(now_ns);
    let start_ns = params.start_ns.unwrap_or_else(|| {
        state
            .config
            .max_query_range
            .map(duration_to_i64_ns)
            .map(|range| end_ns.saturating_sub(range))
            .unwrap_or(i64::MIN)
    });
    validate_query_range(&state.config, start_ns, end_ns).map_err(ApiError::bad_request)?;
    Ok(AttributeParams {
        // Inclusive `end`, like every metadata window: these endpoints answer
        // "did this attribute exist in the window", not "which rows are in
        // the window".
        window: part::MetadataWindow { start_ns, end_ns },
        matchers: params.query.matchers,
    })
}
