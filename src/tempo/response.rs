fn tempo_trace_response(spans: Vec<TraceSpan>) -> serde_json::Value {
    let mut batches: BTreeMap<
        String,
        (serde_json::Value, serde_json::Value, Vec<serde_json::Value>),
    > = BTreeMap::new();
    for span in spans {
        let resource = span
            .resource
            .as_ref()
            .and_then(|resource| serde_json::to_value(resource).ok())
            .unwrap_or_else(|| serde_json::json!({}));
        let scope = span
            .scope
            .as_ref()
            .and_then(|scope| serde_json::to_value(scope).ok())
            .unwrap_or_else(|| serde_json::json!({}));
        let key = serde_json::to_string(&(resource.clone(), scope.clone())).unwrap_or_default();
        let entry = batches
            .entry(key)
            .or_insert_with(|| (resource, scope, Vec::new()));
        entry
            .2
            .push(serde_json::to_value(span.span).unwrap_or_else(|_| serde_json::json!({})));
    }
    serde_json::json!({
        "batches": batches
            .into_values()
            .map(|(resource, scope, spans)| serde_json::json!({
                "resource": resource,
                "instrumentationLibrarySpans": [{
                    "instrumentationLibrary": scope,
                    "spans": spans,
                }],
            }))
            .collect::<Vec<_>>(),
    })
}


