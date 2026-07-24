# Token-saving guidance

- Do not create, spawn, delegate to, or wait for subagents.
- Work directly in the current session.
- Start repository inspection with `git status --short` and
  `git diff --stat`.
- Search with `rg` or `rg --files` before opening files.
- Read only the relevant symbol and line ranges; avoid rereading unchanged
  files.
- Do not print entire files, full diffs, or full successful command logs.
- Use targeted tests while iterating and report only concise success summaries.
- Keep tool output below 8,000 tokens unless more output is required to
  diagnose a failure.
