# Contributing

Thanks for your interest! This repo is intentionally kept focused and deterministic.

## Ground rules
- Keep changes scoped and well‑documented.
- Do not commit datasets or credentials.
- Prefer config‑only changes for environment differences.

## Quick start
1) Create a branch.
2) Run the pipeline locally:
   ```bash
   make setup
   ./scripts/download_data.sh
   make rerun_all_force
   ```
3) Open a PR with a clear summary and rationale.

## Code style
- Keep functions small and explicit.
- Favor readable configs over implicit defaults.

## Tests
There are no formal unit tests yet. Use `make rerun_all_force` to validate output integrity.
