# Opening Trainer Corpus Builder

This repository contains the first runnable C++ baseline for the corpus builder executable `opening-trainer-corpus-builder`.

## Supported modes

The builder supports deterministic scaffold emission, source preflight, range planning, header-only scanning via `--mode scan-headers`, opening extraction, and aggregate raw-count generation. Aggregate mode now supports `--payload-format <jsonl|sqlite>` with `jsonl` as the backward-compatible default.

Generated bundles can include:

- `manifest.json`
- `build_summary.txt`
- `plans/range_plan.json`
- `plans/range_execution_summary.json`
- `plans/range_execution_summary.txt`
- `data/positions_placeholder.jsonl`
- `data/header_scan_preview.jsonl` when requested
- `data/aggregated_position_move_counts.jsonl` for legacy aggregate payloads
- `data/corpus.sqlite` for SQLite aggregate payloads when explicitly requested

Use `--help` to see the full CLI surface, including explicit rating-policy semantics and the honesty note that move replay/final corpus payload emission are still deferred.
