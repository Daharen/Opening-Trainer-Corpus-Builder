# Opening Trainer Corpus Builder

This repository contains the first runnable C++ baseline for the corpus builder executable `opening-trainer-corpus-builder`.

## Supported modes

The builder supports deterministic scaffold emission, source preflight, range planning, and header-only scanning via `--mode scan-headers`. The new scan mode reuses the safe range planner, scans complete game envelopes inside owned byte windows, extracts header tags, and applies explicit rating-policy eligibility checks without performing SAN replay.

Generated bundles can include:

- `manifest.json`
- `build_summary.txt`
- `plans/range_plan.json`
- `plans/range_execution_summary.json`
- `plans/range_execution_summary.txt`
- `data/positions_placeholder.jsonl`
- `data/header_scan_preview.jsonl` when requested

Use `--help` to see the full CLI surface, including explicit rating-policy semantics and the honesty note that move replay/final corpus payload emission are still deferred.
