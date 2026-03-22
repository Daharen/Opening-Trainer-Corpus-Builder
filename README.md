# Opening Trainer Corpus Builder

This repository contains the first runnable C++ baseline for the corpus builder executable `opening-trainer-corpus-builder`.

## Supported mode

The current baseline supports **scaffold dry-run artifact emission only**. It validates the CLI contract and writes a deterministic artifact bundle skeleton containing:

- `manifest.json`
- `build_summary.txt`
- `data/positions_placeholder.jsonl`

Use `--help` to see the full CLI surface, including explicit rating-policy semantics.
