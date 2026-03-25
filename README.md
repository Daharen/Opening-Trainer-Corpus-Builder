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


## SQLite dependency model

SQLite aggregate payload output is wired through a repo-local dependency path by default.
The CMake option `OTCB_USE_SYSTEM_SQLITE` is available for environments that prefer an
already-installed SQLite package, but ordinary local builds do **not** require a machine-level
SQLite development package and do not run `find_package(SQLite3)` unless external mode is
explicitly requested.

- Default: `-DOTCB_USE_SYSTEM_SQLITE=OFF`
- Optional system path: `-DOTCB_USE_SYSTEM_SQLITE=ON`


## Behavioral Training Extract Builder

This repo now includes a separate standalone executable: `build-behavioral-training-extract`.
It ingests timed PGN sources (`.pgn` always; `.pgn.zst` only when built with zstd enabled), replays SAN deterministically, extracts per-move timing events, and writes a deterministic intermediate SQLite artifact called **Behavioral Training Extract**.

This stage is intentionally **not** profile fitting, profile clustering/merging, runtime corpus mutation, or trainer integration.

### CLI usage

```bash
build-behavioral-training-extract   --input /path/month_2024_01.pgn.zst   --output /tmp/behavioral_extract.sqlite   --time-controls 300+0   --time-controls 300+2   --elo-bands 1400-1599   --month 2024-01   --overwrite
```

### Input support matrix

- Plain `.pgn` input is always supported.
- `.pgn.zst` input is build-configuration dependent via CMake option `OTCB_BEHAVIORAL_EXTRACT_ENABLE_ZSTD`.
- Default local build is safe/off: `-DOTCB_BEHAVIORAL_EXTRACT_ENABLE_ZSTD=OFF`.
- To enable zstd input support in environments with repo-controlled zstd availability:

```bash
cmake -S . -B build -DOTCB_BEHAVIORAL_EXTRACT_ENABLE_ZSTD=ON
```

Required:
- `--input` (repeatable)
- `--output`

Supported options:
- `--time-controls`, `--elo-bands`, `--month`, `--max-games`, `--resume`, `--overwrite`, `--workers`, `--log-every`, `--emit-invalid-report`, `--source-label`, `--strict`

### Behavioral Training Extract schema summary

Tables:
- `extract_manifest`: schema version + deterministic policy metadata.
- `source_files`: normalized source file identities.
- `games`: per-game acceptance/rejection rows with exact and normalized time-control fields.
- `move_events`: one row per emitted timing example containing raw and normalized behavioral fields (position key, UCI move, think-time derivation fields, previous-opponent-think feature, buckets, month).
- `invalid_rows`: malformed/invalid detail rows.

Indexes are emitted for future profile-fitting workflows by mover ELO band, time control, position key, move UCI, and source month.

### Determinism guarantees

- Input file list is lexicographically sorted before processing.
- Game IDs are deterministic synthetic IDs (stable hash of source-file path and in-file game index).
- Move events are inserted in stable source-file/game/ply order.
- Running the same command with the same inputs and config produces identical ordered rows.

### Known limitations

- `--workers` is reserved and currently executes in deterministic single-worker mode.
- If built without zstd support, `.pgn.zst` is rejected explicitly at runtime with a rebuild hint.
- If built with zstd support, `.pgn.zst` requires valid zstd-frame input for decompression; `.zst` files without zstd magic are treated as plain text for fixture compatibility.
