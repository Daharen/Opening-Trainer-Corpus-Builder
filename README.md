# Opening Trainer Corpus Builder

This repository contains the first runnable C++ baseline for the corpus builder executable `opening-trainer-corpus-builder`.

## Supported modes

The builder supports deterministic scaffold emission, source preflight, range planning, header-only scanning via `--mode scan-headers`, opening extraction, and aggregate raw-count generation. Aggregate mode now supports `--payload-format <jsonl|sqlite|exact_sqlite_v2_compact>`.

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



## Compact Exact Payload v2 (lossless)

Aggregate mode now supports `--payload-format exact_sqlite_v2_compact` as the canonical compact exact runtime payload.

- Canonical output: `data/corpus_compact.sqlite`
- Transitional dual-emission: compatibility mirror `data/corpus.sqlite` is emitted by default (disable with `--no-legacy-sqlite-mirror`).
- Compact schema is lossless and explainable: `positions` (compact key + inspect key), `moves` (move dictionary), `position_moves` (raw counts).
- SAN is not stored in compact hot runtime rows; SAN is regenerated on demand via `inspect-compact-corpus`.

Manifest contract fields are now canonical corpus truth for retained depth, rating policy/band, exact time control and clocks, and payload identity:
`retained_ply_depth`, `max_supported_player_moves`, `rating_lower_bound`, `rating_upper_bound`, `rating_policy`, `target_rating_band`, `time_control_id`, `initial_time_seconds`, `increment_seconds`, `time_format_label`, `payload_format`, `payload_version`, and dual-emission compatibility fields.

## Exact aggregate-count time-control filtering (truthful single-scope corpora)

`--mode aggregate-counts` now enforces real ingestion-time exact `TimeControl` filtering with `--time-controls`.

- Required for aggregate-counts: `--time-controls <exact_control>` (for example `600+0`, `300+0`, `120+1`).
- Matching is exact (`600+0` matches only `600+0`).
- Missing or malformed `TimeControl` headers are rejected for exact-filtered builds.
- `time_format_label` is display metadata only; it is not used as a filter key.
- Metadata-only stamping is blocked: aggregate builds fail if `--time-controls` is omitted.

Manifest and summaries now include explicit provenance and counters such as:
`filtered_time_controls`, `games_rejected_time_control_mismatch`, and `games_rejected_invalid_time_control`.

This makes final compact exact corpora truthful single-scope bundles prior to mass catalog/background generation.

Inspector/export tooling:

```bash
inspect-compact-corpus --bundle /path/to/bundle --moves e2e4,e7e5 --show-san
inspect-compact-corpus --bundle /path/to/bundle --position-key "rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq -"
```

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
build-behavioral-training-extract   --input /path/month_2024_01.pgn.zst   --output /tmp/behavioral_extract.sqlite   --time-controls 300+0   --time-controls 300+2   --elo-bands 1400-1600   --elo-bands 475-525   --rating-policy both_in_band   --month 2024-01   --overwrite
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
- `--time-controls`, `--elo-bands`, `--rating-policy`, `--month`, `--max-games`, `--resume`, `--overwrite`, `--workers`, `--log-every`, `--emit-invalid-report`, `--source-label`, `--strict`

`--elo-bands` accepts inclusive numeric intervals (`lo-hi`) and can be repeated; repeated ranges are treated as a union.
`--rating-policy` controls side membership semantics for those ranges:
- `both_in_band`: White and Black ratings must both fall inside at least one allowed interval.
- `white_in_band`: only White must fall in-range.
- `black_in_band`: only Black must fall in-range.
- `average_in_band`: average of White/Black must fall in-range.

Derived per-move `mover_elo_band` (200-point buckets) is still emitted for downstream modeling, but it is no longer the CLI filter contract.

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

## Behavioral Profile Builder

This repo now also includes a separate standalone executable: `build-behavioral-profiles`.
It consumes one or more Behavioral Training Extract SQLite artifacts and emits a deterministic,
inspectable, versioned SQLite artifact called a **Behavioral Profile Set**.

This stage fits and merges reusable timing-behavior profiles for two separate families:

- **Move Pressure Profiles**: compact parameter packs that model multiplicative move-weight distortion under clock pressure.
- **Think-Time Profiles**: compact parameter packs that model think-time generation distributions after move choice.

This stage intentionally **does not** integrate with trainer runtime yet and does **not** replace
exact move-frequency corpus semantics. Timing remains a separable overlay layer.

### CLI usage

```bash
build-behavioral-profiles \
  --input-extract /tmp/extract_2024_01.sqlite \
  --input-extract /tmp/extract_2024_02.sqlite \
  --output /tmp/behavioral_profile_set.sqlite \
  --time-controls 300+2 \
  --elo-bands 1200-1799 \
  --month-window 2024-01:2024-02 \
  --min-support 25 \
  --merge-threshold 0.12 \
  --overwrite
```

Required:
- `--input-extract` (repeatable)
- `--output`

Supported options:
- `--time-controls`, `--elo-bands`, `--month-window`, `--max-examples`, `--overwrite`, `--log-every`, `--seed-context-limit`, `--min-support`, `--merge-threshold`, `--strict`, `--emit-fit-diagnostics`, `--emit-invalid-report`

### Behavioral Profile Set schema summary

Tables:
- `profile_manifest`: schema, builder/version, method/merge metadata, threshold, deterministic toggles.
- `profile_build_inputs`: input extract identities and accepted training-row counts.
- `move_pressure_profiles`: reusable move-pressure profile parameters + fit/merge provenance.
- `think_time_profiles`: reusable think-time profile parameters + fit/merge provenance.
- `context_profile_map`: deterministic mapping from context family keys to profile IDs.
- `profile_merge_history`: deterministic merge provenance records.
- `fit_diagnostics` (optional): per-context fit diagnostics.
- `invalid_rows` (optional): strict/validation skip reasons.

### Determinism guarantees

- Input extract paths are sorted before loading.
- Input rows are read in stable `(source_file, game_id, game_ply_index)` order.
- Context family ordering is stable and deterministic.
- Merge order is deterministic and threshold-driven.
- Profile IDs are assigned in deterministic creation order.

### Known limitations

- Initial profile math is intentionally simple (first-pass compact parametric fit).
- Similarity/merge currently uses deterministic L1 parameter distance; richer metrics can be added in later lanes.
- Runtime sampling integration is out of scope for this builder-only lane.


## Timing-Conditioned Corpus Bundle Emitter

This repo now includes a standalone executable: `build-timing-conditioned-corpus-bundle`.
It joins an existing exact corpus artifact (bundle directory or SQLite payload) with an existing
Behavioral Profile Set SQLite artifact and emits a deterministic, inspectable **timing-conditioned
corpus bundle**.

This stage is intentionally an emitter/assembler only. It does **not** parse raw PGN, rebuild the
exact corpus, retrain behavioral profiles, or implement runtime sampling logic.

### CLI usage

```bash
build-timing-conditioned-corpus-bundle   --input-corpus-bundle /tmp/exact_corpus_bundle   --input-profile-set /tmp/behavioral_profile_set.sqlite   --output /tmp/timing_conditioned_bundle   --artifact-id timing_conditioned_2024q1   --prototype-label lane_test   --time-controls 300+2   --elo-bands 1200-1799   --overwrite
```

Required:
- `--input-corpus-bundle`
- `--input-profile-set`
- `--output`

Supported options:
- `--overwrite`, `--artifact-id`, `--prototype-label`, `--time-controls`, `--elo-bands`,
  `--log-every`, `--strict-compatibility`, `--allow-prototype-mismatch`,
  `--embed-fit-diagnostics`, `--emit-progress-log`, `--emit-status-json`

### Output bundle contract (v1)

- `manifest.json`: stable audit surface that records exact corpus identity, profile-set identity,
  compatibility warnings, context contract version, timing overlay policy version, deterministic
  timestamp metadata, trainer-compatibility legacy fields, payload references, explicit canonical
  corpus contract metadata, and explicit timing ELO-band dual-contract metadata.
- `data/exact_corpus.sqlite`: canonical exact payload path (authoritative corpus SQLite).
- `data/corpus.sqlite`: compatibility-only alias path for transitional trainer/runtime consumers.
- `data/behavioral_profile_set.sqlite`: authoritative Behavioral Profile Set SQLite audit artifact.
- `data/timing_overlay.json`: trainer-readable timing overlay JSON export derived from
  `behavioral_profile_set.sqlite` with:
  `context_contract_version`, `timing_overlay_policy_version`, `move_pressure_profiles`,
  `think_time_profiles`, and `context_profile_map`.

Timing-conditioned bundles now explicitly preserve two distinct band concepts:
- **display/filter band** (`--elo-bands` / corpus-facing interval such as `400-600`),
- **canonical runtime bucket band** (profile-map vocabulary such as `400-599` and `600-799`).

The emitter preserves canonical context keys and additionally exports deterministic alias keys in
`timing_overlay.json` for every display/filter band that overlaps a canonical runtime bucket.
Example: `400-600` overlaps both `400-599` and `600-799` (because 600 is inclusive), so both
canonical contexts are alias-addressable via `...|400-600|...`.

`manifest.json` now documents this behavior with:
- `timing_runtime_elo_band_policy_version`
- `timing_runtime_elo_band_vocabulary`
- `timing_display_elo_band`
- `timing_display_to_runtime_elo_band_aliases`
- `timing_overlay_alias_mode`
- `timing_overlay_alias_conflicts`

For current trainer compatibility, `manifest.json` now emits legacy aggregate-bundle fields
(`build_status`, `payload_format`, `position_key_format`, `move_key_format`,
`sqlite_corpus_file`, `corpus_sqlite_file`, `payload_file`, and `payload_status`) and points all
trainer payload pointers at `data/corpus.sqlite`. It also emits `timing_overlay_file` set to
`data/timing_overlay.json`.

Timing-conditioned manifests now also propagate canonical corpus contract fields from the input
exact corpus bundle (`retained_ply_depth`, `max_supported_player_moves`, `time_control_id`,
`initial_time_seconds`, `increment_seconds`, `time_format_label`, `target_rating_band`,
`rating_policy`, `payload_format`, `payload_version`, and `bundle_scope` when available) and fail
fast if required canonical metadata is missing.

Exact payload deduplication now prefers a hardlink compatibility alias:
- canonical write: `data/exact_corpus.sqlite`
- compatibility alias: `data/corpus.sqlite`
- manifest fields: `canonical_exact_payload_file`, `compatibility_exact_payload_file`,
  `exact_payload_alias_mode` (`hardlink`, `copy`, or `none`), and `exact_payload_alias_warnings`.

### Compatibility checks

The emitter validates compatibility and fails by default when runtime interpretation would become
ambiguous. Current checks include position/move key expectations, retained opening depth floor,
profile payload presence, requested time-control presence in `context_profile_map`, and ELO filter
overlap against corpus rating bounds.

`--allow-prototype-mismatch` permits mismatches only when `--prototype-label` is present, and the
manifest records surfaced warnings/errors for auditability.

### Determinism notes

Given the same exact corpus input, profile-set input, and emitter config, output is deterministic:
- deterministic artifact-id derivation (unless overridden),
- stable payload copy names,
- stable manifest field ordering,
- stable timing-overlay JSON ordering (`profile_id` / context key sorted export),
- deterministic build timestamp marker (`"deterministic"`).

### Known limitations

- First implementation is conservative and redundant: payloads are copied side-by-side rather than
  deeply fused.
- Compatibility checks are intentionally strict around context scope and intentionally simple around
  policy-version semantics.
- Runtime-side sampling/overlay application remains out of scope for this builder repo.
