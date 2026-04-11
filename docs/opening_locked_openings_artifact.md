# Opening-Locked Openings Artifact

Builder: `tools/build_opening_locked_openings_artifact.py`

## CLI

```bash
python tools/build_opening_locked_openings_artifact.py \
  --source-root <lichess-openings-repo-root> \
  --output-root <output-root> \
  --bundle-name opening_locked_lichess_openings_v1
```

Outputs:

- `<output-root>/<bundle-name>/manifest.json`
- `<output-root>/<bundle-name>/opening_locked_openings.sqlite`

## Source contract

- Reads `a.tsv` through `e.tsv`.
- Requires exact TSV header: `eco`, `name`, `pgn`.
- Preserves provenance: filename, source row number, eco, name, pgn.
- If source root is git, captures origin URL, branch, and commit in manifest.

## Position identity

`position_key` and `epd` are stored as normalized text:

- pieces placement
- side to move
- castling rights
- legal en passant square only

(No halfmove/fullmove counters.)

## Naming hierarchy

Names are interpreted as cumulative nodes from the Lichess convention:

`Opening family: Variation, Subvariation, ...`

Node kinds:

- `exact_opening`
- `synthetic_family`

Synthetic nodes are materialized when cumulative intermediates are not explicit source names.

## Canonical continuation

- Exact opening names: `exact_lines.is_shortest_for_name=1` only when shortest line is unique.
- Node continuation (`node_moves.is_canonical`): per `(node, from_position)` choose deterministically by shortest remaining plies, then opening name lexical order, then UCI-line lexical order.

## Query-friendly tables

Core semantics for trainer-side queries:

- `path_memberships`: position membership and minimum remaining plies for a node.
- `node_moves`: supported transitions, support counts, and canonical continuation flags.
- `node_closure`: ancestor/descendant traversal for family selection.
