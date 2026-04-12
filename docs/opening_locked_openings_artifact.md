# Opening-Locked Openings Artifact

Builder: `tools/build_opening_locked_openings_artifact.py`

## CLI

```bash
python tools/build_opening_locked_openings_artifact.py \
  --source-root <lichess-openings-repo-root> \
  --output-root <output-root> \
  --bundle-name opening_locked_lichess_openings_family_v1 \
  --artifact-kind opening_locked_openings_family_v1
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

## Artifact families

The builder now supports two explicit artifact kinds:

- `opening_locked_openings` (legacy exact-opening artifact shape, schema version `1`)
- `opening_locked_openings_family_v1` (family-aware artifact shape, schema version `2`)

Both artifact kinds preserve the exact-opening tables:

- `opening_nodes`
- `node_closure`
- `positions`
- `exact_lines`
- `path_memberships`
- `node_moves`

## Family-aware tables (`opening_locked_openings_family_v1`)

### Internal family graph (multi-parent allowed)

- `family_edges(parent_node_id, child_node_id, edge_kind, evidence_score, is_canonical_ui_parent)`
- `family_memberships(family_node_id, member_node_id, membership_kind)`

`family_edges` can contain multiple parent candidates for a child node. This is the internal truth model.

### Transposition relationships (not a tree)

- `transposition_edges(from_node_id, to_node_id, shared_position_count, earliest_shared_ply, relationship_kind)`

These rows capture shared-position convergence between distinct canonical exact lines.

### Canonical UI projection (single parent)

- `ui_tree(child_node_id, ui_parent_node_id, selection_depth)`

`ui_tree` is **not** the full truth model; it is a deterministic display projection built from `family_edges`.

Canonical parent precedence:

1. `lexical_hierarchy`
2. `canonical_line_named_prefix`
3. `preserved_co_membership`
4. lexical tie-break by node id

## Manifest additions for family-aware artifacts

For `opening_locked_openings_family_v1`, manifest fields include:

- `family_edge_count`
- `transposition_edge_count`
- `ui_tree_node_count`
- `family_derivation_rules`
- `canonical_ui_parent_rule`
- `transposition_detection_rule`
