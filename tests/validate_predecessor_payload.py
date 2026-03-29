#!/usr/bin/env python3
import hashlib
import json
import shutil
import sqlite3
import subprocess
import sys
from pathlib import Path


def run(cmd):
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        print(proc.stdout)
        print(proc.stderr)
        raise SystemExit(f"command failed: {' '.join(cmd)}")


def build_bundle(binary: Path, workspace: Path, out_dir: Path, input_pgn: Path, retained_ply: int):
    if out_dir.exists():
        shutil.rmtree(out_dir)
    cmd = [
        str(binary),
        "--mode", "aggregate-counts",
        "--input-pgn", str(input_pgn),
        "--output-dir", str(out_dir),
        "--min-rating", "1000",
        "--max-rating", "2000",
        "--rating-policy", "both_in_band",
        "--retained-ply", str(retained_ply),
        "--threads", "1",
        "--max-games", "0",
        "--position-key-format", "fen_normalized",
        "--move-key-format", "uci",
        "--payload-format", "sqlite",
        "--emit-canonical-predecessors",
        "--time-controls", "600+0",
        "--time-control-id", "600+0",
        "--initial-time-seconds", "600",
        "--increment-seconds", "0",
        "--time-format-label", "Rapid",
    ]
    run(cmd)
    bundles = list(out_dir.iterdir())
    if len(bundles) != 1:
        raise SystemExit("expected exactly one bundle")
    return bundles[0]


def load_predecessor_rows(sqlite_path: Path):
    con = sqlite3.connect(sqlite_path)
    try:
        rows = con.execute(
            """
            select c.position_key as child_key,
                   p.position_key as parent_key,
                   cp.incoming_move_uci,
                   c.depth_from_root,
                   cp.edge_support_count,
                   cp.selection_policy_version
            from canonical_predecessors cp
            join positions c on c.position_id = cp.child_position_id
            left join positions p on p.position_id = cp.parent_position_id
            order by c.position_key
            """
        ).fetchall()
        return rows
    finally:
        con.close()


def find_row(rows, child_key):
    for row in rows:
        if row[0] == child_key:
            return row
    return None


def digest_sqlite(sqlite_path: Path):
    rows = load_predecessor_rows(sqlite_path)
    payload = "\n".join("|".join("" if x is None else str(x) for x in row) for row in rows)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def assert_true(cond, message):
    if not cond:
        raise SystemExit(message)


def main():
    if len(sys.argv) != 3:
        raise SystemExit("usage: validate_predecessor_payload.py <binary> <workspace>")
    binary = Path(sys.argv[1]).resolve()
    workspace = Path(sys.argv[2]).resolve()

    # root + simple line + retained depth + manifest contract
    simple_bundle = build_bundle(binary, workspace, workspace / "out_predecessor_simple", workspace / "tests" / "fixtures_tiny.pgn", 4)
    manifest = json.loads((simple_bundle / "manifest.json").read_text())
    assert_true(manifest.get("canonical_predecessor_emitted") is True, "manifest missing canonical_predecessor_emitted=true")
    assert_true(manifest.get("canonical_predecessor_payload_file") == "data/canonical_predecessor_edges.sqlite", "manifest predecessor payload file mismatch")
    assert_true(manifest.get("canonical_predecessor_payload_contract_version") == "1", "manifest predecessor contract version mismatch")
    assert_true(manifest.get("canonical_predecessor_single_parent_per_position") is True, "manifest predecessor single-parent assertion missing")

    predecessor_sqlite = simple_bundle / "data" / "canonical_predecessor_edges.sqlite"
    assert_true(predecessor_sqlite.exists(), "canonical predecessor sqlite missing")
    rows = load_predecessor_rows(predecessor_sqlite)

    root_key = "rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq -"
    row_root = find_row(rows, root_key)
    assert_true(row_root is not None, "missing root row")
    assert_true(row_root[1] is None and row_root[2] is None and row_root[3] == 0, "root row must have null parent/incoming move and depth 0")

    key_after_e4 = "rnbqkbnr/pppppppp/8/8/4P3/8/PPPP1PPP/RNBQKBNR b KQkq -"
    row_e4 = find_row(rows, key_after_e4)
    assert_true(row_e4 is not None, "missing simple-line row after e2e4")
    assert_true(row_e4[1] == root_key and row_e4[2] == "e2e4" and row_e4[3] == 1, "simple-line predecessor chain incorrect")

    # retained depth boundary
    deep_key = "r1bqkb1r/pppp1ppp/2n2n2/4p3/4P3/5N2/PPPP1PPP/RNBQKB1R w KQkq -"
    assert_true(find_row(rows, deep_key) is None, "position deeper than retained depth must not be emitted")

    # convergent deterministic selection + determinism across repeated run
    convergent_bundle_1 = build_bundle(binary, workspace, workspace / "out_predecessor_conv_a", workspace / "tests" / "fixtures_predecessor_convergent.pgn", 5)
    convergent_bundle_2 = build_bundle(binary, workspace, workspace / "out_predecessor_conv_b", workspace / "tests" / "fixtures_predecessor_convergent.pgn", 5)
    conv_sqlite_1 = convergent_bundle_1 / "data" / "canonical_predecessor_edges.sqlite"
    conv_sqlite_2 = convergent_bundle_2 / "data" / "canonical_predecessor_edges.sqlite"

    # final convergent position after:
    # game A: 1.Nf3 d5 2.g3 Nf6 3.Bg2
    # game B: 1.g3 Nf6 2.Bg2 d5 3.Nf3
    convergent_child = "rnbqkb1r/ppp1pppp/5n2/3p4/8/5NP1/PPPPPPBP/RNBQK2R b KQkq -"
    conv_rows = load_predecessor_rows(conv_sqlite_1)
    conv_row = find_row(conv_rows, convergent_child)
    assert_true(conv_row is not None, "missing convergent child position")

    expected_parent_a = "rnbqkb1r/ppp1pppp/5n2/3p4/8/5NP1/PPPPPP1P/RNBQKB1R w KQkq -"  # before 3.Bg2
    expected_parent_b = "rnbqkb1r/ppp1pppp/5n2/3p4/8/6P1/PPPPPPBP/RNBQK1NR w KQkq -"  # before 3.Nf3
    expected_parent = min(expected_parent_a, expected_parent_b)
    expected_move = "f1g2" if expected_parent == expected_parent_a else "g1f3"
    assert_true(conv_row[1] == expected_parent, "convergent parent selection does not match deterministic tie-break policy")
    assert_true(conv_row[2] == expected_move, "convergent incoming move does not match deterministic tie-break policy")
    assert_true(conv_row[5] == "canonical_predecessor_policy_v1", "selection policy version mismatch")

    assert_true(digest_sqlite(conv_sqlite_1) == digest_sqlite(conv_sqlite_2), "predecessor payload is not deterministic across repeated runs")


if __name__ == "__main__":
    main()
