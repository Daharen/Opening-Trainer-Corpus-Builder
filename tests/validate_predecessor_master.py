#!/usr/bin/env python3
import hashlib
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


def build_bundle(binary: Path, out_dir: Path, input_pgn: Path, min_rating: int, max_rating: int):
    if out_dir.exists():
        shutil.rmtree(out_dir)
    cmd = [
        str(binary),
        "--mode", "aggregate-counts",
        "--input-pgn", str(input_pgn),
        "--output-dir", str(out_dir),
        "--min-rating", str(min_rating),
        "--max-rating", str(max_rating),
        "--rating-policy", "both_in_band",
        "--retained-ply", "5",
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


def merge_master(binary: Path, master_path: Path, sources, delete_after_merge=False):
    if master_path.exists():
        master_path.unlink()
    cmd = [
        str(binary),
        "--mode", "build-predecessor-master",
        "--master-output", str(master_path),
        "--batch-size", "2",
    ]
    for source in sources:
        cmd.extend(["--source-predecessor", str(source)])
    if delete_after_merge:
        cmd.append("--delete-source-after-merge")
    run(cmd)


def run_merge(binary: Path, master_path: Path, sources):
    cmd = [
        str(binary),
        "--mode", "build-predecessor-master",
        "--master-output", str(master_path),
        "--batch-size", "2",
    ]
    for source in sources:
        cmd.extend(["--source-predecessor", str(source)])
    run(cmd)


def load_master_rows(master_path: Path):
    con = sqlite3.connect(master_path)
    try:
        return con.execute(
            """
            select position_key, parent_position_key, incoming_move_uci, depth_from_root,
                   edge_support_count, selection_policy_version, source_artifact_id, source_merge_order
            from master_positions
            order by position_key
            """
        ).fetchall()
    finally:
        con.close()


def assert_true(cond, message):
    if not cond:
        raise SystemExit(message)


def digest_rows(rows):
    payload = "\n".join("|".join("" if x is None else str(x) for x in row) for row in rows)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def main():
    if len(sys.argv) != 3:
        raise SystemExit("usage: validate_predecessor_master.py <binary> <workspace>")
    binary = Path(sys.argv[1]).resolve()
    workspace = Path(sys.argv[2]).resolve()

    a_bundle = build_bundle(binary, workspace / "out_master_a", workspace / "tests" / "fixtures_tiny.pgn", 1000, 2000)
    b_bundle = build_bundle(binary, workspace / "out_master_b", workspace / "tests" / "fixtures_predecessor_convergent.pgn", 1000, 2200)

    a_sqlite = a_bundle / "data" / "canonical_predecessor_edges.sqlite"
    b_sqlite = b_bundle / "data" / "canonical_predecessor_edges.sqlite"

    # 1) single source
    master_single = workspace / "out_predecessor_master_single.sqlite"
    merge_master(binary, master_single, [a_sqlite])
    rows_single = load_master_rows(master_single)
    assert_true(len(rows_single) > 0, "single-source merge produced no rows")

    # 2) precedence and deterministic first-seen-wins with overlapping root key
    master_multi = workspace / "out_predecessor_master_multi.sqlite"
    merge_master(binary, master_multi, [b_sqlite, a_sqlite])
    rows_multi = load_master_rows(master_multi)
    root_key = "rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq -"
    root_rows = [row for row in rows_multi if row[0] == root_key]
    assert_true(len(root_rows) == 1, "expected exactly one root row in master")
    assert_true(root_rows[0][7] == 1, "first source did not win precedence for overlapping root")

    # 3) incremental merge growth
    master_inc = workspace / "out_predecessor_master_incremental.sqlite"
    merge_master(binary, master_inc, [a_sqlite])
    before_count = len(load_master_rows(master_inc))
    run_merge(binary, master_inc, [b_sqlite])
    after_count = len(load_master_rows(master_inc))
    assert_true(after_count >= before_count, "incremental merge unexpectedly shrank master")

    # 4) repeated source re-merge safety
    before_rerun = load_master_rows(master_inc)
    run_merge(binary, master_inc, [b_sqlite])
    after_rerun = load_master_rows(master_inc)
    assert_true(digest_rows(before_rerun) == digest_rows(after_rerun), "re-merging same source changed semantic master rows")

    # 5) deterministic rerun stability
    master_multi_b = workspace / "out_predecessor_master_multi_b.sqlite"
    merge_master(binary, master_multi_b, [b_sqlite, a_sqlite])
    assert_true(digest_rows(rows_multi) == digest_rows(load_master_rows(master_multi_b)), "same ordered inputs produced nondeterministic master output")

    # 6) delete-after-merge safety (only canonical sqlite filename)
    copy_path = workspace / "canonical_predecessor_edges.sqlite"
    if copy_path.exists():
        copy_path.unlink()
    shutil.copy2(a_sqlite, copy_path)
    master_delete = workspace / "out_predecessor_master_delete.sqlite"
    merge_master(binary, master_delete, [copy_path], delete_after_merge=True)
    assert_true(not copy_path.exists(), "delete-after-merge did not delete canonical sqlite source file")


if __name__ == "__main__":
    main()
