#!/usr/bin/env python3
import json
import sqlite3
import subprocess
import sys
from pathlib import Path


def run(cmd, expect_ok=True):
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if expect_ok and proc.returncode != 0:
        print(proc.stdout)
        print(proc.stderr)
        raise SystemExit(f"command failed: {' '.join(cmd)}")
    if not expect_ok and proc.returncode == 0:
        raise SystemExit(f"command unexpectedly succeeded: {' '.join(cmd)}")
    return proc


def build(binary: Path, workspace: Path, tc: str, payload: str = "exact_sqlite_v2_compact"):
    out_dir = workspace / f"out_tc_{tc.replace('+', '_')}_{payload}"
    if out_dir.exists():
        import shutil
        shutil.rmtree(out_dir)
    cmd = [
        str(binary),
        "--mode", "aggregate-counts",
        "--input-pgn", str(workspace / "tests" / "fixtures_time_control_filtering.pgn"),
        "--output-dir", str(out_dir),
        "--min-rating", "1400",
        "--max-rating", "1800",
        "--rating-policy", "both_in_band",
        "--retained-ply", "4",
        "--threads", "1",
        "--max-games", "0",
        "--position-key-format", "fen_normalized",
        "--move-key-format", "uci",
        "--payload-format", payload,
        "--time-controls", tc,
        "--time-control-id", tc,
        "--initial-time-seconds", tc.split("+")[0],
        "--increment-seconds", tc.split("+")[1],
        "--time-format-label", "Rapid",
    ]
    run(cmd)
    bundle = next(out_dir.iterdir())
    return bundle


def total_observations_from_compact(path: Path):
    con = sqlite3.connect(path)
    try:
        value = con.execute("select coalesce(sum(raw_count), 0) from position_moves").fetchone()[0]
        return int(value)
    finally:
        con.close()


def assert_single_scope_counts(bundle: Path):
    manifest = json.loads((bundle / "manifest.json").read_text())
    plans = json.loads((bundle / "plans" / "aggregation_summary.json").read_text())

    assert manifest["filtered_time_controls"] == [manifest["time_control_id"]]
    assert manifest["payload_version"] == "2"
    assert manifest["time_control_id"] in {"600+0", "300+0", "120+1"}

    assert plans["total_games_scanned"] == 5
    assert plans["total_games_accepted_upstream"] == 1
    assert plans["games_rejected_by_time_control_filter"] == 2
    assert plans["games_rejected_invalid_time_control"] == 2
    assert plans["games_rejected_by_rating_filter"] == 0

    assert manifest["games_rejected_time_control_mismatch"] == 2
    assert manifest["games_rejected_invalid_time_control"] == 2


def assert_semantic_identity(bundle: Path):
    compact = bundle / "data" / "corpus_compact.sqlite"
    legacy = bundle / "data" / "corpus.sqlite"
    compact_total = total_observations_from_compact(compact)
    con = sqlite3.connect(legacy)
    try:
        legacy_total = int(con.execute("select coalesce(sum(raw_count), 0) from moves").fetchone()[0])
    finally:
        con.close()
    assert compact_total == legacy_total
    assert compact_total > 0


def assert_metadata_only_rejected(binary: Path, workspace: Path):
    out_dir = workspace / "out_tc_missing_filter"
    if out_dir.exists():
        import shutil
        shutil.rmtree(out_dir)
    cmd = [
        str(binary),
        "--mode", "aggregate-counts",
        "--input-pgn", str(workspace / "tests" / "fixtures_time_control_filtering.pgn"),
        "--output-dir", str(out_dir),
        "--min-rating", "1400",
        "--max-rating", "1800",
        "--rating-policy", "both_in_band",
        "--retained-ply", "4",
        "--threads", "1",
        "--max-games", "0",
        "--position-key-format", "fen_normalized",
        "--move-key-format", "uci",
        "--payload-format", "jsonl",
        "--time-control-id", "600+0",
        "--initial-time-seconds", "600",
        "--increment-seconds", "0",
        "--time-format-label", "Rapid",
    ]
    proc = run(cmd, expect_ok=False)
    if "--time-controls is required" not in (proc.stdout + proc.stderr):
        raise SystemExit("missing expected validation message for required --time-controls")


def main():
    if len(sys.argv) != 3:
        raise SystemExit("usage: validate_time_control_filtering.py <binary> <workspace>")
    binary = Path(sys.argv[1]).resolve()
    workspace = Path(sys.argv[2]).resolve()

    for tc in ("600+0", "300+0", "120+1"):
        bundle = build(binary, workspace, tc)
        assert_single_scope_counts(bundle)
        assert_semantic_identity(bundle)

    assert_metadata_only_rejected(binary, workspace)


if __name__ == "__main__":
    main()
