#!/usr/bin/env python3
import json
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


def build(binary: Path, workspace: Path, out_dir: Path):
    if out_dir.exists():
        import shutil
        shutil.rmtree(out_dir)
    cmd = [
        str(binary),
        "--mode", "aggregate-counts",
        "--input-pgn", str(workspace / "tests" / "fixtures_tiny.pgn"),
        "--output-dir", str(out_dir),
        "--min-rating", "1000",
        "--max-rating", "2000",
        "--rating-policy", "both_in_band",
        "--retained-ply", "4",
        "--threads", "1",
        "--max-games", "0",
        "--position-key-format", "fen_normalized",
        "--move-key-format", "uci",
        "--payload-format", "sqlite",
        "--time-controls", "600+0",
        "--time-control-id", "600+0",
        "--initial-time-seconds", "600",
        "--increment-seconds", "0",
        "--time-format-label", "Rapid",
    ]
    run(cmd)


def collect_signature(sqlite_path: Path):
    con = sqlite3.connect(sqlite_path)
    try:
        rows = con.execute(
            """
            select band_id, policy_variant, scope_variant, position_key, move_key, allowed, resolution_reason_code
            from risky_acceptance_by_band
            order by band_id, policy_variant, position_key, move_key
            """
        ).fetchall()
        return rows
    finally:
        con.close()


def main():
    if len(sys.argv) != 3:
        raise SystemExit("usage: validate_gambit_companion_payload.py <binary> <workspace>")
    binary = Path(sys.argv[1]).resolve()
    workspace = Path(sys.argv[2]).resolve()

    out_a = workspace / "out_gambit_companion_a"
    out_b = workspace / "out_gambit_companion_b"
    build(binary, workspace, out_a)
    build(binary, workspace, out_b)

    bundle_a = next(out_a.iterdir())
    bundle_b = next(out_b.iterdir())
    manifest = json.loads((bundle_a / "manifest.json").read_text())
    if manifest.get("companion_payload_role") != "risky_companion_retained_depth_overlay":
        raise SystemExit("manifest missing companion payload role")

    companion_a = bundle_a / "data" / "risky_companion.sqlite"
    companion_b = bundle_b / "data" / "risky_companion.sqlite"
    if not companion_a.exists() or not companion_b.exists():
        raise SystemExit("companion sqlite missing")

    con = sqlite3.connect(companion_a)
    try:
        ordinary = con.execute("select count(*) from ordinary_move_acceptance_by_band").fetchone()[0]
        baseline = con.execute("select count(*) from opening_variance_baseline_by_band").fetchone()[0]
        metrics = con.execute("select count(*) from risky_entry_metrics").fetchone()[0]
        acceptance = con.execute("select count(*) from risky_acceptance_by_band").fetchone()[0]
        strict = con.execute("select count(*) from risky_entry_metrics where policy_variant='strict'").fetchone()[0]
        lenient = con.execute("select count(*) from risky_entry_metrics where policy_variant='lenient'").fetchone()[0]
        audit = con.execute("select count(*) from risky_acceptance_audit").fetchone()[0]
        if min(ordinary, baseline) <= 0:
            raise SystemExit("required baseline tables are empty")
        if metrics < 0 or acceptance < 0 or strict < 0 or lenient < 0 or audit < 0:
            raise SystemExit("invalid companion table counts")
    finally:
        con.close()

    if collect_signature(companion_a) != collect_signature(companion_b):
        raise SystemExit("companion payload is not deterministic across reruns")


if __name__ == "__main__":
    main()
