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
    return proc


def position_move_counts_from_legacy(path: Path):
    con = sqlite3.connect(path)
    try:
        rows = con.execute(
            """
            select p.position_key, m.move_key, m.raw_count
            from positions p join moves m on m.position_id = p.position_id
            order by p.position_key, m.move_key
            """
        ).fetchall()
        return {(p, m): c for p, m, c in rows}
    finally:
        con.close()


def position_move_counts_from_compact(path: Path):
    con = sqlite3.connect(path)
    try:
        rows = con.execute(
            """
            select p.position_key_inspect, mv.uci_text, pm.raw_count
            from positions p
            join position_moves pm on pm.position_id = p.position_id
            join moves mv on mv.move_id = pm.move_id
            order by p.position_key_inspect, mv.uci_text
            """
        ).fetchall()
        return {(p, m): c for p, m, c in rows}
    finally:
        con.close()


def main():
    if len(sys.argv) != 4:
        raise SystemExit("usage: validate_compact_payload.py <builder> <inspector> <workspace>")
    builder = Path(sys.argv[1]).resolve()
    inspector = Path(sys.argv[2]).resolve()
    workspace = Path(sys.argv[3]).resolve()

    out_dir = workspace / "out_compact_test"
    if out_dir.exists():
        import shutil
        shutil.rmtree(out_dir)

    cmd = [
        str(builder),
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
        "--payload-format", "exact_sqlite_v2_compact",
        "--time-controls", "600+0",
        "--time-control-id", "600+0",
        "--initial-time-seconds", "600",
        "--increment-seconds", "0",
        "--time-format-label", "Rapid",
    ]
    run(cmd)

    bundle = next(out_dir.iterdir())
    manifest = json.loads((bundle / "manifest.json").read_text())
    assert manifest["payload_format"] == "exact_sqlite_v2_compact"
    assert manifest["canonical_payload_format"] == "exact_sqlite_v2_compact"
    assert manifest["compatibility_payload_emitted"] is True
    assert manifest["time_control_id"] == "600+0"
    assert manifest["initial_time_seconds"] == 600
    assert manifest["increment_seconds"] == 0
    assert manifest["time_format_label"] == "Rapid"

    compact = bundle / "data" / "corpus_compact.sqlite"
    legacy = bundle / "data" / "corpus.sqlite"
    if not compact.exists() or not legacy.exists():
        raise SystemExit("expected both compact and legacy sqlite payloads")

    compact_counts = position_move_counts_from_compact(compact)
    legacy_counts = position_move_counts_from_legacy(legacy)
    if compact_counts != legacy_counts:
        raise SystemExit("compact payload is not semantically identical to legacy mirror")

    run([str(inspector), "--bundle", str(bundle), "--moves", "e2e4", "--show-san"])


if __name__ == "__main__":
    main()
