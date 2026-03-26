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


def main():
    if len(sys.argv) != 3:
        raise SystemExit("usage: validate_sqlite_payload.py <binary> <workspace>")
    binary = Path(sys.argv[1]).resolve()
    workspace = Path(sys.argv[2]).resolve()
    out_dir = workspace / "out_sqlite_test"
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

    bundles = list(out_dir.iterdir())
    if len(bundles) != 1:
        raise SystemExit("expected exactly one artifact bundle")
    bundle = bundles[0]

    manifest = json.loads((bundle / "manifest.json").read_text())
    if manifest.get("payload_format") != "sqlite":
        raise SystemExit("manifest payload_format mismatch")

    sqlite_path = bundle / "data" / "corpus.sqlite"
    if not sqlite_path.exists():
        raise SystemExit("sqlite payload missing")

    con = sqlite3.connect(sqlite_path)
    try:
        pos_count = con.execute("select count(*) from positions").fetchone()[0]
        move_count = con.execute("select count(*) from moves").fetchone()[0]
        if pos_count <= 0:
            raise SystemExit("positions table empty")
        if move_count <= 0:
            raise SystemExit("moves table empty")

        root_key = "rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq -"
        row = con.execute(
            """
            select p.total_observations, m.move_key, m.raw_count
            from positions p
            join moves m on m.position_id = p.position_id
            where p.position_key = ?
            order by m.raw_count desc, m.move_key asc
            limit 1
            """,
            (root_key,),
        ).fetchone()
        if row is None:
            raise SystemExit("initial position not found")
        total_observations, move_key, raw_count = row
        if total_observations != 1:
            raise SystemExit(f"unexpected root total_observations: {total_observations}")
        if move_key != "e2e4" or raw_count != 1:
            raise SystemExit(f"unexpected root move/count: {move_key}/{raw_count}")
    finally:
        con.close()


if __name__ == "__main__":
    main()
