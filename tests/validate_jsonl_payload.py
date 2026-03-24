#!/usr/bin/env python3
import json
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
        raise SystemExit("usage: validate_jsonl_payload.py <binary> <workspace>")
    binary = Path(sys.argv[1]).resolve()
    workspace = Path(sys.argv[2]).resolve()
    out_dir = workspace / "out_jsonl_test"
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
        "--payload-format", "jsonl",
    ]
    run(cmd)

    bundles = list(out_dir.iterdir())
    if len(bundles) != 1:
        raise SystemExit("expected exactly one artifact bundle")
    bundle = bundles[0]

    manifest = json.loads((bundle / "manifest.json").read_text())
    if manifest.get("payload_format") != "jsonl":
        raise SystemExit("manifest payload_format mismatch")

    jsonl_path = bundle / "data" / "aggregated_position_move_counts.jsonl"
    if not jsonl_path.exists():
        raise SystemExit("jsonl payload missing")

    lines = [line for line in jsonl_path.read_text().splitlines() if line.strip()]
    if not lines:
        raise SystemExit("jsonl payload empty")


if __name__ == "__main__":
    main()
