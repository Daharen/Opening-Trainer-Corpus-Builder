#!/usr/bin/env python3
import json
import shutil
import subprocess
import sys
from pathlib import Path


def run(cmd):
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        print(proc.stdout)
        print(proc.stderr)
        raise SystemExit(f"command failed: {' '.join(cmd)}")


def build(binary: Path, workspace: Path, out_dir: Path, artifact_id: str, engine_max_loss_cp: int):
    if out_dir.exists():
        shutil.rmtree(out_dir)
    cmd = [
        str(binary),
        "--input-pgn", str(workspace / "tests" / "fixtures_practical_risk_screen.pgn"),
        "--output-dir", str(out_dir),
        "--artifact-id", artifact_id,
        "--min-rating", "1400",
        "--max-rating", "1600",
        "--rating-policy", "both_in_band",
        "--retained-ply", "2",
        "--time-controls", "600+0",
        "--time-control-id", "600+0",
        "--initial-time-seconds", "600",
        "--increment-seconds", "0",
        "--time-format-label", "Rapid",
        "--candidate-min-support", "2",
        "--baseline-min-support", "2",
        "--root-min-support", "5",
        "--engine-path", str(workspace / "tests" / "fake_stockfish.py"),
        "--engine-movetime-ms", "1",
        "--engine-hash-mb", "1",
        "--engine-threads", "1",
        "--baseline-prefix-limit", "1",
        "--candidate-prefix-limit", "4",
        "--engine-accept-policy", "max_loss_cp",
        "--engine-max-loss-cp", str(engine_max_loss_cp),
        "--engine-reference-mode", "root_best",
        "--emit-progress-log",
        "--emit-status-json",
    ]
    run(cmd)
    return out_dir / artifact_id


def parse_jsonl(path: Path):
    rows = []
    for line in path.read_text().splitlines():
        if line.strip():
            rows.append(json.loads(line))
    return rows


def main():
    if len(sys.argv) != 3:
        raise SystemExit("usage: validate_practical_risk_screen.py <binary> <workspace>")

    binary = Path(sys.argv[1]).resolve()
    workspace = Path(sys.argv[2]).resolve()

    out_a = workspace / "out_practical_screen_a"
    out_b = workspace / "out_practical_screen_b"
    bundle_a = build(binary, workspace, out_a, "screen_a", 80)
    bundle_b = build(binary, workspace, out_b, "screen_b", 80)

    manifest = json.loads((bundle_a / "manifest.json").read_text())
    assert manifest["artifact_role"] == "practical_risk_screening"
    assert manifest["stockfish_used"] is True
    assert manifest["external_book_dependency_used"] is False
    assert manifest["mean_penalty_veto_used"] is False

    status = json.loads((bundle_a / "progress" / "latest_status.json").read_text())
    assert status["stage_active"] is False

    rows = parse_jsonl(bundle_a / "root_screen_report.jsonl")
    keyed = {row["position_key"]: row for row in rows if "position_key" in row and "accepted_baseline_move" in row}

    initial_key = "rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq -"
    initial = keyed[initial_key]
    assert initial["accepted_baseline_move"] == "e2e4"
    cand_rows = {row["candidate_move"]: row for row in initial["candidate_engine_rows"]}
    assert cand_rows["c2c4"]["candidate_is_engine_fail"] is True
    assert cand_rows["c2c4"]["candidate_ceiling_beats_baseline"] is True
    assert cand_rows["c2c4"]["final_pass"] is True
    assert "d2d4" not in cand_rows  # empirically dead candidate should never reach engine stage

    rows_b = parse_jsonl(bundle_b / "root_screen_report.jsonl")
    assert [json.dumps(r, sort_keys=True) for r in rows] == [json.dumps(r, sort_keys=True) for r in rows_b]

    out_c = workspace / "out_practical_screen_c"
    bundle_c = build(binary, workspace, out_c, "screen_c", 5)
    rows_c = parse_jsonl(bundle_c / "root_screen_report.jsonl")
    discard_rows = [row for row in rows_c if row.get("reason") == "discard_no_engine_accepted_baseline"]
    assert discard_rows, "expected explicit no accepted baseline discard"


if __name__ == "__main__":
    main()
