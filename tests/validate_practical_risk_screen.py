#!/usr/bin/env python3
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path


def run(cmd, env=None):
    proc = subprocess.run(cmd, capture_output=True, text=True, env=env)
    if proc.returncode != 0:
        print(proc.stdout)
        print(proc.stderr)
        raise SystemExit(f"command failed: {' '.join(cmd)}")


def build(binary: Path, workspace: Path, out_dir: Path, artifact_id: str, mode: str):
    if out_dir.exists():
        shutil.rmtree(out_dir)
    env = os.environ.copy()
    env["FAKE_ENGINE_MODE"] = mode
    cmd = [
        str(binary),
        "--input-pgn", str(workspace / "tests" / "fixtures_practical_risk_screen.pgn"),
        "--output-dir", str(out_dir),
        "--artifact-id", artifact_id,
        "--min-rating", "1400",
        "--max-rating", "1600",
        "--rating-policy", "both_in_band",
        "--retained-ply", "1",
        "--time-controls", "600+0",
        "--time-control-id", "600+0",
        "--initial-time-seconds", "600",
        "--increment-seconds", "0",
        "--time-format-label", "Rapid",
        "--candidate-min-support", "2",
        "--baseline-min-support", "2",
        "--root-min-support", "6",
        "--engine-path", str(workspace / "tests" / "fake_stockfish.py"),
        "--engine-movetime-ms", "1",
        "--engine-hash-mb", "16",
        "--engine-threads", "1",
        "--baseline-prefix-limit", "2",
        "--candidate-prefix-limit", "2",
        "--engine-accept-policy", "max_loss_cp",
        "--engine-max-loss-cp", "20",
        "--engine-reference-mode", "root_best",
        "--emit-progress-log",
        "--emit-status-json",
    ]
    run(cmd, env=env)
    return out_dir / artifact_id


def read_jsonl(path: Path):
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def main():
    if len(sys.argv) != 3:
        raise SystemExit("usage: validate_practical_risk_screen.py <binary> <workspace>")

    binary = Path(sys.argv[1]).resolve()
    workspace = Path(sys.argv[2]).resolve()

    bundle_a = build(binary, workspace, workspace / "out_practical_risk_a", "artifact_a", "mode1")
    bundle_b = build(binary, workspace, workspace / "out_practical_risk_b", "artifact_b", "mode1")

    # deterministic rerun stability
    assert (bundle_a / "root_screen_report.jsonl").read_text() == (bundle_b / "root_screen_report.jsonl").read_text()

    manifest = json.loads((bundle_a / "manifest.json").read_text())
    assert manifest["artifact_role"] == "practical_risk_screening_artifact"
    assert manifest["stockfish_used"] is True
    assert manifest["mean_penalty_veto_used"] is False

    rows = read_jsonl(bundle_a / "root_screen_report.jsonl")
    start_rows = [r for r in rows if r.get("position_key", "").startswith("rnbqkbnr/")]
    assert start_rows, "expected initial root"
    engine_row = [r for r in start_rows if "candidate_engine_checks" in r][0]

    # candidate loses under old mean-penalty style (mu lower than baseline), but passes ceiling-only + engine-fail.
    baseline_mu = engine_row["accepted_baseline_mu_empirical"]
    cand = [c for c in engine_row["candidate_engine_checks"] if c["candidate_move"] == "d2d4"][0]
    assert cand["candidate_mu_empirical"] < baseline_mu
    assert cand["final_pass"] is True

    # progress status json should be valid and finalized
    status = json.loads((bundle_a / "progress" / "latest_status.json").read_text())
    assert status["stage_active"] is False
    assert status["risky_estimated_remaining_work"] is None

    # no accepted baseline in prefix limit case
    bundle_c = build(binary, workspace, workspace / "out_practical_risk_c", "artifact_c", "mode2")
    rows_c = read_jsonl(bundle_c / "root_screen_report.jsonl")
    start_rows_c = [r for r in rows_c if r.get("position_key", "").startswith("rnbqkbnr/") and "candidate_engine_checks" in r]
    assert start_rows_c
    assert start_rows_c[0]["accepted_baseline_move"] is None
    assert start_rows_c[0]["accepted_baseline_engine_reason"] == "discard_no_engine_accepted_baseline"

    summary = json.loads((bundle_c / "screen_summary.json").read_text())
    assert summary["roots_discarded_no_engine_accepted_baseline"] >= 1


if __name__ == "__main__":
    main()
