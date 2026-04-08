#!/usr/bin/env python3
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path


def run(cmd, env=None):
    cp = subprocess.run(cmd, capture_output=True, text=True, env=env)
    if cp.returncode != 0:
        print(cp.stdout)
        print(cp.stderr)
        raise SystemExit(f"command failed: {' '.join(cmd)}")


def build(binary: Path, workspace: Path, out_dir: Path, artifact: str, pgn: Path, mode: str, extra=None):
    if out_dir.exists():
        shutil.rmtree(out_dir)
    log_file = out_dir / "engine.log"
    out_dir.mkdir(parents=True, exist_ok=True)
    cmd = [
        str(binary),
        "--input-pgn", str(pgn),
        "--output-dir", str(out_dir),
        "--artifact-id", artifact,
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
        "--root-min-support", "4",
        "--engine-path", str(workspace / "tests" / "mock_stockfish.py"),
        "--engine-movetime-ms", "1",
        "--engine-hash-mb", "16",
        "--engine-threads", "1",
        "--baseline-prefix-limit", "2",
        "--candidate-prefix-limit", "2",
        "--engine-accept-policy", "max_loss_cp",
        "--engine-max-loss-cp", "25",
        "--engine-reference-mode", "root_best",
        "--emit-progress-log",
        "--emit-status-json",
        "--heartbeat-seconds", "1",
    ]
    if extra:
        cmd.extend(extra)
    env = dict(os.environ)
    env["MOCK_ENGINE_MODE"] = mode
    env["MOCK_ENGINE_LOG"] = str(log_file)
    run(cmd, env=env)
    return out_dir / artifact, log_file


def load_jsonl(path: Path):
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def main():
    if len(sys.argv) != 3:
        raise SystemExit("usage: validate_practical_risk_screen.py <binary> <workspace>")
    binary = Path(sys.argv[1]).resolve()
    workspace = Path(sys.argv[2]).resolve()

    bundle_a, _ = build(binary, workspace, workspace / "out_prs_a", "a", workspace / "tests" / "fixtures_practical_risk_screen.pgn", "default")
    bundle_b, _ = build(binary, workspace, workspace / "out_prs_b", "b", workspace / "tests" / "fixtures_practical_risk_screen.pgn", "default")

    manifest = json.loads((bundle_a / "manifest.json").read_text())
    assert manifest["artifact_role"] == "practical_risk_screening"
    assert manifest["stockfish_used"] is True
    assert manifest["external_book_dependency_used"] is False
    assert manifest["mean_penalty_veto_used"] is False

    rows = load_jsonl(bundle_a / "root_screen_report.jsonl")
    initial_key = "rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq -"
    root0 = next(r for r in rows if r["position_key"] == initial_key)
    assert root0["accepted_baseline_move"] == "e2e4"
    assert root0["candidate_engine_reports"][0]["candidate_move"] == "d2d4"
    assert root0["candidate_engine_reports"][0]["candidate_is_engine_fail"] is True
    assert root0["candidate_engine_reports"][0]["candidate_ceiling_beats_baseline"] is True
    assert root0["candidate_engine_reports"][0]["final_pass"] is True

    rows_b = load_jsonl(bundle_b / "root_screen_report.jsonl")
    root0_b = next(r for r in rows_b if r["position_key"] == initial_key)
    assert root0_b["candidate_engine_reports"][0]["final_reason_code"] == root0["candidate_engine_reports"][0]["final_reason_code"]

    status = json.loads((bundle_a / "progress" / "latest_status.json").read_text())
    assert status["stage_active"] is False

    no_base_bundle, _ = build(
        binary,
        workspace,
        workspace / "out_prs_nobase",
        "nobase",
        workspace / "tests" / "fixtures_practical_risk_screen.pgn",
        "no_baseline",
        extra=["--baseline-prefix-limit", "1"],
    )
    nb_rows = load_jsonl(no_base_bundle / "root_screen_report.jsonl")
    nb_root = next(r for r in nb_rows if r["position_key"] == initial_key)
    assert nb_root["root_reason_code"] == "discard_no_engine_accepted_baseline"

    dead_bundle, dead_log = build(
        binary,
        workspace,
        workspace / "out_prs_dead",
        "dead",
        workspace / "tests" / "fixtures_practical_risk_empirical_dead.pgn",
        "default",
    )
    dead_rows = load_jsonl(dead_bundle / "root_screen_report.jsonl")
    dead_root = next(r for r in dead_rows if r["position_key"] == initial_key)
    assert dead_root["root_reason_code"] == "discard_no_candidate_empirical_survivors"
    if dead_log.exists():
        assert dead_log.read_text().strip() == ""


if __name__ == "__main__":
    main()
