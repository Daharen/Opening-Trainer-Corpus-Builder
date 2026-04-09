#!/usr/bin/env python3
import json
import shutil
import sqlite3
import subprocess
import sys
from pathlib import Path


def run(cmd, env=None):
    cp = subprocess.run(cmd, capture_output=True, text=True, env=env)
    if cp.returncode != 0:
        print(cp.stdout)
        print(cp.stderr)
        raise SystemExit(f"command failed: {' '.join(cmd)}")


def build_stage_a(binary: Path, workspace: Path, out_dir: Path, artifact: str):
    if out_dir.exists():
        shutil.rmtree(out_dir)
    cmd = [
        str(binary),
        "--input-pgn", str(workspace / "tests" / "practical_risk" / "fixtures" / "practical_risk_prestockfish_fixture.pgn"),
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
        "--root-min-support", "2",
        "--move-min-support", "2",
        "--deep-line-min-support", "2",
        "--deep-total-plies", "6",
        "--deep-own-plies", "3",
        "--sigma-global-min-lines", "3",
    ]
    run(cmd)
    return out_dir / artifact


def build_stage_b(binary: Path, stage_a_bundle: Path, out_dir: Path, artifact: str, baseline_prefix_limit: int):
    if out_dir.exists():
        shutil.rmtree(out_dir)
    cmd = [
        str(binary),
        "--prestockfish-bundle", str(stage_a_bundle),
        "--output-dir", str(out_dir),
        "--artifact-id", artifact,
        "--engine-path", str(stage_a_bundle.parent.parent / "tests" / "mock_stockfish.py"),
        "--engine-movetime-ms", "50",
        "--engine-hash-mb", "16",
        "--engine-threads", "1",
        "--engine-accept-policy", "max_loss_cp",
        "--engine-max-loss-cp", "15",
        "--engine-reference-mode", "root_best",
        "--baseline-prefix-limit", str(baseline_prefix_limit),
        "--candidate-prefix-limit", "8",
        "--emit-progress-log",
        "--emit-status-json",
        "--heartbeat-seconds", "1",
    ]
    env = dict(**__import__("os").environ)
    env["PYTHON_EXECUTABLE"] = sys.executable
    env["MOCK_ENGINE_MODE"] = "overlay_perspective"
    run(cmd, env=env)
    return out_dir / artifact


def validate_bundle(bundle: Path, expect_baseline: bool):
    manifest = json.loads((bundle / "manifest.json").read_text())
    assert manifest["artifact_role"] == "practical_risk_stockfish_overlay"
    assert manifest["stockfish_used"] is True
    assert manifest["external_book_dependency_used"] is False

    status = json.loads((bundle / "progress" / "latest_status.json").read_text())
    assert status["stage"] == "finalize"
    assert status["stage_active"] is False

    db = sqlite3.connect(bundle / "practical_risk_stockfish_overlay.sqlite")
    cur = db.cursor()
    tables = {r[0] for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    required = {
        "artifact_metadata",
        "engine_metadata",
        "move_engine_evals",
        "root_direct_baselines",
        "accepted_bucket_ceiling_priors",
    }
    assert required.issubset(tables)

    root_count = cur.execute("SELECT COUNT(*) FROM root_direct_baselines").fetchone()[0]
    assert root_count > 0
    reconstruction_failures = cur.execute("SELECT COUNT(*) FROM root_direct_baselines WHERE reason_code='board_reconstruction_failed'").fetchone()[0]
    assert reconstruction_failures == 0

    accepted = cur.execute("SELECT COUNT(*) FROM move_engine_evals WHERE is_engine_accepted=1").fetchone()[0]
    failing = cur.execute("SELECT COUNT(*) FROM move_engine_evals WHERE is_engine_fail=1").fetchone()[0]
    assert accepted > 0
    assert failing > 0

    white_roots = cur.execute("SELECT COUNT(DISTINCT position_key) FROM move_engine_evals WHERE instr(position_key, ' w ') > 0").fetchone()[0]
    black_roots = cur.execute("SELECT COUNT(DISTINCT position_key) FROM move_engine_evals WHERE instr(position_key, ' b ') > 0").fetchone()[0]
    assert white_roots > 0
    assert black_roots > 0

    accepted_white = cur.execute(
        "SELECT COUNT(*) FROM move_engine_evals WHERE is_engine_accepted=1 AND instr(position_key, ' w ') > 0"
    ).fetchone()[0]
    accepted_black = cur.execute(
        "SELECT COUNT(*) FROM move_engine_evals WHERE is_engine_accepted=1 AND instr(position_key, ' b ') > 0"
    ).fetchone()[0]
    assert accepted_white > 0
    assert accepted_black > 0

    inconsistent_loss_rows = cur.execute(
        "SELECT COUNT(*) FROM move_engine_evals WHERE abs((root_best_cp - move_cp) - loss_cp) > 1e-6"
    ).fetchone()[0]
    assert inconsistent_loss_rows == 0

    sign_bug_like_rows = cur.execute(
        "SELECT COUNT(*) FROM move_engine_evals WHERE is_engine_accepted=1 AND loss_cp < -30"
    ).fetchone()[0]
    assert sign_bug_like_rows == 0

    priors = cur.execute("SELECT COUNT(*) FROM accepted_bucket_ceiling_priors").fetchone()[0]
    assert priors > 0

    found = cur.execute("SELECT COUNT(*) FROM root_direct_baselines WHERE baseline_found=1").fetchone()[0]
    missing = cur.execute("SELECT COUNT(*) FROM root_direct_baselines WHERE baseline_found=0").fetchone()[0]
    if expect_baseline:
        assert found > 0
    else:
        assert missing > 0

    db.close()


def main():
    if len(sys.argv) != 4:
        raise SystemExit("usage: validate_practical_risk_stockfish_overlay.py <stage_a_binary> <stage_b_binary> <workspace>")

    stage_a_binary = Path(sys.argv[1]).resolve()
    stage_b_binary = Path(sys.argv[2]).resolve()
    workspace = Path(sys.argv[3]).resolve()

    stage_a_bundle = build_stage_a(stage_a_binary, workspace, workspace / "out_stageb_stagea", "stagea")
    bundle_baseline = build_stage_b(stage_b_binary, stage_a_bundle, workspace / "out_stageb_overlay_a", "overlay_a", 8)
    bundle_no_baseline = build_stage_b(stage_b_binary, stage_a_bundle, workspace / "out_stageb_overlay_b", "overlay_b", 1)

    validate_bundle(bundle_baseline, expect_baseline=True)
    validate_bundle(bundle_no_baseline, expect_baseline=False)


if __name__ == "__main__":
    main()
