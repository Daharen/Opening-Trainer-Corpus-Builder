#!/usr/bin/env python3
import json
import shutil
import sqlite3
import subprocess
import sys
from pathlib import Path


def run(cmd):
    cp = subprocess.run(cmd, capture_output=True, text=True)
    if cp.returncode != 0:
        print(cp.stdout)
        print(cp.stderr)
        raise SystemExit(f"command failed: {' '.join(cmd)}")


def build(binary: Path, workspace: Path, out_dir: Path, artifact: str):
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
        "--retained-ply", "1",
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
        "--emit-progress-log",
        "--emit-status-json",
        "--heartbeat-seconds", "1",
    ]
    run(cmd)
    return out_dir / artifact


def main():
    if len(sys.argv) != 3:
        raise SystemExit("usage: validate_practical_risk_prestockfish.py <binary> <workspace>")

    binary = Path(sys.argv[1]).resolve()
    workspace = Path(sys.argv[2]).resolve()

    bundle_a = build(binary, workspace, workspace / "out_prps_a", "a")
    bundle_b = build(binary, workspace, workspace / "out_prps_b", "b")

    manifest = json.loads((bundle_a / "manifest.json").read_text())
    assert manifest["artifact_role"] == "practical_risk_prestockfish"
    assert manifest["stockfish_used"] is False
    assert manifest["external_book_dependency_used"] is False

    status = json.loads((bundle_a / "progress" / "latest_status.json").read_text())
    assert status["stage"] == "finalize"
    assert status["stage_active"] is False

    db = sqlite3.connect(bundle_a / "practical_risk_prestockfish.sqlite")
    cur = db.cursor()

    tables = {r[0] for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    required = {"artifact_metadata", "roots", "root_moves", "deep_lines", "sigma_global_priors"}
    assert required.issubset(tables)

    initial_key = "rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq -"
    root = cur.execute("SELECT root_support FROM roots WHERE position_key=?", (initial_key,)).fetchone()
    assert root and root[0] == 12

    e4 = cur.execute("SELECT move_support, wins, draws, losses, mu, sigma_mode, n_qual FROM root_moves WHERE position_key=? AND move_uci='e2e4'", (initial_key,)).fetchone()
    assert e4[0] == 6 and e4[1] == 3 and e4[2] == 1 and e4[3] == 2
    assert abs(e4[4] - (3.5 / 6.0)) < 1e-9
    assert e4[5] == "observed"
    assert e4[6] >= 3

    d4 = cur.execute("SELECT sigma_mode, n_qual FROM root_moves WHERE position_key=? AND move_uci='d2d4'", (initial_key,)).fetchone()
    assert d4[0] == "shrunk_sparse"
    assert d4[1] == 2

    c4 = cur.execute("SELECT sigma_mode, n_qual FROM root_moves WHERE position_key=? AND move_uci='c2c4'", (initial_key,)).fetchone()
    assert c4[0] == "prior_only_sparse"
    assert c4[1] == 0

    prior_count = cur.execute("SELECT COUNT(*) FROM sigma_global_priors").fetchone()[0]
    assert prior_count >= 1

    deep_qual = cur.execute("SELECT COUNT(*) FROM deep_lines WHERE position_key=? AND root_move_uci='e2e4' AND qualifies_sigma=1", (initial_key,)).fetchone()[0]
    assert deep_qual >= 3

    db_b = sqlite3.connect(bundle_b / "practical_risk_prestockfish.sqlite")
    cur_b = db_b.cursor()
    sig_a = cur.execute("SELECT move_uci, sigma_deep, sigma_mode FROM root_moves WHERE position_key=? ORDER BY move_uci", (initial_key,)).fetchall()
    sig_b = cur_b.execute("SELECT move_uci, sigma_deep, sigma_mode FROM root_moves WHERE position_key=? ORDER BY move_uci", (initial_key,)).fetchall()
    assert sig_a == sig_b
    db.close()
    db_b.close()


if __name__ == "__main__":
    main()
