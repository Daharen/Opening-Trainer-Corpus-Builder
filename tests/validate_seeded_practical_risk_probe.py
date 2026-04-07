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


def build(binary: Path, workspace: Path, out_dir: Path, artifact_id: str):
    if out_dir.exists():
        shutil.rmtree(out_dir)
    cmd = [
        str(binary),
        "--input-pgn", str(workspace / "tests" / "fixtures_seeded_probe_small.pgn"),
        "--output-dir", str(out_dir),
        "--artifact-id", artifact_id,
        "--min-rating", "1400",
        "--max-rating", "1600",
        "--rating-policy", "both_in_band",
        "--time-controls", "600+0",
        "--time-control-id", "600+0",
        "--initial-time-seconds", "600",
        "--increment-seconds", "0",
        "--time-format-label", "Rapid",
        "--probe-spec", str(workspace / "tests" / "fixtures_seeded_probe_spec.json"),
        "--emit-progress-log",
        "--emit-status-json",
    ]
    run(cmd)
    return out_dir / artifact_id


def main():
    if len(sys.argv) != 3:
        raise SystemExit("usage: validate_seeded_practical_risk_probe.py <binary> <workspace>")

    binary = Path(sys.argv[1]).resolve()
    workspace = Path(sys.argv[2]).resolve()

    out_a = workspace / "out_seeded_probe_a"
    out_b = workspace / "out_seeded_probe_b"
    bundle_a = build(binary, workspace, out_a, "probe_a")
    bundle_b = build(binary, workspace, out_b, "probe_b")

    manifest = json.loads((bundle_a / "manifest.json").read_text())
    assert manifest["artifact_role"] == "seeded_practical_risk_probe"
    assert manifest["stockfish_used"] is False
    assert manifest["external_book_dependency_used"] is False

    report = json.loads((bundle_a / "probe_report.json").read_text())
    probe = report["probes"][0]
    assert probe["games_reaching_probe_position"] == 4
    assert probe["candidate_entries"][0]["support_count"] == 2
    assert probe["baseline_entries"][0]["support_count"] == 2
    assert "comparison" in probe
    assert probe["comparison"]["provisional_accept"] is True

    assert (bundle_a / "progress" / "progress.log").exists()
    status = json.loads((bundle_a / "progress" / "latest_status.json").read_text())
    assert status["stage_active"] is False

    # deterministic replay signature
    report_b = json.loads((bundle_b / "probe_report.json").read_text())
    probe_b = report_b["probes"][0]
    sig_a = (probe["candidate_entries"][0]["recursive_eval_mean"], probe["comparison"]["candidate_ceiling"])
    sig_b = (probe_b["candidate_entries"][0]["recursive_eval_mean"], probe_b["comparison"]["candidate_ceiling"])
    assert sig_a == sig_b


if __name__ == "__main__":
    main()
