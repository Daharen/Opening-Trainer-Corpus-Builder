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
        "--risky-candidate-min-support", "1",
        "--risky-n-min", "1",
        "--emit-progress-log",
        "--emit-status-json",
    ]
    return run(cmd)


def collect_signature(sqlite_path: Path):
    con = sqlite3.connect(sqlite_path)
    try:
        rows = con.execute(
            """
            select band_id, policy_variant, scope_variant, position_key, move_key, allowed, reason_code
            from risky_acceptance_by_band
            order by band_id, policy_variant, scope_variant, position_key, move_key
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
    proc = build(binary, workspace, out_a)
    build(binary, workspace, out_b)

    bundle_a = next(out_a.iterdir())
    bundle_b = next(out_b.iterdir())

    manifest = json.loads((bundle_a / "manifest.json").read_text())
    if manifest.get("companion_payload_role") != "compact_risky_overlay_companion":
        raise SystemExit("manifest missing companion payload role")
    if manifest.get("full_external_opening_book_used") is not False:
        raise SystemExit("manifest should explicitly say full external opening book was not used")
    if manifest.get("retained_opening_scope_mode") != "retained_opening_window_only":
        raise SystemExit("manifest must explicitly state retained opening window scope")

    progress_log = bundle_a / "progress" / "progress.log"
    status_json = bundle_a / "progress" / "latest_status.json"
    if not progress_log.exists() or not status_json.exists():
        raise SystemExit("expected progress artifacts to be emitted")
    log_text = progress_log.read_text()
    if "compute-risky-overlay" not in log_text:
        raise SystemExit("expected risky stage progress lines")

    status = json.loads(status_json.read_text())
    for key in [
        "risky_positions_considered",
        "risky_candidate_fails_considered",
        "risky_candidates_skipped_support",
        "risky_candidates_evaluated",
        "risky_admitted_rows",
        "risky_unresolved_rows",
        "risky_rejected_rows",
    ]:
        if key not in status:
            raise SystemExit(f"missing risky progress key in status json: {key}")

    companion_a = bundle_a / "data" / "gambit_acceptance_companion.sqlite"
    companion_b = bundle_b / "data" / "gambit_acceptance_companion.sqlite"
    if not companion_a.exists() or not companion_b.exists():
        raise SystemExit("companion sqlite missing")

    con = sqlite3.connect(companion_a)
    try:
        ordinary = con.execute("select count(*) from ordinary_move_acceptance_by_band").fetchone()[0]
        baseline = con.execute("select count(*) from opening_variance_baseline_by_band").fetchone()[0]
        annotation = con.execute("select count(*) from sharp_move_annotation").fetchone()[0]
        metrics = con.execute("select count(*) from risky_entry_metrics").fetchone()[0]
        acceptance = con.execute("select count(*) from risky_acceptance_by_band").fetchone()[0]
        strict = con.execute("select count(*) from risky_entry_metrics where policy_variant='strict'").fetchone()[0]
        lenient = con.execute("select count(*) from risky_entry_metrics where policy_variant='lenient'").fetchone()[0]
        gambit_scope = con.execute("select count(*) from risky_acceptance_by_band where scope_variant='risky_gambit'").fetchone()[0]
        sharp_scope = con.execute("select count(*) from risky_acceptance_by_band where scope_variant='risky_sharp'").fetchone()[0]
        audit = con.execute("select count(*) from risky_acceptance_audit").fetchone()[0]
        non_fail_rows = con.execute(
            """
            select count(*) from risky_entry_metrics m
            join ordinary_move_acceptance_by_band o
              on o.band_id=m.band_id and o.policy_scope=m.policy_variant and o.position_key=m.position_key and o.move_key=m.move_key
            where o.grade != 'fail'
            """
        ).fetchone()[0]
        if ordinary <= 0 or baseline <= 0 or annotation <= 0:
            raise SystemExit("core companion tables are unexpectedly empty")
        if strict != lenient:
            raise SystemExit("strict/lenient policy emission should be symmetric for this fixture")
        if metrics > 0 and min(acceptance, sharp_scope, audit) <= 0:
            raise SystemExit("metrics emitted but acceptance/audit rows missing")
        if gambit_scope < 0:
            raise SystemExit("gambit scope query failed")
        if non_fail_rows != 0:
            raise SystemExit("candidate narrowing violated: non-fail rows evaluated in risky metrics")
    finally:
        con.close()

    if collect_signature(companion_a) != collect_signature(companion_b):
        raise SystemExit("companion payload is not deterministic across reruns")

    if "opening_book" in proc.stdout.lower() or "opening_book" in proc.stderr.lower():
        raise SystemExit("unexpected full-book dependency surfaced in build output")


if __name__ == "__main__":
    main()
