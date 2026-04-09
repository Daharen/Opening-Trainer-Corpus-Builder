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


def ensure_clean(path: Path):
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def make_stage_c_bundle(bundle: Path, band_id: str, rows):
    db = sqlite3.connect(bundle / "practical_risk_final.sqlite")
    cur = db.cursor()
    cur.executescript(
        """
        CREATE TABLE artifact_metadata(key TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE final_move_admissions(
            position_key TEXT NOT NULL,
            move_uci TEXT NOT NULL,
            move_support INTEGER NOT NULL,
            popularity_rank INTEGER NOT NULL,
            ceiling REAL NOT NULL,
            engine_quality_class TEXT NOT NULL,
            is_engine_accepted INTEGER NOT NULL,
            is_engine_fail INTEGER NOT NULL,
            raw_loss_cp REAL NOT NULL,
            loss_cp REAL NOT NULL,
            good_inclusive_min_move TEXT NULL,
            good_inclusive_min_ceiling REAL NULL,
            good_exclusive_min_move TEXT NULL,
            good_exclusive_min_ceiling REAL NULL,
            admitted_if_good_accepted INTEGER NOT NULL,
            admitted_if_good_rejected INTEGER NOT NULL,
            admission_reason_good_accepted TEXT NOT NULL,
            admission_reason_good_rejected TEXT NOT NULL,
            PRIMARY KEY(position_key, move_uci)
        );
        CREATE TABLE root_final_thresholds(
            position_key TEXT PRIMARY KEY,
            good_inclusive_min_move TEXT NULL,
            good_inclusive_min_ceiling REAL NULL,
            good_exclusive_min_move TEXT NULL,
            good_exclusive_min_ceiling REAL NULL,
            total_move_count INTEGER NOT NULL,
            accepted_move_count INTEGER NOT NULL,
            failed_move_count INTEGER NOT NULL,
            admitted_move_count_good_accepted INTEGER NOT NULL,
            admitted_move_count_good_rejected INTEGER NOT NULL,
            admitted_failed_move_count_good_accepted INTEGER NOT NULL,
            admitted_failed_move_count_good_rejected INTEGER NOT NULL
        );
        """
    )

    metadata = [
        ("artifact_role", "practical_risk_final"),
        ("time_control_id", "600+0"),
        ("time_control_family_id", "rapid"),
        ("policy_version_identity", "v1"),
        ("explanation_family_assumptions", "sharp_gambit"),
        ("dual_policy_model", "good_inclusive_good_exclusive"),
        ("rating_band", band_id),
    ]
    cur.executemany("INSERT INTO artifact_metadata(key, value) VALUES(?, ?)", metadata)

    cur.executemany(
        """
        INSERT INTO final_move_admissions(
            position_key, move_uci, move_support, popularity_rank, ceiling,
            engine_quality_class, is_engine_accepted, is_engine_fail,
            raw_loss_cp, loss_cp,
            good_inclusive_min_move, good_inclusive_min_ceiling,
            good_exclusive_min_move, good_exclusive_min_ceiling,
            admitted_if_good_accepted, admitted_if_good_rejected,
            admission_reason_good_accepted, admission_reason_good_rejected
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        rows,
    )

    total = len(rows)
    admitted_incl = sum(r[14] for r in rows)
    admitted_excl = sum(r[15] for r in rows)
    cur.execute(
        """
        INSERT INTO root_final_thresholds VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            "p1",
            "m1",
            0.6,
            "m1",
            0.65,
            total,
            total,
            0,
            admitted_incl,
            admitted_excl,
            0,
            0,
        ),
    )

    db.commit()
    db.close()



def validate_bundle(bundle: Path, expected_bulk_moves: int):
    manifest = json.loads((bundle / "manifest.json").read_text())
    assert manifest["artifact_role"] == "practical_risk_reconciled"
    assert manifest["success_threads_emitted"] is False
    assert manifest["failure_threads_emitted"] is True

    db = sqlite3.connect(bundle / "practical_risk_reconciled.sqlite")
    cur = db.cursor()

    tables = {r[0] for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert {"reconciled_move_admissions", "failure_explanations", "reconciled_root_summaries", "artifact_metadata"}.issubset(tables)

    # Downward propagation: move admitted in high is inherited in middle/low.
    low = cur.execute(
        """
        SELECT reconciled_admitted_if_good_accepted, admission_origin_good_accepted
        FROM reconciled_move_admissions
        WHERE band_id='1200-1399' AND position_key='p1' AND move_uci='m_high'
        """
    ).fetchone()
    assert low == (1, "inherited_from_higher_band")

    # No upward promotion: low-local admission should not admit high.
    high_lowmove = cur.execute(
        """
        SELECT reconciled_admitted_if_good_accepted
        FROM reconciled_move_admissions
        WHERE band_id='2000-2199' AND position_key='p1' AND move_uci='m_low'
        """
    ).fetchone()[0]
    assert high_lowmove == 0

    # Upward boundary fields.
    boundary = cur.execute(
        """
        SELECT first_failing_higher_band_good_accepted, first_failure_reason_good_accepted
        FROM reconciled_move_admissions
        WHERE band_id='1200-1399' AND position_key='p1' AND move_uci='m_low'
        """
    ).fetchone()
    assert boundary == ("1600-1799", "failed_move_below_good_inclusive_min")

    # Failure-only explanations: no row for accepted move/mode.
    accepted_explanations = cur.execute(
        """
        SELECT COUNT(*)
        FROM failure_explanations fe
        JOIN reconciled_move_admissions r
          ON r.band_id=fe.band_id AND r.position_key=fe.position_key AND r.move_uci=fe.move_uci
        WHERE fe.mode_id='good_inclusive' AND r.reconciled_admitted_if_good_accepted=1
        """
    ).fetchone()[0]
    assert accepted_explanations == 0

    # Strict-mode explanation for Good-only move.
    strict_row = cur.execute(
        """
        SELECT reason_code, template_id, toggle_state_required
        FROM failure_explanations
        WHERE band_id='1600-1799' AND position_key='p1' AND move_uci='m_good' AND mode_id='good_exclusive'
        """
    ).fetchone()
    assert strict_row == ("strict_mode_rejects_good", "FAIL_STRICT_MODE_REJECTS_GOOD", "sharp_on")

    # Threshold mapping.
    below_row = cur.execute(
        """
        SELECT reason_code, template_id
        FROM failure_explanations
        WHERE band_id='1600-1799' AND position_key='p1' AND move_uci='m_low' AND mode_id='good_inclusive'
        """
    ).fetchone()
    assert below_row == ("failed_below_threshold", "FAIL_BELOW_THRESHOLD")

    no_threshold_row = cur.execute(
        """
        SELECT reason_code, template_id
        FROM failure_explanations
        WHERE band_id='1200-1399' AND position_key='p1' AND move_uci='m_nothr' AND mode_id='good_inclusive'
        """
    ).fetchone()
    assert no_threshold_row == ("no_threshold_available", "FAIL_NO_THRESHOLD")

    # Toggle metadata representable.
    toggle_count = cur.execute("SELECT COUNT(*) FROM failure_explanations WHERE toggle_state_required IS NOT NULL").fetchone()[0]
    assert toggle_count > 0

    bulk_count = cur.execute("""
        SELECT COUNT(*)
        FROM reconciled_move_admissions
        WHERE position_key='p_bulk'
    """).fetchone()[0]
    assert bulk_count == expected_bulk_moves * 3

    bulk_summary = cur.execute("""
        SELECT COUNT(*)
        FROM reconciled_root_summaries
        WHERE position_key='p_bulk'
    """).fetchone()[0]
    assert bulk_summary == 3

    db.close()


def main():
    if len(sys.argv) != 3:
        raise SystemExit("usage: validate_practical_risk_reconciled.py <reconciled_binary> <workspace>")

    binary = Path(sys.argv[1]).resolve()
    workspace = Path(sys.argv[2]).resolve()

    fixture_root = workspace / "tests" / "tmp_practical_risk_reconciled_fixture"
    root = fixture_root / "stage_c_family"
    ensure_clean(root)

    high_bundle = root / "2000-2199"
    mid_bundle = root / "1600-1799"
    low_bundle = root / "1200-1399"
    high_bundle.mkdir(parents=True, exist_ok=True)
    mid_bundle.mkdir(parents=True, exist_ok=True)
    low_bundle.mkdir(parents=True, exist_ok=True)

    high_rows = [
        ("p1", "m_high", 100, 1, 0.80, "best", 1, 0, 0.0, 0.0, "m_high", 0.70, "m_high", 0.70, 1, 1, "accepted_engine_class", "accepted_engine_class"),
        ("p1", "m_low", 90, 2, 0.50, "fail", 0, 1, 60.0, 60.0, "m_high", 0.70, "m_high", 0.70, 0, 0, "failed_move_below_good_inclusive_min", "failed_move_below_good_exclusive_min"),
        ("p1", "m_good", 85, 3, 0.66, "good", 1, 0, 0.0, 0.0, "m_high", 0.70, "m_high", 0.70, 1, 0, "accepted_good_inclusive", "good_rejected_in_strict_mode"),
        ("p1", "m_below", 80, 4, 0.40, "fail", 0, 1, 80.0, 80.0, "m_high", 0.70, "m_high", 0.70, 0, 0, "failed_move_below_good_inclusive_min", "failed_move_below_good_exclusive_min"),
        ("p1", "m_nothr", 70, 5, 0.30, "fail", 0, 1, 90.0, 90.0, None, None, None, None, 0, 0, "no_good_inclusive_min_available", "no_good_exclusive_min_available"),
    ]
    mid_rows = [
        ("p1", "m_high", 100, 1, 0.80, "best", 1, 0, 0.0, 0.0, "m_high", 0.70, "m_high", 0.70, 0, 0, "failed_move_below_good_inclusive_min", "failed_move_below_good_exclusive_min"),
        ("p1", "m_low", 90, 2, 0.55, "fail", 0, 1, 55.0, 55.0, "m_high", 0.70, "m_high", 0.70, 0, 0, "failed_move_below_good_inclusive_min", "failed_move_below_good_exclusive_min"),
        ("p1", "m_good", 85, 3, 0.66, "good", 1, 0, 0.0, 0.0, "m_high", 0.70, "m_high", 0.70, 1, 0, "accepted_good_inclusive", "good_rejected_in_strict_mode"),
        ("p1", "m_below", 80, 4, 0.40, "fail", 0, 1, 80.0, 80.0, "m_high", 0.70, "m_high", 0.70, 0, 0, "failed_move_below_good_inclusive_min", "failed_move_below_good_exclusive_min"),
        ("p1", "m_nothr", 70, 5, 0.30, "fail", 0, 1, 90.0, 90.0, None, None, None, None, 0, 0, "no_good_inclusive_min_available", "no_good_exclusive_min_available"),
    ]
    low_rows = [
        ("p1", "m_high", 100, 1, 0.80, "best", 1, 0, 0.0, 0.0, "m_high", 0.70, "m_high", 0.70, 0, 0, "failed_move_below_good_inclusive_min", "failed_move_below_good_exclusive_min"),
        ("p1", "m_low", 90, 2, 0.72, "excellent", 1, 0, 0.0, 0.0, "m_high", 0.70, "m_high", 0.70, 1, 1, "accepted_engine_class", "accepted_engine_class"),
        ("p1", "m_good", 85, 3, 0.66, "good", 1, 0, 0.0, 0.0, "m_high", 0.70, "m_high", 0.70, 1, 0, "accepted_good_inclusive", "good_rejected_in_strict_mode"),
        ("p1", "m_below", 80, 4, 0.40, "fail", 0, 1, 80.0, 80.0, "m_high", 0.70, "m_high", 0.70, 0, 0, "failed_move_below_good_inclusive_min", "failed_move_below_good_exclusive_min"),
        ("p1", "m_nothr", 70, 5, 0.30, "fail", 0, 1, 90.0, 90.0, None, None, None, None, 0, 0, "no_good_inclusive_min_available", "no_good_exclusive_min_available"),
    ]


    bulk_move_count = 250
    for i in range(bulk_move_count):
        move = f"m_bulk_{i:04d}"
        high_rows.append(("p_bulk", move, 40, i + 1, 0.71, "good", 1, 0, 0.0, 0.0, move, 0.60, move, 0.60, 1, 0, "accepted_good_inclusive", "good_rejected_in_strict_mode"))
        mid_rows.append(("p_bulk", move, 38, i + 1, 0.69, "good", 1, 0, 0.0, 0.0, move, 0.60, move, 0.60, 1, 0, "accepted_good_inclusive", "good_rejected_in_strict_mode"))
        low_rows.append(("p_bulk", move, 35, i + 1, 0.68, "good", 1, 0, 0.0, 0.0, move, 0.60, move, 0.60, 1, 0, "accepted_good_inclusive", "good_rejected_in_strict_mode"))

    make_stage_c_bundle(high_bundle, "2000-2199", high_rows)
    make_stage_c_bundle(mid_bundle, "1600-1799", mid_rows)
    make_stage_c_bundle(low_bundle, "1200-1399", low_rows)

    out_dir = fixture_root / "out"
    if out_dir.exists():
        shutil.rmtree(out_dir)

    cmd = [
        str(binary),
        "--final-bundle-root", str(root),
        "--output-dir", str(out_dir),
        "--artifact-family-id", "family_fixture",
        "--time-control-id", "600+0",
        "--band-order", "2000-2199,1600-1799,1200-1399",
        "--emit-progress-log",
        "--emit-status-json",
        "--heartbeat-seconds", "1",
    ]
    run(cmd)

    validate_bundle(out_dir / "family_fixture", expected_bulk_moves=bulk_move_count)
    print("validate_practical_risk_reconciled: PASS")


if __name__ == "__main__":
    main()
