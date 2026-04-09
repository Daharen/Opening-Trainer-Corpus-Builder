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


def make_stage_a_fixture(bundle: Path):
    db = sqlite3.connect(bundle / "practical_risk_prestockfish.sqlite")
    cur = db.cursor()
    cur.executescript(
        """
        CREATE TABLE roots(position_key TEXT PRIMARY KEY, root_support INTEGER NOT NULL, side_to_move TEXT NOT NULL, rating_band TEXT NOT NULL, rating_policy TEXT NOT NULL, time_control_id TEXT NOT NULL, retained_ply INTEGER NOT NULL, deep_total_plies INTEGER NOT NULL, deep_own_plies INTEGER NOT NULL);
        CREATE TABLE root_moves(position_key TEXT NOT NULL, move_uci TEXT NOT NULL, move_support INTEGER NOT NULL, wins INTEGER NOT NULL, draws INTEGER NOT NULL, losses INTEGER NOT NULL, mu REAL NOT NULL, sigma_deep REAL NOT NULL, sigma_mode TEXT NOT NULL, ceiling REAL NOT NULL, n_qual INTEGER NOT NULL, popularity_rank INTEGER NOT NULL, PRIMARY KEY(position_key, move_uci));
        """
    )
    roots = [
        ("root_1", 10, "white", "1400-1600", "both_in_band", "600+0", 2, 6, 3),
        ("root_2", 10, "white", "1400-1600", "both_in_band", "600+0", 2, 6, 3),
    ]
    cur.executemany("INSERT INTO roots VALUES(?,?,?,?,?,?,?,?,?)", roots)
    moves = [
        ("root_1", "a1a2", 100, 60, 20, 20, 0.7, 0.1, "observed", 0.70, 8, 1),
        ("root_1", "a2a3", 90, 50, 20, 20, 0.6, 0.1, "observed", 0.50, 8, 2),
        ("root_1", "a3a4", 80, 40, 20, 20, 0.5, 0.1, "observed", 0.55, 8, 3),
        ("root_1", "a4a5", 70, 30, 20, 20, 0.4, 0.1, "observed", 0.45, 8, 4),
        ("root_2", "b1b2", 100, 60, 20, 20, 0.7, 0.1, "observed", 0.60, 8, 1),
        ("root_2", "b2b3", 90, 50, 20, 20, 0.6, 0.1, "observed", 0.80, 8, 2),
    ]
    cur.executemany("INSERT INTO root_moves VALUES(?,?,?,?,?,?,?,?,?,?,?,?)", moves)
    db.commit()
    db.close()


def make_stage_b_fixture(bundle: Path):
    db = sqlite3.connect(bundle / "practical_risk_stockfish_overlay.sqlite")
    cur = db.cursor()
    cur.executescript(
        """
        CREATE TABLE move_engine_evals(position_key TEXT NOT NULL, move_uci TEXT NOT NULL, move_support INTEGER NOT NULL, popularity_rank INTEGER NOT NULL, root_best_cp REAL NOT NULL, move_cp REAL NOT NULL, raw_loss_cp REAL NOT NULL, loss_cp REAL NOT NULL, engine_quality_class TEXT NOT NULL, ceiling REAL NOT NULL, is_engine_accepted INTEGER NOT NULL, is_engine_fail INTEGER NOT NULL, eval_source TEXT NOT NULL, cache_hit INTEGER NOT NULL, PRIMARY KEY(position_key, move_uci));
        CREATE TABLE root_direct_baselines(position_key TEXT PRIMARY KEY, accepted_baseline_move TEXT NULL, accepted_baseline_support INTEGER NOT NULL, accepted_baseline_rank INTEGER NOT NULL, baseline_found INTEGER NOT NULL, reason_code TEXT NOT NULL);
        CREATE TABLE root_engine_thresholds(position_key TEXT PRIMARY KEY, good_inclusive_min_ceiling REAL NULL, good_exclusive_min_ceiling REAL NULL, good_inclusive_min_move TEXT NULL, good_exclusive_min_move TEXT NULL, accepted_move_count INTEGER NOT NULL, accepted_move_count_good_inclusive INTEGER NOT NULL, accepted_move_count_good_exclusive INTEGER NOT NULL, failed_move_count INTEGER NOT NULL);
        """
    )
    evals = [
        ("root_1", "a1a2", 100, 1, 30.0, 30.0, 0.0, 0.0, "book", 0.70, 1, 0, "fixture", 0),
        ("root_1", "a2a3", 90, 2, 30.0, 10.0, 20.0, 20.0, "good", 0.50, 1, 0, "fixture", 0),
        ("root_1", "a3a4", 80, 3, 30.0, -20.0, 50.0, 50.0, "fail", 0.55, 0, 1, "fixture", 0),
        ("root_1", "a4a5", 70, 4, 30.0, -30.0, 60.0, 60.0, "fail", 0.45, 0, 1, "fixture", 0),
        ("root_2", "b1b2", 100, 1, 30.0, 10.0, 20.0, 20.0, "good", 0.60, 1, 0, "fixture", 0),
        ("root_2", "b2b3", 90, 2, 30.0, -30.0, 60.0, 60.0, "fail", 0.80, 0, 1, "fixture", 0),
    ]
    cur.executemany("INSERT INTO move_engine_evals VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)", evals)

    thresholds = [
        ("root_1", 0.50, 0.70, "a2a3", "a1a2", 2, 2, 1, 2),
        ("root_2", 0.60, None, "b1b2", None, 1, 1, 0, 1),
    ]
    cur.executemany("INSERT INTO root_engine_thresholds VALUES(?,?,?,?,?,?,?,?,?)", thresholds)

    baselines = [
        ("root_1", "a1a2", 100, 1, 1, "fixture"),
        ("root_2", "b1b2", 100, 1, 1, "fixture"),
    ]
    cur.executemany("INSERT INTO root_direct_baselines VALUES(?,?,?,?,?,?)", baselines)

    db.commit()
    db.close()


def build_stage_c(binary: Path, stage_a_bundle: Path, stage_b_bundle: Path, out_dir: Path, artifact: str):
    if out_dir.exists():
        shutil.rmtree(out_dir)
    cmd = [
        str(binary),
        "--prestockfish-bundle", str(stage_a_bundle),
        "--stockfish-overlay-bundle", str(stage_b_bundle),
        "--output-dir", str(out_dir),
        "--artifact-id", artifact,
        "--emit-progress-log",
        "--emit-status-json",
        "--heartbeat-seconds", "1",
    ]
    run(cmd)
    return out_dir / artifact


def validate_bundle(bundle: Path):
    manifest = json.loads((bundle / "manifest.json").read_text())
    assert manifest["artifact_role"] == "practical_risk_final"
    assert manifest["stockfish_used"] is False
    assert manifest["good_inclusive_policy"] is True
    assert manifest["good_exclusive_policy"] is True

    db = sqlite3.connect(bundle / "practical_risk_final.sqlite")
    cur = db.cursor()
    tables = {r[0] for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert {"final_move_admissions", "root_final_thresholds", "artifact_metadata"}.issubset(tables)

    rows = cur.execute(
        """
        SELECT
            position_key, move_uci, engine_quality_class, ceiling,
            good_inclusive_min_ceiling, good_exclusive_min_ceiling,
            admitted_if_good_accepted, admitted_if_good_rejected
        FROM final_move_admissions
        """
    ).fetchall()
    assert rows

    for position_key, move_uci, cls, ceiling, incl_min, excl_min, admit_incl, admit_excl in rows:
        if cls in {"book", "best", "excellent"}:
            assert admit_incl == 1
            assert admit_excl == 1
        elif cls == "good":
            assert admit_incl == 1
            assert admit_excl == 0
        elif cls == "fail":
            expect_incl = 1 if incl_min is not None and ceiling >= incl_min else 0
            expect_excl = 1 if excl_min is not None and ceiling >= excl_min else 0
            assert admit_incl == expect_incl
            assert admit_excl == expect_excl
        else:
            raise AssertionError(f"unexpected class {cls} for {position_key} {move_uci}")

    root_threshold_rows = cur.execute(
        """
        SELECT
            position_key,
            good_inclusive_min_ceiling,
            good_exclusive_min_ceiling,
            good_inclusive_min_move,
            good_exclusive_min_move
        FROM root_final_thresholds
        """
    ).fetchall()
    assert root_threshold_rows

    for position_key, incl_ceiling, excl_ceiling, incl_move, excl_move in root_threshold_rows:
        incl = cur.execute(
            """
            SELECT move_uci, ceiling
            FROM final_move_admissions
            WHERE position_key=? AND engine_quality_class IN ('book','best','excellent','good')
            ORDER BY ceiling ASC, popularity_rank ASC
            LIMIT 1
            """,
            (position_key,),
        ).fetchone()
        if incl is None:
            assert incl_ceiling is None
            assert incl_move is None
        else:
            assert abs(incl_ceiling - incl[1]) <= 1e-9
            assert incl_move == incl[0]

        excl = cur.execute(
            """
            SELECT move_uci, ceiling
            FROM final_move_admissions
            WHERE position_key=? AND engine_quality_class IN ('book','best','excellent')
            ORDER BY ceiling ASC, popularity_rank ASC
            LIMIT 1
            """,
            (position_key,),
        ).fetchone()
        if excl is None:
            assert excl_ceiling is None
            assert excl_move is None
            fail_accept_strict = cur.execute(
                """
                SELECT COUNT(*)
                FROM final_move_admissions
                WHERE position_key=? AND engine_quality_class='fail' AND admitted_if_good_rejected=1
                """,
                (position_key,),
            ).fetchone()[0]
            assert fail_accept_strict == 0
        else:
            assert abs(excl_ceiling - excl[1]) <= 1e-9
            assert excl_move == excl[0]

    count_rows = cur.execute(
        """
        SELECT
            position_key,
            total_move_count,
            accepted_move_count,
            failed_move_count,
            admitted_move_count_good_accepted,
            admitted_move_count_good_rejected,
            admitted_failed_move_count_good_accepted,
            admitted_failed_move_count_good_rejected
        FROM root_final_thresholds
        """
    ).fetchall()
    for (
        position_key,
        total_move_count,
        accepted_move_count,
        failed_move_count,
        admitted_move_count_good_accepted,
        admitted_move_count_good_rejected,
        admitted_failed_move_count_good_accepted,
        admitted_failed_move_count_good_rejected,
    ) in count_rows:
        recomputed = cur.execute(
            """
            SELECT
                COUNT(*),
                SUM(is_engine_accepted),
                SUM(is_engine_fail),
                SUM(admitted_if_good_accepted),
                SUM(admitted_if_good_rejected),
                SUM(CASE WHEN engine_quality_class='fail' THEN admitted_if_good_accepted ELSE 0 END),
                SUM(CASE WHEN engine_quality_class='fail' THEN admitted_if_good_rejected ELSE 0 END)
            FROM final_move_admissions
            WHERE position_key=?
            """,
            (position_key,),
        ).fetchone()
        assert total_move_count == recomputed[0]
        assert accepted_move_count == recomputed[1]
        assert failed_move_count == recomputed[2]
        assert admitted_move_count_good_accepted == recomputed[3]
        assert admitted_move_count_good_rejected == recomputed[4]
        assert admitted_failed_move_count_good_accepted == recomputed[5]
        assert admitted_failed_move_count_good_rejected == recomputed[6]

    db.close()


def main():
    if len(sys.argv) != 3:
        raise SystemExit("usage: validate_practical_risk_final.py <stage_c_binary> <workspace>")

    stage_c_binary = Path(sys.argv[1]).resolve()
    workspace = Path(sys.argv[2]).resolve()

    fixture_root = workspace / "tests" / "tmp_practical_risk_final_fixture"
    stage_a_bundle = fixture_root / "stage_a" / "a_fixture"
    stage_b_bundle = fixture_root / "stage_b" / "b_fixture"

    ensure_clean(stage_a_bundle)
    ensure_clean(stage_b_bundle)
    make_stage_a_fixture(stage_a_bundle)
    make_stage_b_fixture(stage_b_bundle)

    bundle = build_stage_c(stage_c_binary, stage_a_bundle, stage_b_bundle, fixture_root / "stage_c", "c_fixture")
    validate_bundle(bundle)


if __name__ == "__main__":
    main()
