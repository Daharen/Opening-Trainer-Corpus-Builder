#!/usr/bin/env python3
import hashlib
import shutil
import sqlite3
import subprocess
import sys
from pathlib import Path


def run(cmd):
    cp = subprocess.run(cmd, text=True, capture_output=True)
    if cp.returncode != 0:
        raise RuntimeError(f"command failed: {' '.join(cmd)}\nstdout={cp.stdout}\nstderr={cp.stderr}")
    return cp.stdout


def fetchall(db_path, sql):
    con = sqlite3.connect(db_path)
    try:
        return con.execute(sql).fetchall()
    finally:
        con.close()


def digest(db_path):
    con = sqlite3.connect(db_path)
    try:
        h = hashlib.sha256()
        ordered_queries = [
            "SELECT * FROM profile_manifest ORDER BY schema_version, builder_version",
            "SELECT * FROM profile_build_inputs ORDER BY input_path",
            "SELECT * FROM move_pressure_profiles ORDER BY profile_id",
            "SELECT * FROM think_time_profiles ORDER BY profile_id",
            "SELECT * FROM context_profile_map ORDER BY time_control_id, mover_elo_band, clock_pressure_bucket, prev_opp_think_bucket, opening_ply_band",
            "SELECT * FROM profile_merge_history ORDER BY family, source_context_key, target_profile_id",
        ]
        for sql in ordered_queries:
            rows = con.execute(sql).fetchall()
            for row in rows:
                h.update(repr(row).encode("utf-8"))
        return h.hexdigest()
    finally:
        con.close()


def main():
    extract_exe = Path(sys.argv[1])
    profile_exe = Path(sys.argv[2])
    repo = Path(sys.argv[3])

    work = repo / "build" / "behavioral_profile_test"
    if work.exists():
        shutil.rmtree(work)
    work.mkdir(parents=True)

    extract_db = work / "extract.sqlite"
    run([
        str(extract_exe),
        "--input", str(repo / "tests" / "fixtures_timed_small.pgn"),
        "--output", str(extract_db),
        "--overwrite",
        "--time-controls", "300+2",
        "--month", "2024-01",
    ])

    out1 = work / "profiles1.sqlite"
    out2 = work / "profiles2.sqlite"

    out = run([
        str(profile_exe),
        "--input-extract", str(extract_db),
        "--output", str(out1),
        "--overwrite",
        "--time-controls", "300+2",
        "--month-window", "2024-01:2024-01",
        "--min-support", "1",
        "--merge-threshold", "0.18",
        "--log-every", "10",
        "--emit-fit-diagnostics",
    ])

    required = [
        "extract files loaded:",
        "raw move events seen:",
        "training examples accepted:",
        "contexts fitted:",
        "candidate profiles created:",
        "profiles merged:",
        "final move-pressure profiles emitted:",
        "final think-time profiles emitted:",
    ]
    for marker in required:
        assert marker in out, marker

    tables = {r[0] for r in fetchall(out1, "SELECT name FROM sqlite_master WHERE type='table'")}
    for name in [
        "profile_manifest",
        "profile_build_inputs",
        "move_pressure_profiles",
        "think_time_profiles",
        "context_profile_map",
        "profile_merge_history",
        "fit_diagnostics",
    ]:
        assert name in tables, name

    assert fetchall(out1, "SELECT COUNT(*) FROM move_pressure_profiles")[0][0] > 0
    assert fetchall(out1, "SELECT COUNT(*) FROM think_time_profiles")[0][0] > 0
    assert fetchall(out1, "SELECT COUNT(*) FROM context_profile_map")[0][0] > 0

    manifest = fetchall(out1, "SELECT schema_version,builder_version,fitting_method_version,merge_metric_version,merge_threshold_value FROM profile_manifest")
    assert manifest and manifest[0][0] == "1"

    mapping = fetchall(out1, "SELECT move_pressure_profile_id,think_time_profile_id FROM context_profile_map LIMIT 10")
    assert all(a and b for a, b in mapping)

    run([
        str(profile_exe),
        "--input-extract", str(extract_db),
        "--output", str(out2),
        "--overwrite",
        "--time-controls", "300+2",
        "--month-window", "2024-01:2024-01",
        "--min-support", "1",
        "--merge-threshold", "0.18",
        "--emit-fit-diagnostics",
    ])

    assert digest(out1) == digest(out2)

    # Merge determinism / sanity: with high threshold we should merge something.
    merged = fetchall(out1, "SELECT COUNT(*) FROM profile_merge_history")[0][0]
    assert merged >= 1

    print("behavioral_profile_validation_ok")


if __name__ == "__main__":
    main()
