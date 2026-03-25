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
        cur = con.execute(sql)
        return cur.fetchall()
    finally:
        con.close()


def digest_rows(rows):
    h = hashlib.sha256()
    for row in rows:
        h.update(repr(row).encode("utf-8"))
    return h.hexdigest()


def main():
    exe = Path(sys.argv[1])
    repo = Path(sys.argv[2])
    fixture = repo / "tests" / "fixtures_timed_small.pgn"

    work = repo / "build" / "behavioral_extract_test"
    if work.exists():
        shutil.rmtree(work)
    work.mkdir(parents=True)

    db1 = work / "extract1.sqlite"
    db2 = work / "extract2.sqlite"
    zst_like = work / "fixture_like.pgn.zst"
    zst_like.write_text(fixture.read_text())

    out = run([
        str(exe),
        "--input", str(fixture),
        "--input", str(zst_like),
        "--output", str(db1),
        "--overwrite",
        "--time-controls", "300+2",
        "--month", "2024-01",
    ])
    required = [
        "files processed:",
        "games seen:",
        "games accepted:",
        "games rejected:",
        "move events emitted:",
        "rows skipped due to missing clock data:",
        "rows skipped due to invalid time control:",
    ]
    for marker in required:
        assert marker in out, marker

    tables = {r[0] for r in fetchall(db1, "SELECT name FROM sqlite_master WHERE type='table'")}
    for name in ["extract_manifest", "source_files", "games", "move_events", "invalid_rows"]:
        assert name in tables, name

    tc = fetchall(db1, "SELECT DISTINCT time_control_id FROM move_events ORDER BY 1")
    assert tc == [("300+2",)]

    first_move = fetchall(db1, "SELECT is_first_move_for_side, opponent_previous_think_time_seconds FROM move_events WHERE game_ply_index=1 LIMIT 1")
    assert first_move and first_move[0][0] == 1 and first_move[0][1] is None

    run([
        str(exe),
        "--input", str(fixture),
        "--input", str(zst_like),
        "--output", str(db2),
        "--overwrite",
        "--time-controls", "300+2",
        "--month", "2024-01",
    ])

    rows1 = fetchall(db1, "SELECT game_id,source_file,source_month,game_ply_index,move_uci,think_time_seconds,mover_clock_ratio,think_time_ratio,clock_pressure_bucket,prev_opp_think_bucket FROM move_events ORDER BY source_file, game_id, game_ply_index")
    rows2 = fetchall(db2, "SELECT game_id,source_file,source_month,game_ply_index,move_uci,think_time_seconds,mover_clock_ratio,think_time_ratio,clock_pressure_bucket,prev_opp_think_bucket FROM move_events ORDER BY source_file, game_id, game_ply_index")
    assert digest_rows(rows1) == digest_rows(rows2)

    print("behavioral_extract_validation_ok")


if __name__ == "__main__":
    main()
