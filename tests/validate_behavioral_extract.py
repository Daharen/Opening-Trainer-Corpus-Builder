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


def accepted_by_index(db_path):
    rows = fetchall(db_path, "SELECT source_game_index, accepted, rejection_reason FROM games ORDER BY source_game_index")
    return {idx: (accepted, reason) for (idx, accepted, reason) in rows}


def main():
    exe = Path(sys.argv[1])
    repo = Path(sys.argv[2])
    zstd_enabled = bool(int(sys.argv[3])) if len(sys.argv) > 3 else False
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
        "--output", str(db2),
        "--overwrite",
        "--time-controls", "300+2",
        "--month", "2024-01",
    ])

    rows1 = fetchall(db1, "SELECT game_id,source_file,source_month,game_ply_index,move_uci,think_time_seconds,mover_clock_ratio,think_time_ratio,clock_pressure_bucket,prev_opp_think_bucket FROM move_events ORDER BY source_file, game_id, game_ply_index")
    rows2 = fetchall(db2, "SELECT game_id,source_file,source_month,game_ply_index,move_uci,think_time_seconds,mover_clock_ratio,think_time_ratio,clock_pressure_bucket,prev_opp_think_bucket FROM move_events ORDER BY source_file, game_id, game_ply_index")
    assert digest_rows(rows1) == digest_rows(rows2)

    if zstd_enabled:
        run([
            str(exe),
            "--input", str(zst_like),
            "--output", str(work / "extract_zstd.sqlite"),
            "--overwrite",
            "--time-controls", "300+2",
            "--month", "2024-01",
        ])
    else:
        cp = subprocess.run([
            str(exe),
            "--input", str(zst_like),
            "--output", str(work / "extract_zstd.sqlite"),
            "--overwrite",
            "--time-controls", "300+2",
            "--month", "2024-01",
        ], text=True, capture_output=True)
        assert cp.returncode != 0
        combined = f"{cp.stdout}\n{cp.stderr}"
        assert "does not include zstd input support" in combined


    range_fixture = repo / "tests" / "fixtures_timed_elo_ranges.pgn"

    def run_range_case(name, extra_args):
        out_db = work / f"{name}.sqlite"
        run([
            str(exe),
            "--input", str(range_fixture),
            "--output", str(out_db),
            "--overwrite",
            "--time-controls", "300+2",
            "--elo-bands", "1000-1200",
            *extra_args,
        ])
        return out_db

    both_db = run_range_case("elo_both", ["--rating-policy", "both_in_band"])
    both = accepted_by_index(both_db)
    assert both[1][0] == 0 and both[1][1] == "filtered_elo_range"
    assert both[2][0] == 0 and both[2][1] == "filtered_elo_range"
    assert both[3][0] == 1 and both[3][1] is None

    white_db = run_range_case("elo_white", ["--rating-policy", "white_in_band"])
    white = accepted_by_index(white_db)
    assert white[1][0] == 1
    assert white[2][0] == 0 and white[2][1] == "filtered_elo_range"
    assert white[3][0] == 1

    black_db = run_range_case("elo_black", ["--rating-policy", "black_in_band"])
    black = accepted_by_index(black_db)
    assert black[1][0] == 0 and black[1][1] == "filtered_elo_range"
    assert black[2][0] == 1
    assert black[3][0] == 1

    union_db = work / "elo_union.sqlite"
    run([
        str(exe),
        "--input", str(range_fixture),
        "--output", str(union_db),
        "--overwrite",
        "--time-controls", "300+2",
        "--rating-policy", "both_in_band",
        "--elo-bands", "1000-1200",
        "--elo-bands", "850-950",
    ])
    union_rows = accepted_by_index(union_db)
    assert union_rows[1][0] == 1
    assert union_rows[2][0] == 1
    assert union_rows[3][0] == 1

    cp = subprocess.run([
        str(exe),
        "--input", str(range_fixture),
        "--output", str(work / "elo_bad.sqlite"),
        "--overwrite",
        "--time-controls", "300+2",
        "--elo-bands", "1000-",
    ], text=True, capture_output=True)
    assert cp.returncode != 0
    assert "Invalid --elo-bands range" in (cp.stdout + cp.stderr)

    print("behavioral_extract_validation_ok")


if __name__ == "__main__":
    main()
