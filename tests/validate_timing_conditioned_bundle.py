#!/usr/bin/env python3
import hashlib
import json
import shutil
import sqlite3
import subprocess
import sys
from pathlib import Path


def run(cmd, expect_ok=True):
    cp = subprocess.run(cmd, text=True, capture_output=True)
    if expect_ok and cp.returncode != 0:
        raise RuntimeError(f"command failed: {' '.join(cmd)}\nstdout={cp.stdout}\nstderr={cp.stderr}")
    return cp


def digest_bundle(bundle_path: Path) -> str:
    h = hashlib.sha256()
    manifest_bytes = (bundle_path / "manifest.json").read_bytes()
    h.update(manifest_bytes)
    for file_name in ["corpus.sqlite", "exact_corpus.sqlite", "behavioral_profile_set.sqlite", "timing_overlay.json"]:
        h.update((bundle_path / "data" / file_name).read_bytes())
    return h.hexdigest()


def main():
    corpus_exe = Path(sys.argv[1])
    extract_exe = Path(sys.argv[2])
    profile_exe = Path(sys.argv[3])
    emitter_exe = Path(sys.argv[4])
    repo = Path(sys.argv[5])

    work = repo / "build" / "timing_conditioned_bundle_test"
    if work.exists():
        shutil.rmtree(work)
    work.mkdir(parents=True)

    corpus_out = work / "corpus_out"
    run([
        str(corpus_exe),
        "--mode", "aggregate-counts",
        "--input-pgn", str(repo / "tests" / "fixtures_tiny.pgn"),
        "--output-dir", str(corpus_out),
        "--min-rating", "1000",
        "--max-rating", "2000",
        "--rating-policy", "both_in_band",
        "--retained-ply", "12",
        "--threads", "1",
        "--position-key-format", "fen_normalized",
        "--move-key-format", "uci",
        "--payload-format", "sqlite",
    ])
    corpus_bundle = next(corpus_out.iterdir())

    extract_db = work / "extract.sqlite"
    run([
        str(extract_exe),
        "--input", str(repo / "tests" / "fixtures_timed_small.pgn"),
        "--output", str(extract_db),
        "--overwrite",
        "--time-controls", "300+2",
        "--month", "2024-01",
    ])

    profile_db = work / "profile.sqlite"
    run([
        str(profile_exe),
        "--input-extract", str(extract_db),
        "--output", str(profile_db),
        "--overwrite",
        "--time-controls", "300+2",
        "--month-window", "2024-01:2024-01",
        "--min-support", "1",
    ])

    bundle_a = work / "bundle_a"
    out_a = run([
        str(emitter_exe),
        "--input-corpus-bundle", str(corpus_bundle),
        "--input-profile-set", str(profile_db),
        "--output", str(bundle_a),
        "--overwrite",
        "--artifact-id", "timing_fixture",
        "--prototype-label", "test-lane",
        "--time-controls", "300+2",
        "--elo-bands", "1000-2000",
        "--emit-progress-log",
        "--emit-status-json",
    ]).stdout

    for marker in [
        "corpus artifacts loaded:",
        "profile artifacts loaded:",
        "positions examined:",
        "move rows examined:",
        "contexts mapped:",
        "profiles referenced:",
        "compatibility warnings surfaced:",
        "final bundle emitted path:",
    ]:
        assert marker in out_a, marker

    manifest = json.loads((bundle_a / "manifest.json").read_text())
    required_fields = [
        "artifact_schema_version",
        "artifact_id",
        "emitter_version",
        "builder_repo_commit",
        "build_timestamp_utc",
        "exact_corpus_artifact_identity",
        "behavioral_profile_set_artifact_identity",
        "timing_overlay_policy_version",
        "context_key_contract_version",
        "build_status",
        "payload_format",
        "position_key_format",
        "move_key_format",
        "retained_ply_depth",
        "sqlite_corpus_file",
        "corpus_sqlite_file",
        "payload_file",
        "payload_status",
        "timing_overlay_file",
        "compatibility_warnings",
    ]
    for field in required_fields:
        assert field in manifest, field

    assert (bundle_a / "data" / "corpus.sqlite").exists()
    assert (bundle_a / "data" / "exact_corpus.sqlite").exists()
    assert (bundle_a / "data" / "behavioral_profile_set.sqlite").exists()
    assert (bundle_a / "data" / "timing_overlay.json").exists()
    assert manifest["build_status"] == "aggregation_complete"
    assert manifest["payload_format"] == "sqlite"
    assert manifest["position_key_format"] == "fen_normalized"
    assert manifest["move_key_format"] == "uci"
    assert manifest["sqlite_corpus_file"] == "data/corpus.sqlite"
    assert manifest["corpus_sqlite_file"] == "data/corpus.sqlite"
    assert manifest["payload_file"] == "data/corpus.sqlite"
    assert manifest["timing_overlay_file"] == "data/timing_overlay.json"

    overlay = json.loads((bundle_a / "data" / "timing_overlay.json").read_text())
    assert overlay["context_contract_version"] == manifest["context_key_contract_version"]
    assert overlay["timing_overlay_policy_version"] == manifest["timing_overlay_policy_version"]
    assert isinstance(overlay["move_pressure_profiles"], dict)
    assert isinstance(overlay["think_time_profiles"], dict)
    assert isinstance(overlay["context_profile_map"], dict)
    assert overlay["move_pressure_profiles"]
    assert overlay["think_time_profiles"]
    assert overlay["context_profile_map"]
    sample_move = next(iter(overlay["move_pressure_profiles"].values()))
    for field in ["pressure_sensitivity", "decisiveness", "move_diversity"]:
        assert field in sample_move
    sample_think = next(iter(overlay["think_time_profiles"].values()))
    for field in ["base_time_scale", "spread", "short_mass", "deep_think_tail_mass", "timeout_tail_mass"]:
        assert field in sample_think
    sample_context_key, sample_context_value = next(iter(overlay["context_profile_map"].items()))
    assert sample_context_key.count("|") == 4
    assert "move_pressure_profile_id" in sample_context_value
    assert "think_time_profile_id" in sample_context_value

    con = sqlite3.connect(bundle_a / "data" / "behavioral_profile_set.sqlite")
    try:
        assert con.execute("SELECT COUNT(*) FROM context_profile_map").fetchone()[0] > 0
        assert con.execute("SELECT COUNT(*) FROM move_pressure_profiles").fetchone()[0] > 0
        assert con.execute("SELECT COUNT(*) FROM think_time_profiles").fetchone()[0] > 0
    finally:
        con.close()

    con = sqlite3.connect(bundle_a / "data" / "corpus.sqlite")
    try:
        assert con.execute("SELECT COUNT(*) FROM positions").fetchone()[0] > 0
        assert con.execute("SELECT COUNT(*) FROM moves").fetchone()[0] > 0
    finally:
        con.close()

    bundle_b = work / "bundle_b"
    run([
        str(emitter_exe),
        "--input-corpus-bundle", str(corpus_bundle),
        "--input-profile-set", str(profile_db),
        "--output", str(bundle_b),
        "--overwrite",
        "--artifact-id", "timing_fixture",
        "--prototype-label", "test-lane",
        "--time-controls", "300+2",
        "--elo-bands", "1000-2000",
        "--emit-progress-log",
        "--emit-status-json",
    ])
    assert digest_bundle(bundle_a) == digest_bundle(bundle_b)

    bad = run([
        str(emitter_exe),
        "--input-corpus-bundle", str(corpus_bundle),
        "--input-profile-set", str(profile_db),
        "--output", str(work / "bad_bundle"),
        "--overwrite",
        "--time-controls", "10+0",
    ], expect_ok=False)
    assert bad.returncode != 0

    proto = run([
        str(emitter_exe),
        "--input-corpus-bundle", str(corpus_bundle),
        "--input-profile-set", str(profile_db),
        "--output", str(work / "proto_bundle"),
        "--overwrite",
        "--time-controls", "10+0",
        "--allow-prototype-mismatch",
        "--prototype-label", "allow-mismatch",
    ])
    assert proto.returncode == 0

    hotfix_work = work / "alias_hotfix"
    hotfix_work.mkdir(parents=True, exist_ok=True)

    alias_corpus_bundle = hotfix_work / "corpus_bundle"
    (alias_corpus_bundle / "data").mkdir(parents=True, exist_ok=True)
    alias_corpus_sqlite = alias_corpus_bundle / "data" / "corpus.sqlite"
    con = sqlite3.connect(alias_corpus_sqlite)
    try:
        con.execute("CREATE TABLE positions(position_key TEXT PRIMARY KEY, total_count INTEGER NOT NULL)")
        con.execute("CREATE TABLE moves(position_key TEXT NOT NULL, move_uci TEXT NOT NULL, move_count INTEGER NOT NULL)")
        con.execute("INSERT INTO positions(position_key,total_count) VALUES('fen',1)")
        con.execute("INSERT INTO moves(position_key,move_uci,move_count) VALUES('fen','e2e4',1)")
        con.commit()
    finally:
        con.close()
    (alias_corpus_bundle / "manifest.json").write_text(json.dumps({
        "artifact_id": "alias_corpus",
        "position_key_format": "fen_normalized",
        "move_key_format": "uci",
        "retained_opening_ply": 12,
        "rating_lower_bound": 400,
        "rating_upper_bound": 1300,
        "rating_policy": "both_in_band",
        "raw_counts_preserved": True,
        "payload_format": "sqlite",
        "source_corpus_identity": "test",
        "input_pgn_path": "test_input.pgn",
        "aggregate_sqlite_file": "data/corpus.sqlite",
    }, indent=2))

    alias_profile = hotfix_work / "profile.sqlite"
    con = sqlite3.connect(alias_profile)
    try:
        con.executescript("""
            CREATE TABLE profile_manifest (
                schema_version TEXT,
                builder_version TEXT,
                source_month_window_summary TEXT,
                rating_policy_version TEXT,
                fitting_method_version TEXT,
                merge_metric_version TEXT,
                merge_threshold_value TEXT
            );
            INSERT INTO profile_manifest VALUES ('1','profile_alias_fixture','2024-01:2024-01','rating_v1','fit_v1','merge_v1','0.05');
            CREATE TABLE move_pressure_profiles (
                profile_id TEXT PRIMARY KEY,
                pressure_sensitivity REAL,
                decisiveness REAL,
                move_diversity REAL
            );
            CREATE TABLE think_time_profiles (
                profile_id TEXT PRIMARY KEY,
                base_time_scale REAL,
                spread REAL,
                short_mass REAL,
                deep_think_tail_mass REAL,
                timeout_tail_mass REAL
            );
            CREATE TABLE context_profile_map (
                time_control_id TEXT,
                mover_elo_band TEXT,
                clock_pressure_bucket TEXT,
                prev_opp_think_bucket TEXT,
                opening_ply_band TEXT,
                support_count INTEGER,
                move_pressure_profile_id TEXT,
                think_time_profile_id TEXT
            );
        """)
        con.executemany("INSERT INTO move_pressure_profiles VALUES(?,?,?,?)", [
            ("mp_400_599", 0.1, 0.2, 0.3),
            ("mp_600_799", 0.2, 0.3, 0.4),
            ("mp_1000_1199", 0.3, 0.4, 0.5),
            ("mp_1200_1399", 0.4, 0.5, 0.6),
        ])
        con.executemany("INSERT INTO think_time_profiles VALUES(?,?,?,?,?,?)", [
            ("tt_400_599", 1.0, 0.1, 0.1, 0.1, 0.1),
            ("tt_600_799", 1.1, 0.2, 0.2, 0.2, 0.2),
            ("tt_1000_1199", 1.2, 0.3, 0.3, 0.3, 0.3),
            ("tt_1200_1399", 1.3, 0.4, 0.4, 0.4, 0.4),
        ])
        con.executemany("INSERT INTO context_profile_map VALUES(?,?,?,?,?,?,?,?)", [
            ("600+0", "400-599", "comfortable", "none", "01-10", 8, "mp_400_599", "tt_400_599"),
            ("600+0", "600-799", "comfortable", "none", "01-10", 5, "mp_600_799", "tt_600_799"),
            ("600+0", "1000-1199", "comfortable", "none", "01-10", 9, "mp_1000_1199", "tt_1000_1199"),
            ("600+0", "1200-1399", "comfortable", "none", "01-10", 6, "mp_1200_1399", "tt_1200_1399"),
        ])
        con.commit()
    finally:
        con.close()
    source_profile_hash = hashlib.sha256(alias_profile.read_bytes()).hexdigest()

    alias_bundle_a = hotfix_work / "bundle_a"
    run([
        str(emitter_exe),
        "--input-corpus-bundle", str(alias_corpus_bundle),
        "--input-profile-set", str(alias_profile),
        "--output", str(alias_bundle_a),
        "--overwrite",
        "--artifact-id", "alias_hotfix_fixture",
        "--time-controls", "600+0",
        "--elo-bands", "400-600",
        "--elo-bands", "1000-1200",
    ])
    alias_manifest = json.loads((alias_bundle_a / "manifest.json").read_text())
    assert alias_manifest["timing_runtime_elo_band_policy_version"] == "derived_200_point_bucket_v1"
    assert alias_manifest["timing_display_elo_band"] == "400-600,1000-1200"
    assert alias_manifest["timing_overlay_alias_mode"] == "context_profile_map_alias_export_v1"
    assert alias_manifest["timing_runtime_elo_band_vocabulary"] == ["1000-1199", "1200-1399", "400-599", "600-799"]
    assert alias_manifest["timing_display_to_runtime_elo_band_aliases"]["400-600"] == ["400-599", "600-799"]
    assert alias_manifest["timing_display_to_runtime_elo_band_aliases"]["1000-1200"] == ["1000-1199", "1200-1399"]
    assert alias_manifest["timing_overlay_alias_conflicts"]

    alias_overlay = json.loads((alias_bundle_a / "data" / "timing_overlay.json").read_text())
    cmap = alias_overlay["context_profile_map"]
    assert "600+0|400-599|comfortable|none|01-10" in cmap
    assert "600+0|600-799|comfortable|none|01-10" in cmap
    assert "600+0|400-600|comfortable|none|01-10" in cmap
    assert "600+0|1000-1200|comfortable|none|01-10" in cmap
    assert cmap["600+0|400-600|comfortable|none|01-10"] == {
        "move_pressure_profile_id": "mp_400_599",
        "think_time_profile_id": "tt_400_599",
    }
    assert cmap["600+0|1000-1200|comfortable|none|01-10"] == {
        "move_pressure_profile_id": "mp_1000_1199",
        "think_time_profile_id": "tt_1000_1199",
    }

    alias_bundle_b = hotfix_work / "bundle_b"
    run([
        str(emitter_exe),
        "--input-corpus-bundle", str(alias_corpus_bundle),
        "--input-profile-set", str(alias_profile),
        "--output", str(alias_bundle_b),
        "--overwrite",
        "--artifact-id", "alias_hotfix_fixture",
        "--time-controls", "600+0",
        "--elo-bands", "400-600",
        "--elo-bands", "1000-1200",
    ])
    assert digest_bundle(alias_bundle_a) == digest_bundle(alias_bundle_b)
    assert source_profile_hash == hashlib.sha256(alias_profile.read_bytes()).hexdigest()

    print("timing_conditioned_bundle_validation_ok")


if __name__ == "__main__":
    main()
