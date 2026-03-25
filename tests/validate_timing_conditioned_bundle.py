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
    for file_name in ["exact_corpus.sqlite", "behavioral_profile_set.sqlite"]:
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
        "compatibility_warnings",
    ]
    for field in required_fields:
        assert field in manifest, field

    assert (bundle_a / "data" / "exact_corpus.sqlite").exists()
    assert (bundle_a / "data" / "behavioral_profile_set.sqlite").exists()

    con = sqlite3.connect(bundle_a / "data" / "behavioral_profile_set.sqlite")
    try:
        assert con.execute("SELECT COUNT(*) FROM context_profile_map").fetchone()[0] > 0
        assert con.execute("SELECT COUNT(*) FROM move_pressure_profiles").fetchone()[0] > 0
        assert con.execute("SELECT COUNT(*) FROM think_time_profiles").fetchone()[0] > 0
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

    print("timing_conditioned_bundle_validation_ok")


if __name__ == "__main__":
    main()
