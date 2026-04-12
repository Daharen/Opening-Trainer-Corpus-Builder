import json
import sqlite3
from pathlib import Path

from tools.build_opening_locked_openings_artifact import build_artifact, parse_line, SourceRow


def _write_source(root: Path):
    rows = [
        ("B20", "Sicilian Defense: Najdorf Variation, English Attack", "1.e4 c5 2.Nf3 d6 3.d4 cxd4 4.Nxd4 Nf6 5.Nc3 a6"),
        ("B90", "Sicilian Defense: Najdorf Variation", "1.e4 c5 2.Nf3 d6 3.d4 cxd4 4.Nxd4 Nf6 5.Nc3"),
        ("C20", "King's Pawn Game", "1.e4 e5"),
    ]
    for fn in ["a.tsv", "b.tsv", "c.tsv", "d.tsv", "e.tsv"]:
        lines = ["eco\tname\tpgn"]
        if fn == "a.tsv":
            for eco, name, pgn in rows:
                lines.append(f"{eco}\t{name}\t{pgn}")
        (root / fn).write_text("\n".join(lines) + "\n", encoding="utf-8")


def _open_db(bundle: Path):
    return sqlite3.connect(bundle / "opening_locked_openings.sqlite")


def test_build_and_semantics(tmp_path: Path):
    src = tmp_path / "src"
    out = tmp_path / "out"
    src.mkdir(); out.mkdir()
    _write_source(src)

    bundle = build_artifact(src, out, "opening_locked_lichess_openings_v1")
    manifest = json.loads((bundle / "manifest.json").read_text())
    assert manifest["artifact_kind"] == "opening_locked_openings"
    assert manifest["artifact_schema_version"] == "1"
    assert manifest["total_exact_lines"] == 3

    con = _open_db(bundle)
    try:
        # synthetic family node exists
        kind = con.execute("select node_kind from opening_nodes where node_name='Sicilian Defense'").fetchone()[0]
        assert kind == "synthetic_family"

        # closure includes family -> exact leaf
        depth = con.execute(
            """
            select nc.depth
            from node_closure nc
            join opening_nodes a on a.node_id=nc.ancestor_node_id
            join opening_nodes d on d.node_id=nc.descendant_node_id
            where a.node_name='Sicilian Defense' and d.node_name='Sicilian Defense: Najdorf Variation, English Attack'
            """
        ).fetchone()[0]
        assert depth == 2

        # prefix membership includes initial position for family
        cnt = con.execute(
            """
            select count(*)
            from path_memberships pm
            join positions p on p.position_id=pm.position_id
            join opening_nodes n on n.node_id=pm.node_id
            where p.position_key='rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq -'
              and n.node_name='Sicilian Defense'
            """
        ).fetchone()[0]
        assert cnt == 1

        # classification: e4 in sicilian, e4 e5 leaves sicilian to other named opening
        e4_key = "rnbqkbnr/pppppppp/8/8/4P3/8/PPPP1PPP/RNBQKBNR b KQkq -"
        in_sicilian = con.execute(
            """
            select count(*) from path_memberships pm
            join positions p on p.position_id=pm.position_id
            join opening_nodes n on n.node_id=pm.node_id
            where p.position_key=? and n.node_name='Sicilian Defense'
            """,
            (e4_key,),
        ).fetchone()[0]
        assert in_sicilian == 1

        e4e5_key = "rnbqkbnr/pppp1ppp/8/4p3/4P3/8/PPPP1PPP/RNBQKBNR w KQkq -"
        in_sicilian_after_e5 = con.execute(
            """
            select count(*) from path_memberships pm
            join positions p on p.position_id=pm.position_id
            join opening_nodes n on n.node_id=pm.node_id
            where p.position_key=? and n.node_name='Sicilian Defense'
            """,
            (e4e5_key,),
        ).fetchone()[0]
        assert in_sicilian_after_e5 == 0

        named_after_e5 = con.execute(
            """
            select count(*) from path_memberships pm
            join positions p on p.position_id=pm.position_id
            where p.position_key=?
            """,
            (e4e5_key,),
        ).fetchone()[0]
        assert named_after_e5 > 0

        unnamed_key = "rnbqkbnr/pppppppp/8/8/4P3/8/PPPPQPPP/RNB1KBNR b KQkq -"
        unnamed_count = con.execute("select count(*) from positions where position_key=?", (unnamed_key,)).fetchone()[0]
        assert unnamed_count == 0

        # canonical continuation stable at root for Sicilian Defense is e2e4
        root_can = con.execute(
            """
            select nm.move_uci
            from node_moves nm
            join opening_nodes n on n.node_id=nm.node_id
            join positions p on p.position_id=nm.from_position_id
            where n.node_name='Sicilian Defense'
              and p.position_key='rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq -'
              and nm.is_canonical=1
            """
        ).fetchone()[0]
        assert root_can == "e2e4"
    finally:
        con.close()


def test_pgn_parse_to_uci_and_epd():
    row = SourceRow("a.tsv", 2, "B90", "Sicilian Defense", "1.e4 c5 2.Nf3")
    parsed = parse_line(row)
    assert parsed.uci_moves == ["e2e4", "c7c5", "g1f3"]
    assert parsed.position_keys[-1].startswith("rnbqkbnr/pp1ppppp/8/2p5/4P3/5N2")


def test_deterministic_rerun(tmp_path: Path):
    src = tmp_path / "src"
    out = tmp_path / "out"
    src.mkdir(); out.mkdir()
    _write_source(src)

    b1 = build_artifact(src, out, "b1")
    b2 = build_artifact(src, out, "b2")

    def dump(db):
        con = sqlite3.connect(db)
        try:
            data = []
            for t in ["source_files", "opening_nodes", "node_closure", "positions", "exact_lines", "path_memberships", "node_moves"]:
                cols = len(con.execute(f"pragma table_info({t})").fetchall())
                order_by = ','.join(str(i) for i in range(1, cols + 1))
                data.append(con.execute(f"select * from {t} order by {order_by}").fetchall())
            return data
        finally:
            con.close()

    assert dump(b1 / "opening_locked_openings.sqlite") == dump(b2 / "opening_locked_openings.sqlite")


def _write_family_source(root: Path):
    rows = [
        ("B90", "Sicilian Defense: Najdorf Variation", "1.e4 c5 2.Nf3 d6"),
        ("B91", "Sicilian Defense: Najdorf Variation, English Attack", "1.e4 c5 2.Nf3 d6 3.d4 cxd4 4.Nxd4"),
        ("A40", "Queen's Pawn Game", "1.d4 Nf6"),
        ("E20", "Nimzo-Indian Defense", "1.d4 Nf6 2.c4 e6 3.Nc3 Bb4"),
        ("E20", "Nimzo-Indian Defense: English Move Order", "1.c4 Nf6 2.Nc3 e6 3.d4 Bb4"),
    ]
    for fn in ["a.tsv", "b.tsv", "c.tsv", "d.tsv", "e.tsv"]:
        lines = ["eco\tname\tpgn"]
        if fn == "a.tsv":
            for eco, name, pgn in rows:
                lines.append(f"{eco}\t{name}\t{pgn}")
        (root / fn).write_text("\n".join(lines) + "\n", encoding="utf-8")


def test_family_artifact_derivation_and_manifest(tmp_path: Path):
    src = tmp_path / "src"
    out = tmp_path / "out"
    src.mkdir(); out.mkdir()
    _write_family_source(src)

    bundle = build_artifact(src, out, "opening_locked_lichess_openings_family_v1", artifact_kind="opening_locked_openings_family_v1")
    manifest = json.loads((bundle / "manifest.json").read_text())
    assert manifest["artifact_kind"] == "opening_locked_openings_family_v1"
    assert manifest["artifact_schema_version"] == "2"
    assert manifest["family_edge_count"] > 0
    assert manifest["transposition_edge_count"] > 0
    assert manifest["ui_tree_node_count"] > 0

    con = _open_db(bundle)
    try:
        # lexical hierarchy parent signal is preserved as an internal family edge
        lexical_edge = con.execute(
            """
            select count(*)
            from family_edges fe
            join opening_nodes p on p.node_id=fe.parent_node_id
            join opening_nodes c on c.node_id=fe.child_node_id
            where p.node_name='Sicilian Defense: Najdorf Variation'
              and c.node_name='Sicilian Defense: Najdorf Variation, English Attack'
              and fe.edge_kind='lexical_hierarchy'
            """
        ).fetchone()[0]
        assert lexical_edge == 1

        # earlier named opening on canonical line becomes a parent candidate
        named_prefix_edge = con.execute(
            """
            select count(*)
            from family_edges fe
            join opening_nodes p on p.node_id=fe.parent_node_id
            join opening_nodes c on c.node_id=fe.child_node_id
            where p.node_name='Queen''s Pawn Game'
              and c.node_name='Nimzo-Indian Defense'
              and fe.edge_kind='canonical_line_named_prefix'
            """
        ).fetchone()[0]
        assert named_prefix_edge == 1

        # transposition detection emits explicit shared-position edges
        transposition_cnt = con.execute(
            """
            select count(*)
            from transposition_edges te
            join opening_nodes a on a.node_id=te.from_node_id
            join opening_nodes b on b.node_id=te.to_node_id
            where ((a.node_name='Nimzo-Indian Defense' and b.node_name='Nimzo-Indian Defense: English Move Order')
                or (a.node_name='Nimzo-Indian Defense: English Move Order' and b.node_name='Nimzo-Indian Defense'))
              and te.shared_position_count > 0
              and te.earliest_shared_ply > 0
            """
        ).fetchone()[0]
        assert transposition_cnt == 1

        # ui_tree has exactly one canonical parent per child when multiple parent candidates exist
        ui_count = con.execute(
            """
            select count(*)
            from ui_tree ut
            join opening_nodes c on c.node_id=ut.child_node_id
            where c.node_name='Nimzo-Indian Defense'
            """
        ).fetchone()[0]
        assert ui_count == 1

        internal_parent_candidates = con.execute(
            """
            select count(*)
            from family_edges fe
            join opening_nodes c on c.node_id=fe.child_node_id
            where c.node_name='Nimzo-Indian Defense'
            """
        ).fetchone()[0]
        assert internal_parent_candidates >= 2
    finally:
        con.close()


def test_family_artifact_deterministic_rerun(tmp_path: Path):
    src = tmp_path / "src"
    out = tmp_path / "out"
    src.mkdir(); out.mkdir()
    _write_family_source(src)

    b1 = build_artifact(src, out, "family_run_1", artifact_kind="opening_locked_openings_family_v1")
    b2 = build_artifact(src, out, "family_run_2", artifact_kind="opening_locked_openings_family_v1")

    def dump_family(db):
        con = sqlite3.connect(db)
        try:
            data = []
            for t in ["family_edges", "family_memberships", "transposition_edges", "ui_tree"]:
                cols = len(con.execute(f"pragma table_info({t})").fetchall())
                order_by = ','.join(str(i) for i in range(1, cols + 1))
                data.append(con.execute(f"select * from {t} order by {order_by}").fetchall())
            return data
        finally:
            con.close()

    assert dump_family(b1 / "opening_locked_openings.sqlite") == dump_family(b2 / "opening_locked_openings.sqlite")
