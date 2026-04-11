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
