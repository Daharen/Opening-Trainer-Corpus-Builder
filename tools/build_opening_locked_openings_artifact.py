#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
import json
import re
import sqlite3
import subprocess
from dataclasses import dataclass
from pathlib import Path

FILES = ["a.tsv", "b.tsv", "c.tsv", "d.tsv", "e.tsv"]
FILES_ORDER = {f: i for i, f in enumerate(FILES)}


@dataclass(frozen=True)
class SourceRow:
    source_file: str
    source_row_number: int
    eco: str
    name: str
    pgn: str


@dataclass
class ParsedLine:
    row: SourceRow
    uci_moves: list[str]
    position_keys: list[str]
    epds: list[str]


class Board:
    def __init__(self):
        self.board = {}
        self.turn = "w"
        self.castling = set("KQkq")
        self.ep_square = None
        self._setup()

    def _setup(self):
        pieces = "RNBQKBNR"
        for i, p in enumerate(pieces):
            self.board[i] = p
            self.board[8 + i] = "P"
            self.board[48 + i] = "p"
            self.board[56 + i] = p.lower()

    def clone(self):
        b = Board.__new__(Board)
        b.board = dict(self.board)
        b.turn = self.turn
        b.castling = set(self.castling)
        b.ep_square = self.ep_square
        return b

    @staticmethod
    def sq(file_idx, rank_idx):
        return rank_idx * 8 + file_idx

    @staticmethod
    def sq_name(sq):
        return f"{'abcdefgh'[sq % 8]}{sq // 8 + 1}"

    @staticmethod
    def parse_sq(name):
        return (int(name[1]) - 1) * 8 + "abcdefgh".index(name[0])

    def _is_legal_ep(self, ep_sq):
        direction = 1 if self.turn == "w" else -1
        rank = ep_sq // 8
        for df in (-1, 1):
            f = ep_sq % 8 + df
            r = rank - direction
            if 0 <= f < 8 and 0 <= r < 8:
                if self.board.get(self.sq(f, r)) == ("P" if self.turn == "w" else "p"):
                    return True
        return False

    def fen_parts(self):
        rows = []
        for r in range(7, -1, -1):
            empty = 0
            out = ""
            for f in range(8):
                p = self.board.get(self.sq(f, r))
                if p is None:
                    empty += 1
                else:
                    if empty:
                        out += str(empty)
                        empty = 0
                    out += p
            if empty:
                out += str(empty)
            rows.append(out)
        castling = "".join(c for c in "KQkq" if c in self.castling) or "-"
        ep = "-"
        if self.ep_square is not None and self._is_legal_ep(self.ep_square):
            ep = self.sq_name(self.ep_square)
        return "/".join(rows), self.turn, castling, ep

    def position_key(self):
        return " ".join(self.fen_parts())

    def epd(self):
        return self.position_key()

    def is_attacked(self, sq, by):
        is_white = by == "w"
        pawn = "P" if is_white else "p"
        knight = "N" if is_white else "n"
        bishop = "B" if is_white else "b"
        rook = "R" if is_white else "r"
        queen = "Q" if is_white else "q"
        king = "K" if is_white else "k"
        rank, file = sq // 8, sq % 8

        for df in (-1, 1):
            rr = rank - (1 if is_white else -1)
            ff = file + df
            if 0 <= rr < 8 and 0 <= ff < 8 and self.board.get(self.sq(ff, rr)) == pawn:
                return True

        for dr, df in [(-2, -1), (-2, 1), (-1, -2), (-1, 2), (1, -2), (1, 2), (2, -1), (2, 1)]:
            rr, ff = rank + dr, file + df
            if 0 <= rr < 8 and 0 <= ff < 8 and self.board.get(self.sq(ff, rr)) == knight:
                return True

        sliders = [
            (-1, -1, (bishop, queen)), (-1, 1, (bishop, queen)), (1, -1, (bishop, queen)), (1, 1, (bishop, queen)),
            (-1, 0, (rook, queen)), (1, 0, (rook, queen)), (0, -1, (rook, queen)), (0, 1, (rook, queen)),
        ]
        for dr, df, pieces in sliders:
            rr, ff = rank + dr, file + df
            while 0 <= rr < 8 and 0 <= ff < 8:
                p = self.board.get(self.sq(ff, rr))
                if p is not None:
                    if p in pieces:
                        return True
                    break
                rr += dr
                ff += df

        for dr in (-1, 0, 1):
            for df in (-1, 0, 1):
                if dr == 0 and df == 0:
                    continue
                rr, ff = rank + dr, file + df
                if 0 <= rr < 8 and 0 <= ff < 8 and self.board.get(self.sq(ff, rr)) == king:
                    return True
        return False

    def in_check(self, side):
        k = "K" if side == "w" else "k"
        king_sq = next((sq for sq, p in self.board.items() if p == k), None)
        if king_sq is None:
            raise ValueError("missing king")
        return self.is_attacked(king_sq, "b" if side == "w" else "w")

    def apply_uci(self, uci):
        src = self.parse_sq(uci[:2])
        dst = self.parse_sq(uci[2:4])
        promo = uci[4] if len(uci) == 5 else None
        piece = self.board.pop(src)
        captured = self.board.get(dst)

        if piece in ("P", "p") and self.ep_square is not None and dst == self.ep_square and captured is None and src % 8 != dst % 8:
            self.board.pop(dst - 8 if piece == "P" else dst + 8, None)

        if piece == "K":
            self.castling.discard("K")
            self.castling.discard("Q")
            if src == self.parse_sq("e1") and dst == self.parse_sq("g1"):
                self.board[self.parse_sq("f1")] = self.board.pop(self.parse_sq("h1"))
            elif src == self.parse_sq("e1") and dst == self.parse_sq("c1"):
                self.board[self.parse_sq("d1")] = self.board.pop(self.parse_sq("a1"))
        elif piece == "k":
            self.castling.discard("k")
            self.castling.discard("q")
            if src == self.parse_sq("e8") and dst == self.parse_sq("g8"):
                self.board[self.parse_sq("f8")] = self.board.pop(self.parse_sq("h8"))
            elif src == self.parse_sq("e8") and dst == self.parse_sq("c8"):
                self.board[self.parse_sq("d8")] = self.board.pop(self.parse_sq("a8"))

        for sq, flag in [("a1", "Q"), ("h1", "K"), ("a8", "q"), ("h8", "k")]:
            s = self.parse_sq(sq)
            if src == s or dst == s:
                self.castling.discard(flag)

        if promo:
            piece = promo.upper() if piece.isupper() else promo.lower()
        self.board[dst] = piece

        self.ep_square = None
        if piece == "P" and src // 8 == 1 and dst // 8 == 3:
            self.ep_square = src + 8
        elif piece == "p" and src // 8 == 6 and dst // 8 == 4:
            self.ep_square = src - 8

        self.turn = "b" if self.turn == "w" else "w"

    def _path_clear(self, src, dst, dr, df):
        rr, ff = src // 8 + dr, src % 8 + df
        while 0 <= rr < 8 and 0 <= ff < 8:
            sq = self.sq(ff, rr)
            if sq == dst:
                return True
            if sq in self.board:
                return False
            rr += dr
            ff += df
        return False

    def can_piece_reach(self, piece, src, dst, capture):
        sr, sf = src // 8, src % 8
        dr, df = dst // 8, dst % 8
        d_rank, d_file = dr - sr, df - sf
        target = self.board.get(dst)

        if piece in ("P", "p"):
            direction = 1 if piece == "P" else -1
            start = 1 if piece == "P" else 6
            if capture:
                if d_rank == direction and abs(d_file) == 1:
                    if target is not None and target.isupper() != piece.isupper():
                        return True
                    return self.ep_square == dst and target is None
                return False
            if d_file != 0:
                return False
            if d_rank == direction and target is None:
                return True
            return sr == start and d_rank == 2 * direction and target is None and self.board.get(src + direction * 8) is None

        if piece in ("N", "n"):
            return (abs(d_rank), abs(d_file)) in {(1, 2), (2, 1)} and (target is None or target.isupper() != piece.isupper())

        if piece in ("B", "b"):
            if abs(d_rank) != abs(d_file):
                return False
            return self._path_clear(src, dst, 1 if d_rank > 0 else -1, 1 if d_file > 0 else -1) and (target is None or target.isupper() != piece.isupper())

        if piece in ("R", "r"):
            if d_rank != 0 and d_file != 0:
                return False
            return self._path_clear(src, dst, 0 if d_rank == 0 else (1 if d_rank > 0 else -1), 0 if d_file == 0 else (1 if d_file > 0 else -1)) and (target is None or target.isupper() != piece.isupper())

        if piece in ("Q", "q"):
            if d_rank == 0 or d_file == 0:
                srn, sfn = (0 if d_rank == 0 else (1 if d_rank > 0 else -1)), (0 if d_file == 0 else (1 if d_file > 0 else -1))
            elif abs(d_rank) == abs(d_file):
                srn, sfn = (1 if d_rank > 0 else -1), (1 if d_file > 0 else -1)
            else:
                return False
            return self._path_clear(src, dst, srn, sfn) and (target is None or target.isupper() != piece.isupper())

        if piece in ("K", "k"):
            return max(abs(d_rank), abs(d_file)) == 1 and (target is None or target.isupper() != piece.isupper())

        return False


def tokenize_pgn_moves(pgn):
    text = re.sub(r"\{[^}]*\}", " ", pgn)
    text = re.sub(r"\([^)]*\)", " ", text)
    out = []
    for tok in text.split():
        if tok in {"1-0", "0-1", "1/2-1/2", "*"}:
            continue
        if re.match(r"^\d+\.{1,3}$", tok):
            continue
        if re.match(r"^\d+\.", tok):
            tok = tok.split(".", 1)[1]
        if tok:
            out.append(tok)
    return out


def san_to_uci(board, san):
    san = san.strip().replace("+", "").replace("#", "").replace("!", "").replace("?", "")
    if san in {"O-O", "0-0"}:
        return "e1g1" if board.turn == "w" else "e8g8"
    if san in {"O-O-O", "0-0-0"}:
        return "e1c1" if board.turn == "w" else "e8c8"

    m = re.match(r"^([KQRBN])?([a-h1-8]{0,2})(x?)([a-h][1-8])(=([QRBN]))?$", san)
    if not m:
        raise ValueError(f"unsupported SAN token: {san}")
    piece_letter, disamb, capture_flag, dst_name, _, promo = m.groups()
    dst = Board.parse_sq(dst_name)
    capture = capture_flag == "x"
    piece = (piece_letter or "P") if board.turn == "w" else (piece_letter or "P").lower()

    from_file = from_rank = None
    for c in disamb:
        if c in "abcdefgh":
            from_file = "abcdefgh".index(c)
        else:
            from_rank = int(c) - 1

    candidates = []
    for src, p in board.board.items():
        if p != piece:
            continue
        if from_file is not None and src % 8 != from_file:
            continue
        if from_rank is not None and src // 8 != from_rank:
            continue
        if not board.can_piece_reach(piece, src, dst, capture):
            continue
        uci = Board.sq_name(src) + Board.sq_name(dst) + (promo.lower() if promo else "")
        test = board.clone()
        test.apply_uci(uci)
        if not test.in_check("b" if test.turn == "w" else "w"):
            candidates.append(uci)

    if len(candidates) != 1:
        raise ValueError(f"ambiguous or illegal SAN token {san}; candidates={candidates}")
    return candidates[0]


def split_name_nodes(name):
    if ":" in name:
        head, rest = name.split(":", 1)
        parts = [head.strip()] + [p.strip() for p in rest.split(",") if p.strip()]
    else:
        parts = [name.strip()]
    out = []
    for i in range(1, len(parts) + 1):
        out.append(parts[0] if i == 1 else f"{parts[0]}: {', '.join(parts[1:i])}")
    return out


def read_rows(source_root: Path):
    rows = []
    source_files = []
    for fn in FILES:
        path = source_root / fn
        if not path.exists():
            raise ValueError(f"required source file missing: {path}")
        data = path.read_bytes()
        sha = hashlib.sha256(data).hexdigest()
        row_count = 0
        with path.open(newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f, delimiter="\t")
            if reader.fieldnames != ["eco", "name", "pgn"]:
                raise ValueError(f"{fn} header mismatch: {reader.fieldnames}")
            for i, rec in enumerate(reader, start=2):
                row_count += 1
                eco, name, pgn = (rec.get("eco") or "").strip(), (rec.get("name") or "").strip(), (rec.get("pgn") or "").strip()
                if not eco or not name or not pgn:
                    raise ValueError(f"malformed row {fn}:{i}")
                rows.append(SourceRow(fn, i, eco, name, pgn))
        source_files.append({"filename": fn, "sha256": sha, "row_count": row_count})
    rows.sort(key=lambda r: (FILES_ORDER[r.source_file], r.source_row_number, r.eco, r.name, r.pgn))
    return rows, source_files


def parse_line(row):
    b = Board()
    position_keys = [b.position_key()]
    epds = [b.epd()]
    uci_moves = []
    for tok in tokenize_pgn_moves(row.pgn):
        mv = san_to_uci(b, tok)
        b.apply_uci(mv)
        uci_moves.append(mv)
        position_keys.append(b.position_key())
        epds.append(b.epd())
    return ParsedLine(row, uci_moves, position_keys, epds)


def git_provenance(source_root: Path):
    def run(args):
        try:
            return subprocess.run(args, cwd=source_root, capture_output=True, text=True, check=True).stdout.strip() or None
        except Exception:
            return None

    if not (source_root / ".git").exists():
        return {"source_repo_url": None, "source_branch": None, "source_commit": None}
    return {
        "source_repo_url": run(["git", "config", "--get", "remote.origin.url"]),
        "source_branch": run(["git", "branch", "--show-current"]),
        "source_commit": run(["git", "rev-parse", "HEAD"]),
    }


def build_artifact(source_root: Path, output_root: Path, bundle_name: str):
    rows, source_files = read_rows(source_root)
    parsed = sorted([parse_line(r) for r in rows], key=lambda p: (p.row.name, len(p.uci_moves), " ".join(p.uci_moves), p.row.source_file, p.row.source_row_number))
    exact_names = sorted({p.row.name for p in parsed})

    node_names = set(exact_names)
    for n in exact_names:
        node_names.update(split_name_nodes(n))
    node_names = sorted(node_names)
    node_id = {n: i + 1 for i, n in enumerate(node_names)}
    kind = {n: ("exact_opening" if n in exact_names else "synthetic_family") for n in node_names}

    descendants = {n: [] for n in node_names}
    for p in parsed:
        for n in split_name_nodes(p.row.name):
            descendants[n].append(p)

    best = {n: min(descendants[n], key=lambda p: (len(p.uci_moves), p.row.name, " ".join(p.uci_moves))) for n in node_names}
    shortest_total = {n: len(best[n].uci_moves) for n in node_names}
    canonical_exact = {n: best[n].row.name for n in node_names}
    depth = {n: len(split_name_nodes(n)) for n in node_names}

    position_keys = sorted({k for p in parsed for k in p.position_keys})
    position_id = {k: i + 1 for i, k in enumerate(position_keys)}

    bundle = output_root / bundle_name
    bundle.mkdir(parents=True, exist_ok=True)
    db_path = bundle / "opening_locked_openings.sqlite"
    if db_path.exists():
        db_path.unlink()
    con = sqlite3.connect(db_path)
    con.executescript("""
create table meta(key text primary key, value text not null);
create table source_files(source_file_id integer primary key, filename text not null unique, sha256 text not null, row_count integer not null);
create table opening_nodes(node_id integer primary key, node_name text not null unique, node_kind text not null, canonical_exact_name text not null, family_depth integer not null, shortest_total_ply integer not null);
create table node_closure(ancestor_node_id integer not null, descendant_node_id integer not null, depth integer not null, primary key (ancestor_node_id, descendant_node_id));
create table positions(position_id integer primary key, position_key text not null unique, epd text not null, ply_from_root integer not null, side_to_move text not null);
create table exact_lines(line_id integer primary key, exact_node_id integer not null, eco text not null, source_file_id integer not null, source_row_number integer not null, opening_name text not null, pgn text not null, uci_line text not null, total_ply integer not null, terminal_position_id integer not null, is_shortest_for_name integer not null);
create table path_memberships(position_id integer not null, node_id integer not null, min_remaining_plies integer not null, is_terminal_for_exact integer not null, primary key (position_id, node_id));
create table node_moves(node_id integer not null, from_position_id integer not null, move_uci text not null, to_position_id integer not null, support_count integer not null, is_canonical integer not null, primary key (node_id, from_position_id, move_uci, to_position_id));
""")

    source_file_id = {sf["filename"]: i + 1 for i, sf in enumerate(source_files)}
    con.executemany("insert into source_files(source_file_id,filename,sha256,row_count) values(?,?,?,?)", [(source_file_id[sf["filename"]], sf["filename"], sf["sha256"], sf["row_count"]) for sf in source_files])
    con.executemany("insert into opening_nodes(node_id,node_name,node_kind,canonical_exact_name,family_depth,shortest_total_ply) values(?,?,?,?,?,?)", [(node_id[n], n, kind[n], canonical_exact[n], depth[n], shortest_total[n]) for n in node_names])

    closure = set()
    for n in node_names:
        path = split_name_nodes(n)
        for i, anc in enumerate(path):
            closure.add((node_id[anc], node_id[n], len(path) - i - 1))
    con.executemany("insert into node_closure(ancestor_node_id,descendant_node_id,depth) values(?,?,?)", sorted(closure))

    pos_info = {}
    for p in parsed:
        for i, k in enumerate(p.position_keys):
            if k not in pos_info or i < pos_info[k][1]:
                pos_info[k] = (p.epds[i], i, k.split()[1])
    con.executemany("insert into positions(position_id,position_key,epd,ply_from_root,side_to_move) values(?,?,?,?,?)", [(position_id[k], k, pos_info[k][0], pos_info[k][1], pos_info[k][2]) for k in position_keys])

    by_name = {}
    for p in parsed:
        by_name.setdefault(p.row.name, []).append(p)
    shortest_unique = {}
    for n, arr in by_name.items():
        m = min(len(x.uci_moves) for x in arr)
        c = [x for x in arr if len(x.uci_moves) == m]
        shortest_unique[n] = c[0] if len(c) == 1 else None

    exact_rows = []
    for i, p in enumerate(parsed, start=1):
        exact_rows.append((i, node_id[p.row.name], p.row.eco, source_file_id[p.row.source_file], p.row.source_row_number, p.row.name, p.row.pgn, " ".join(p.uci_moves), len(p.uci_moves), position_id[p.position_keys[-1]], 1 if shortest_unique[p.row.name] is p else 0))
    con.executemany("insert into exact_lines(line_id,exact_node_id,eco,source_file_id,source_row_number,opening_name,pgn,uci_line,total_ply,terminal_position_id,is_shortest_for_name) values(?,?,?,?,?,?,?,?,?,?,?)", exact_rows)

    memberships = {}
    move_counts = {}
    occurrences = {}
    for p in parsed:
        for n in split_name_nodes(p.row.name):
            nid = node_id[n]
            for i, pk in enumerate(p.position_keys):
                key = (position_id[pk], nid)
                rem = len(p.uci_moves) - i
                term = 1 if (i == len(p.uci_moves) and kind[n] == "exact_opening" and n == p.row.name) else 0
                if key not in memberships:
                    memberships[key] = (rem, term)
                else:
                    memberships[key] = (min(memberships[key][0], rem), max(memberships[key][1], term))
            for i, mv in enumerate(p.uci_moves):
                k = (nid, position_id[p.position_keys[i]], mv, position_id[p.position_keys[i + 1]])
                move_counts[k] = move_counts.get(k, 0) + 1
                occurrences.setdefault(k, []).append((len(p.uci_moves) - (i + 1), p.row.name, " ".join(p.uci_moves)))

    con.executemany("insert into path_memberships(position_id,node_id,min_remaining_plies,is_terminal_for_exact) values(?,?,?,?)", [(pid, nid, rem, term) for (pid, nid), (rem, term) in sorted(memberships.items())])

    grouped = {}
    for k in move_counts:
        grouped.setdefault((k[0], k[1]), []).append(k)
    canonical_keys = {min(v, key=lambda x: min(occurrences[x])) for v in grouped.values()}
    node_move_rows = [(k[0], k[1], k[2], k[3], move_counts[k], 1 if k in canonical_keys else 0) for k in sorted(move_counts)]
    con.executemany("insert into node_moves(node_id,from_position_id,move_uci,to_position_id,support_count,is_canonical) values(?,?,?,?,?,?)", node_move_rows)

    con.executemany("insert into meta(key,value) values(?,?)", sorted([("artifact_schema_version", "1"), ("artifact_kind", "opening_locked_openings"), ("position_key_format", "fen_pieces_side_castling_legal_ep")]))
    con.commit()
    con.close()

    gp = git_provenance(source_root)
    manifest = {
        "artifact_schema_version": "1",
        "artifact_id": bundle_name,
        "artifact_kind": "opening_locked_openings",
        "source_kind": "lichess_chess_openings_git_repo",
        "source_repo_url": gp["source_repo_url"],
        "source_branch": gp["source_branch"],
        "source_commit": gp["source_commit"],
        "build_timestamp_utc": dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "source_file_list": [sf["filename"] for sf in source_files],
        "total_source_rows": len(rows),
        "total_exact_opening_names": len(exact_names),
        "total_opening_nodes": len(node_names),
        "total_positions": len(position_keys),
        "total_exact_lines": len(parsed),
        "total_node_moves": len(node_move_rows),
        "position_key_format": "fen_pieces side_to_move castling_rights legal_en_passant",
        "naming_hierarchy_rule": "Opening family: Variation, Subvariation cumulative nodes",
        "canonical_continuation_rule": "shortest remaining exact descendant; tiebreak exact name then uci line",
        "bundle_files": ["manifest.json", "opening_locked_openings.sqlite"],
    }
    (bundle / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    return bundle


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--source-root", required=True)
    ap.add_argument("--output-root", required=True)
    ap.add_argument("--bundle-name", default="opening_locked_lichess_openings_v1")
    args = ap.parse_args()
    build_artifact(Path(args.source_root), Path(args.output_root), args.bundle_name)


if __name__ == "__main__":
    main()
