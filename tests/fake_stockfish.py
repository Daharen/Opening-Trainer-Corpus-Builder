#!/usr/bin/env python3
import sys

SCORES = {
    "rnbqkbnr/pppppppp/8/8/4P3/8/PPPP1PPP/RNBQKBNR b KQkq -": 10,    # after 1.e4 (from white POV +10)
    "rnbqkbnr/pppppppp/8/8/3PP3/8/PPP2PPP/RNBQKBNR b KQkq -": 120,   # after 1.d4
    "rnbqkbnr/pppppppp/8/8/2PP4/8/PP2PPPP/RNBQKBNR b KQkq -": 160,   # after 1.c4
    "rnbqkbnr/pppppppp/8/8/8/5N2/PPPPPPPP/RNBQKB1R b KQkq -": 30,    # after 1.Nf3
}
ROOT = "rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq -"
ROOT_E4 = "rnbqkbnr/pppppppp/8/8/4P3/8/PPPP1PPP/RNBQKBNR b KQkq -"
MOVE_MAP = {
    (ROOT, "e2e4"): ROOT_E4,
    (ROOT, "d2d4"): "rnbqkbnr/pppppppp/8/8/3PP3/8/PPP2PPP/RNBQKBNR b KQkq -",
    (ROOT, "c2c4"): "rnbqkbnr/pppppppp/8/8/2PP4/8/PP2PPPP/RNBQKBNR b KQkq -",
    (ROOT, "g1f3"): "rnbqkbnr/pppppppp/8/8/8/5N2/PPPPPPPP/RNBQKB1R b KQkq -",
    (ROOT_E4, "c7c5"): "rnbqkbnr/pp1ppppp/8/2p5/4P3/8/PPPP1PPP/RNBQKBNR w KQkq -",
    (ROOT_E4, "e7e5"): "rnbqkbnr/pppp1ppp/8/4p3/4P3/8/PPPP1PPP/RNBQKBNR w KQkq -",
}


def score_for_fen(fen: str, moves: list[str]) -> int:
    core = " ".join(fen.split(" ")[:4])
    for mv in moves:
        core = MOVE_MAP.get((core, mv), core)
    if core == ROOT:
        return 0
    return SCORES.get(core, 0)

for raw in sys.stdin:
    line = raw.strip()
    if not line:
        continue
    if line == "uci":
        print("id name fake-stockfish")
        print("uciok")
        sys.stdout.flush()
    elif line == "isready":
        print("readyok")
        sys.stdout.flush()
    elif line.startswith("setoption"):
        pass
    elif line.startswith("position fen "):
        payload = line[len("position fen "):]
        if " moves " in payload:
            current_fen, moves_raw = payload.split(" moves ", 1)
            current_moves = [m for m in moves_raw.split(" ") if m]
        else:
            current_fen = payload
            current_moves = []
    elif line.startswith("go movetime "):
        cp = score_for_fen(current_fen, current_moves)
        print(f"info depth 1 score cp {cp} nodes 1 nps 1")
        print("bestmove 0000")
        sys.stdout.flush()
    elif line == "quit":
        break
