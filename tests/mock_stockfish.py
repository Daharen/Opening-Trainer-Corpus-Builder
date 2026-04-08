#!/usr/bin/env python3
import os
import sys

mode = os.environ.get("MOCK_ENGINE_MODE", "default")
log_path = os.environ.get("MOCK_ENGINE_LOG")

def score_for(move):
    if mode == "no_baseline":
        if move == "":
            return 30
        if move == "e2e4":
            return 120
        if move == "d2d4":
            return 60
        return 40
    # default
    if move == "":
        return 30
    if move == "e2e4":
        return -10
    if move == "d2d4":
        return 20
    return 0

while True:
    line = sys.stdin.readline()
    if not line:
        break
    line = line.strip()
    if line == "uci":
        print("id name mock-stockfish")
        print("uciok")
        sys.stdout.flush()
    elif line.startswith("setoption"):
        pass
    elif line == "isready":
        print("readyok")
        sys.stdout.flush()
    elif line.startswith("position fen "):
        parts = line.split(" moves ")
        move = ""
        if len(parts) > 1:
            move = parts[1].strip().split()[0]
        os.environ["_LAST_MOVE"] = move
    elif line.startswith("go movetime"):
        move = os.environ.get("_LAST_MOVE", "")
        cp = score_for(move)
        if log_path:
            with open(log_path, "a", encoding="utf-8") as f:
                f.write(f"go move={move} cp={cp}\n")
        print(f"info depth 1 score cp {cp} nodes 1 nps 1")
        print("bestmove e2e4")
        sys.stdout.flush()
    elif line == "quit":
        break
