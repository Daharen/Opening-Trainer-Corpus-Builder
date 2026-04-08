#!/usr/bin/env python3
import os
import sys

mode = os.environ.get("FAKE_ENGINE_MODE", "mode1")
current_move = None

print("id name fake-stockfish-" + mode)
print("id author test")
print("uciok")
sys.stdout.flush()

while True:
    line = sys.stdin.readline()
    if not line:
        break
    line = line.strip()
    if line == "isready":
        print("readyok")
        sys.stdout.flush()
    elif line.startswith("position fen "):
        current_move = None
        if " moves " in line:
            current_move = line.split(" moves ", 1)[1].split()[-1]
    elif line.startswith("go "):
        cp = 0
        if mode == "mode1":
            if current_move is None:
                cp = 50
            elif current_move == "e2e4":
                cp = -40
            elif current_move == "d2d4":
                cp = 0
            elif current_move == "g1f3":
                cp = -50
            else:
                cp = 0
        else:
            if current_move is None:
                cp = 60
            elif current_move == "e2e4":
                cp = -20
            elif current_move == "d2d4":
                cp = 15
            elif current_move == "g1f3":
                cp = -10
            else:
                cp = 10
        print(f"info depth 1 score cp {cp} nodes 1")
        print("bestmove 0000")
        sys.stdout.flush()
    elif line == "uci":
        print("id name fake-stockfish-" + mode)
        print("uciok")
        sys.stdout.flush()
    elif line == "quit":
        break
