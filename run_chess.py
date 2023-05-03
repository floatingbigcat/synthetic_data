from multiprocessing import Process, cpu_count
import random
import jsonlines
import time
import chess.engine
import chess
import pandas as pd
from huggingface_hub import HfApi
import glob
import os
api = HfApi()


# Read the JSONL file and load into a new list
loaded_moves_list = []
with jsonlines.open(r"/vol0003/hp190122/data/Synthesis_for_gpt/game_moves.jsonl") as reader:
    for game_moves in reader:
        loaded_moves_list.append(game_moves)

if len(loaded_moves_list)!=0:
    print(loaded_moves_list [:3])


# Function to generate a chess game
def generate_game(time_per_move):
    engine = chess.engine.SimpleEngine.popen_uci(r"/vol0003/mdt0/home/u11570/scratch/Stockfish/src/stockfish")
    board = chess.Board()
    
    moves = []

    j = random.randint(0,len(loaded_moves_list))

    if len(loaded_moves_list)!=0:
        for move in loaded_moves_list[j]:
            board.push(chess.Move.from_uci(move))

    max_retries = 10  # Set the maximum number of retries for a move before skipping the game

    while not board.is_game_over():
        try:
            result = engine.play(board, chess.engine.Limit(time=time_per_move))
            move = result.move
            uci_move = move.uci()  # Get the move in UCI format
            moves.append(uci_move)
            board.push(move)
        except asyncio.TimeoutError:
            max_retries -= 1
            time.sleep(1)
            if max_retries == 0:
                break  # Skip the current game if the maximum number of retries is reached
            continue  # Retry the move

    engine.quit()
    # print(moves)
    return moves

# Function to generate multiple games and store the results in a DataFrame
def generate_games(num_games, time_per_move):
    df = pd.DataFrame(columns=["Game", "Moves"])
    allmoves=0
    for i in range(num_games):
        moves = generate_game(time_per_move)
        allmoves += len(moves)
        df = pd.concat([df, pd.DataFrame({"Game": [i], "Moves": [moves]})], ignore_index=True)
    
    print(allmoves)
    return df
def main_process():
 # Example usage: generate 1 games with 0.07 second per move
    while True:
    s = time.time()
    df = generate_games(1000, 0.07)
    n = random.randint(0,1000000000000)
    #df.to_csv("chess_games.csv", index=False)
    df.to_parquet(str(n)+".parquet", engine='pyarrow')

    while True:
        try:
        api.upload_file(
        path_or_fileobj=str(n)+".parquet",
        path_in_repo=str(n)+".parquet",
        repo_id="Selfplay/chess-selfplay-data",
        repo_type="dataset",
        token="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",
        create_pr=False,
        )
        os.remove(str(n)+".parquet")
        break
        except:
        pass


    #print(df)
    print(time.time()-s)

num_cores = cpu_count()
print(f'cores numbers is {num_cores}')
processes = []

for _ in range(num_cores):
        p = Process(target=main_process)
        p.start()
        time.sleep(3)
        processes.append(p)

for p in processes:
        p.join()
