from multiprocessing import Process, cpu_count,set_start_method
import random
import jsonlines
import time
import chess.engine
import chess
import pandas as pd
from huggingface_hub import HfApi
import glob
import asyncio
import os
from mpi4py import MPI

api = HfApi()

# Read the JSONL file and load into a new list
loaded_moves_list = []
with jsonlines.open(r"/vol0003/hp190122/data/Synthesis_for_gpt/chess_game_moves.jsonl") as reader:
    for game_moves in reader:
        loaded_moves_list.append(game_moves)
if len(loaded_moves_list)!=0:
    print(loaded_moves_list [:3])
    
# Function to generate a chess game
def generate_game(time_per_move):
    engine = None
    init_retries = 5
    while engine is None:
        try:
            engine = chess.engine.SimpleEngine.popen_uci(r"/vol0003/hp190122/data/Synthesis_for_gpt/Stockfish/src/stockfish",timeout=30)
        except Exception as e:
            init_retries -= 1
            if init_retries == 0:
                return []
            else:
                continue
            
    board = chess.Board()
    
    moves = []

    j = random.randint(0,len(loaded_moves_list))

    if len(loaded_moves_list)!=0:
        for move in loaded_moves_list[j]:
            board.push(chess.Move.from_uci(move))

    max_retries = 5 # Set the maximum number of retries for a move before skipping the game

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
        except Exception as e:
            # Handle other exceptions
            print(f"Finish the game as an error occurred: {str(e)}")
            # Additional error handling or recovery code can be added here
            break  # Terminate the loop or handle the error in an appropriate way

    engine.quit()
    # print(moves)
    return moves

# Function to generate multiple games and store the results in a DataFrame
def generate_games(num_games, time_per_move):
    df = pd.DataFrame(columns=["Game", "Moves"])
    allmoves=0
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    for i in range(num_games):
        print(f'This is {os.getpid()} from node {rank} on {i}th games now')
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
        chess_dir = r'/vol0003/hp190122/data/Synthesis_for_gpt/chess'
        par_path = os.path.join(chess_dir,str(n)+".parquet") 
        df.to_parquet(par_path, engine='pyarrow')
        print(f'generate 1000 game cost time is {time.time()-s}')

        while True:
            try:
                api.upload_file(
                path_or_fileobj=par_path,
                path_in_repo=str(n)+".parquet",
                repo_id="Selfplay/chess-selfplay-data",
                repo_type="dataset",
                token="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",
                create_pr=False,
                )
                # os.remove(str(n)+".parquet")
                break
            except:
                pass


    #print(df)

if __name__=='__main__':
    num_cores = cpu_count()
    print(f'cores numbers is {num_cores}')
    processes = []
    
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    print(f'hello world from {rank}')
    
    for _ in range(num_cores):
            p = Process(target=main_process)
            p.start()
            time.sleep(1)
            processes.append(p)

    for p in processes:
            p.join()
            
    # synchronize all MPI ranks
    comm.Barrier()

    # print a message from the root process
    if rank == 0:
        print("All processes have finished.")
