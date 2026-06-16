# for reading Lichess Eval DB from compressed file into individual lines
import json
import io
import zstandard as zstd

# for batch-writing to Parquet file
import pyarrow as pa
import pyarrow.parquet as pq

lichess_eval_db =  r"./lichess-eval-db/lichess_db_eval.jsonl.zst"
output_evals_pq = r"./lichess-eval-db/evals.parquet"

# write BATCH_SIZE positions into output_evals_pq at a time 
BATCH_SIZE = 1_000_000 # number of positions
pq_schema = pa.schema([
    ("fen", pa.string()),
    ("eval_type", pa.string()),
    ("eval_value", pa.int32()),
    ("best_move", pa.string()),
]) # schema for individual rows
batch = []
counter = 0

# open lichess eval DB
with open(lichess_eval_db, 'rb') as file:
    dcmp = zstd.ZstdDecompressor()

    # read lichess_eval_db line by line and write to output_eval_pq in batches
    with dcmp.stream_reader(file) as reader:
        with pq.ParquetWriter(output_evals_pq, pq_schema) as writer:
            
            # for each line, get the fen, eval_value, eval_type, best_move
            for line in io.TextIOWrapper(reader, encoding='utf-8'):

                # single position with fen and evals
                pos_evals = json.loads(line)

                # get position as a fen
                fen = pos_evals["fen"]

                # get eval with the highest knodes = most search effort
                max_effort_eval = max(pos_evals["evals"], key=lambda e: e["knodes"])

                # get the first (best) pv
                # pv = principal variation
                first_pv = max_effort_eval["pvs"][0]

                # check which eval type the pv gives
                if "mate" in first_pv:
                    eval_type = "mate"
                    eval_value = first_pv["mate"]
                else:
                    eval_type = "cp"
                    eval_value = first_pv["cp"]

                # get best move in the given position = first move in the top/first pv
                best_move = first_pv["line"].split()[0]

                # add current position to batch
                batch.append({"fen": fen, "eval_type": eval_type, "eval_value": eval_value, "best_move": best_move})
                
                # print progress every 1m positions
                counter += 1
                if counter % 1_000_000 == 0:
                    print(f"{counter:,} positions processed")

                # write current batch to output_eval_pq if it is larger than BATCH_SIZE
                if len(batch) >= BATCH_SIZE:
                    writer.write_table(pa.Table.from_pylist(batch, schema=pq_schema))
                    batch = []
            
            # write any remaining positions in batch to output_eval_pq
            if batch:
                writer.write_table(pa.Table.from_pylist(batch, schema=pq_schema))