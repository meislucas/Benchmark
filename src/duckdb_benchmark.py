import time
import duckdb
import logging
import gc
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

def benchmark(func, *args, **kwargs):
    repeat = 10  # Number of times to repeat the operation
    total_time = 0
    for _ in range(repeat):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        total_time += end_time - start_time
    average_time = total_time / repeat
    return result, average_time

def duckdb_benchmark(csv_files):
    times = []
    output_dir = '../benchmark_outputs'
    os.makedirs(output_dir, exist_ok=True)

    logging.info("Starting duckdb benchmark")

    for csv_file in csv_files:
        logging.info(f"Benchmarking with file: {csv_file}")

        # Clear memory
        gc.collect()

        # Reading
        con = duckdb.connect()
        df1, read_time = benchmark(con.execute, f"SELECT * FROM read_csv_auto('{csv_file}')")
        df2, _ = benchmark(con.execute, f"SELECT * FROM read_csv_auto('{csv_file}')")
        logging.info(f"Read time: {read_time}")

        # Create temporary tables for df1 and df2
        con.execute(f"CREATE TEMPORARY TABLE temp_df1 AS SELECT * FROM read_csv_auto('{csv_file}')")
        con.execute(f"CREATE TEMPORARY TABLE temp_df2 AS SELECT * FROM read_csv_auto('{csv_file}')")

        # Writing
        output_file = os.path.join(output_dir, 'output_duckdb.csv')
        _, write_time = benchmark(con.execute, f"COPY (SELECT * FROM temp_df1) TO '{output_file}' (FORMAT CSV, HEADER)")
        logging.info(f"Write time: {write_time}")

        # Aggregation
        _, agg_time = benchmark(lambda: con.execute("SELECT event_type, SUM(price) FROM temp_df1 GROUP BY event_type").fetchall())
        logging.info(f"Aggregation time: {agg_time}")

        # Filtering
        _, filter_time = benchmark(lambda: con.execute("SELECT * FROM temp_df1 WHERE price > 300").fetchall())
        logging.info(f"Filter time: {filter_time}")

        # Joining
        _, join_time = benchmark(lambda: con.execute("SELECT * FROM temp_df1 JOIN temp_df2 USING (product_id)").fetchall())
        logging.info(f"Join time: {join_time}")

        # Drop temporary tables
        con.execute("DROP TABLE temp_df1")
        con.execute("DROP TABLE temp_df2")

        times.append((csv_file, 'duckdb', read_time, write_time, agg_time, filter_time, join_time))
    return times