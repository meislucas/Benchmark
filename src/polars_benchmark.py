import time
import polars as pl
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

def polars_benchmark(csv_files):
    times = []
    output_dir = '../benchmark_outputs'
    os.makedirs(output_dir, exist_ok=True)

    logging.info("Starting polars benchmark")

    for csv_file in csv_files:
        logging.info(f"Benchmarking with file: {csv_file}")

        # Clear memory and set threads
        gc.collect()
        os.environ["OMP_NUM_THREADS"] = "1"

        # Reading
        df1, read_time = benchmark(pl.scan_csv, csv_file)
        df2, _ = benchmark(pl.scan_csv, csv_file)
        logging.info(f"Read time: {read_time}")

        # Writing
        df1_eagerloaded = df1.collect()
        output_file = os.path.join(output_dir, 'output_polars.csv')
        _, write_time = benchmark(df1_eagerloaded.write_csv, output_file)
        logging.info(f"Write time: {write_time}")

        # Aggregation
        _, agg_time = benchmark(lambda: df1.group_by('event_type').agg(pl.sum('price')).collect())
        logging.info(f"Aggregation time: {agg_time}")

        # Filtering
        _, filter_time = benchmark(lambda: df1.filter(pl.col('price') > 300).collect())
        logging.info(f"Filter time: {filter_time}")

        # Joining
        _, join_time = benchmark(lambda: df1.join(df2, on='product_id').collect())
        logging.info(f"Join time: {join_time}")

        times.append((csv_file, 'polars', read_time, write_time, agg_time, filter_time, join_time))
    return times