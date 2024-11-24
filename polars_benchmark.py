import time
import polars as pl
import logging
import gc
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

def benchmark(func, *args, **kwargs):
    start_time = time.time()
    result = func(*args, **kwargs)
    end_time = time.time()
    return result, end_time - start_time

def polars_benchmark(csv_files):
    times = []

    logging.info("Starting polars benchmark")

    for csv_file in csv_files:
        logging.info(f"Benchmarking with file: {csv_file}")

        # Clear memory and set threads
        gc.collect()
        os.environ["OMP_NUM_THREADS"] = "1"

        # Reading
        df1, read_time = benchmark(pl.read_csv, csv_file)
        df2, _ = benchmark(pl.read_csv, csv_file)
        logging.info(f"Read time: {read_time}")

        # Writing
        _, write_time = benchmark(df1.write_csv, 'output_polars.csv')
        logging.info(f"Write time: {write_time}")

        # Aggregation
        _, agg_time = benchmark(df1.groupby('event_type').agg(pl.sum('price')))
        logging.info(f"Aggregation time: {agg_time}")

        # Filtering
        _, filter_time = benchmark(df1.filter(pl.col('price') > 10))
        logging.info(f"Filter time: {filter_time}")

        # Joining
        logging.info("Starting join")
        _, join_time = benchmark(df1.join, df2, on='product_id')
        logging.info(f"Join time: {join_time}")

        times.append((csv_file, 'polars', read_time, write_time, agg_time, filter_time, join_time))
    return times