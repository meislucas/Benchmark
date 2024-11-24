import time
import pandas as pd
import logging
import gc
import os

logging.basicConfig(level=logging.INFO, format='%(message)s')

def benchmark(func, *args, **kwargs):
    start_time = time.time()
    result = func(*args, **kwargs)
    end_time = time.time()
    return result, end_time - start_time

def pandas_benchmark(csv_files):
    logging.info("Starting pandas benchmark")

    times = []

    for csv_file in csv_files:
        logging.info(f"Benchmarking with file: {csv_file}")

        gc.collect()
        os.environ["OMP_NUM_THREADS"] = "1"

        # Reading
        df1, read_time = benchmark(pd.read_csv, csv_file)
        df2, _ = benchmark(pd.read_csv, csv_file)
        logging.info(f"Read time: {read_time} seconds")

        # Writing
        _, write_time = benchmark(df1.to_csv, 'output_pandas.csv', index=False)
        logging.info(f"Write time: {write_time} seconds")

        # Aggregation
        _, agg_time = benchmark(df1.groupby('event_type').agg, {'price': 'sum'})
        logging.info(f"Aggregation time: {agg_time} seconds")

        # Filtering
        _, filter_time = benchmark(df1[df1['price'] > 10].copy)
        logging.info(f"Filter time: {filter_time} seconds")

        # Joining
        logging.info("Starting join")
        df1['product_id'] = df1['product_id'].astype(str)
        df2['product_id'] = df2['product_id'].astype(str)
        _, join_time = benchmark(df1.merge, df2, on='product_id', how='left')
        logging.info(f"Join time: {join_time} seconds")

        times.append((csv_file, 'pandas', read_time, write_time, agg_time, filter_time, join_time))
    return times