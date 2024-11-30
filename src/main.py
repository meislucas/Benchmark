import csv
from pandas_benchmark import pandas_benchmark
from polars_benchmark import polars_benchmark
from duckdb_benchmark import duckdb_benchmark

# Define the CSV file paths
csv_files = ['data/data_100.csv', 'data/data_10000.csv', 'data/data_1000000.csv', 'data/data_2000000.csv']

# Run benchmarks
duckdb_times = duckdb_benchmark(csv_files)
pandas_times = pandas_benchmark(csv_files)
polars_times = polars_benchmark(csv_files)


# Combine all benchmark results
all_times = pandas_times + polars_times + duckdb_times

# Write results to CSV
with open('../benchmark_results.csv', 'w', newline='') as csvfile:
    fieldnames = ['csv_file', 'library', 'read_time', 'write_time', 'agg_time', 'filter_time', 'join_time']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    for row in all_times:
        writer.writerow({
            'csv_file': row[0],
            'library': row[1],
            'read_time': row[2],
            'write_time': row[3],
            'agg_time': row[4],
            'filter_time': row[5],
            'join_time': row[6]
        })

# Print results
print("Benchmark results written to benchmark_results.csv")