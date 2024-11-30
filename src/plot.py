import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

df = pd.read_csv('../benchmark_results.csv')

csv_files = df['csv_file'].unique()
metrics = ['read_time', 'write_time', 'agg_time', 'filter_time', 'join_time']

fig, axes = plt.subplots(nrows=2, ncols=2, figsize=(15, 15))
axes = axes.flatten()

for i, csv_file in enumerate(csv_files):
    ax = axes[i]
    subset = df[df['csv_file'] == csv_file]
    x = np.arange(len(metrics))  # the label locations
    width = 0.2  # the width of the bars

    for j, library in enumerate(subset['library'].unique()):
        library_subset = subset[subset['library'] == library]
        times = [library_subset[metric].values[0] * 1000 for metric in metrics]  # Convert to milliseconds
        ax.bar(x + j * width, times, width, label=library)

    ax.set_xlabel('Operation')
    ax.set_ylabel('Time (ms)')
    ax.set_title(f'Benchmark for {csv_file}')
    ax.set_xticks(x + width / 2)
    ax.set_xticklabels([metric.replace('_', ' ').title() for metric in metrics], rotation=45)
    ax.legend()

plt.tight_layout()
plt.savefig('benchmark_combined.png')
plt.close()

print("Benchmark plots saved successfully.")