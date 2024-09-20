import os
import pandas as pd
import matplotlib.pyplot as plt
import re
import seaborn as sns


def load_timing_data(folder_path):
    timing_data = []

    for filename in os.listdir(folder_path):
        if filename.endswith('.log'):
            run_name = filename.split('_')[0]  # Extract the run name (e.g., "duckdb" from "duckdb_timing.log")
            with open(os.path.join(folder_path, filename), 'r') as file:
                lines = file.readlines()
                for line in lines:
                    match = re.search(r'Run \d+: ([\d.]+) seconds', line)
                    if match:
                        timing_data.append({'Run': run_name, 'Duration': float(match.group(1))})

    return timing_data


# Create a DataFrame for easier handling
folder_path = 'measurements-txt/logs/'
timing_data = load_timing_data(folder_path)

# Create a DataFrame for easier handling
df = pd.DataFrame(timing_data)
df.groupby("Run").agg('mean').sort_values(by='Duration', ascending=False)

# Visualization
plt.figure(figsize=(12, 6))
ax = sns.barplot(data=df.groupby("Run").agg('mean').sort_values(by='Duration', ascending=False), 
            y='Run', x='Duration', palette='Set2')

for container in ax.containers:   
    ax.bar_label(container, fmt='%i') 

plt.title('Timing for Different Implementations')
plt.xlabel('Duration (seconds)')
plt.ylabel(None)
plt.tight_layout()
plt.show()