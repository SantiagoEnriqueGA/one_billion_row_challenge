import subprocess
import time
import os

def run_script_with_timing(executable, script_name):
    """Runs a given Python script with a specific executable, times its execution, and returns the output and duration."""
    print(f"Running script: {script_name} \n\t Executable: {executable}.")
    start_time = time.time()
    
    result = subprocess.run([executable, script_name], capture_output=True, text=True)
    duration = time.time() - start_time
    return result.stdout, duration

def compare_outputs(output1, output2):
    """Compares two outputs using git diff's word-diff format."""
    result = subprocess.run(
        ["git", "diff", "--no-index", "--word-diff=porcelain"],
        input=f"{output1}\n{output2}",
        text=True,
        capture_output=True,
    )
    return result.stdout

def save_output_to_file(filename, content):
    """Saves the output content to a file."""
    with open(filename, "w") as f:
        f.write(content)

def main(compare=True):
    # Dictionary of executables and scripts
    pypy_exec = r"C:\Users\sega9\anaconda3\envs\my-pypy-env\python.exe"
    pyspark_exec = r"C:\Users\sega9\anaconda3\envs\pyspark_env\python.exe"
    scripts = {               
        "duckdb": ("python", "solutionsParquet/blc_DuckDB.py"),
        
        "polars-pypy": (pypy_exec, "solutionsParquet/blc_Polars.py"),
        "polars-cpy": ("python", "solutionsParquet/blc_Polars.py"),
        
        "pyspark": (pyspark_exec, "solutionsParquet/blc_PySpark.py"),
        
        "dask": ("python", "solutionsParquet/blc_Dask.py"),
    }

    num_runs = 3

    # Create directories for output and logs if they do not exist
    os.makedirs("measurements-parquet/output", exist_ok=True)
    os.makedirs("measurements-parquet/logs", exist_ok=True)

    for key, (executable, script) in scripts.items():
        total_durations = []
        for run in range(num_runs):
            output, duration = run_script_with_timing(executable, script)
            total_durations.append(duration)
            
            # Save the outputs to files
            save_output_to_file(f"measurements-parquet/output/{key}_run{run + 1}.txt", output)
        
        # Log the execution times
        avg_duration = sum(total_durations) / num_runs
        with open(f"measurements-parquet/logs/{key}_timing.log", "w") as log_file:
            for i, duration in enumerate(total_durations, start=1):
                log_file.write(f"Run {i}: {duration:.4f} seconds\n")
            log_file.write(f"Average Duration: {avg_duration:.4f} seconds\n")

    # Print the timing results
    for key in scripts:
        with open(f"measurements-parquet/logs/{key}_timing.log", "r") as log_file:
            print(f"\nTiming results for {key}:")
            print(log_file.read())

    if compare:
        # Compare the outputs
        comparisons = {
            "python_vs_pypy": compare_outputs(open("output/python_run1.txt").read(), open("output/pypy_run1.txt").read()),
            "python_vs_polars": compare_outputs(open("output/python_run1.txt").read(), open("output/polars_run1.txt").read()),
            "python_vs_duckdb": compare_outputs(open("output/python_run1.txt").read(), open("output/duckdb_run1.txt").read()),
            "python_vs_dask": compare_outputs(open("output/python_run1.txt").read(), open("output/dask_run1.txt").read()),
            "python_vs_pyspark": compare_outputs(open("output/python_run1.txt").read(), open("output/pyspark_run1.txt").read()),
            "python_vs_sqlite-cpython": compare_outputs(open("output/python_run1.txt").read(), open("output/sqlite_run1.txt").read()),
            "python_vs_sqlite-pypy": compare_outputs(open("output/python_run1.txt").read(), open("output/sqlite-pypy_run1.txt").read()),
        }

        # Print the differences, if any
        for comparison_name, diff in comparisons.items():
            if diff:
                print(f"\nDifference between {comparison_name.replace('_', ' and ')}: {diff}")
            else:
                print(f"\nNo difference between {comparison_name.replace('_', ' and ')}")

if __name__ == "__main__":
    main(compare=False)
