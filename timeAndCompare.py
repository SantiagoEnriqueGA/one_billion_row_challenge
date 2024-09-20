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
    py39_exec = r"C:\Users\sega9\anaconda3\envs\py39\python.exe"
    scripts = {
        "duckdb": ("python", "solutions/blc_DuckDB.py"),
        
        "polars-cpy": ("python", "solutions/blc_Polars.py"),
        "polars-pypy": (pypy_exec, "solutions/blc_Polars.py"),
        
        "python-singleCore": ("python", "solutions/blc_SingleCore.py"),
        "pypy-singleCore": (pypy_exec, "solutions/blc_SingleCore.py"),
        
        "python-multiCore-concurent": ("python", "solutions/blc_MultiCore_concurent.py"),
        "python-multiCore-mutiproccess": ("python", "solutions/blc_MultiCore_multiprocessing.py"),
        
        "pypy-multiCore-concurent": (pypy_exec, "solutions/blc_MultiCore_concurent.py"),
        "pypy-multiCore-mutiproccess": (pypy_exec, "solutions/blc_MultiCore_multiprocessing.py"),
        
        "pyspark": (pyspark_exec, "solutions/blc_PySpark.py"),
        
        "dask": ("python", "solutions/blc_Dask.py"),
        
        "pandas-cpy": ("python", "solutions/blc_Pandas.py"),
        "pandas-cpy-pyarrow": ("python", "solutions/blc_PandasPyarrow.py"),
        "pandas-pypy": (pypy_exec, "solutions/blc_Pandas.py"),
        
        "sqlite-cpy": ("python", "solutions/blc_SQLite.py"),
        "sqlite-pypy": (pypy_exec, "solutions/blc_SQLite.py"),
        
        # "vaex": (py39_exec, "solutions/blc_Vaex.py"),
        # "pandas-cpy-modin": ("python", "solutions/blc_PandasModin.py"),
    }

    num_runs = 3

    # Create directories for output and logs if they do not exist
    output_dir = "measurements-txt/output"
    log_dir = "measurements-txt/logs"
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(log_dir, exist_ok=True)

    for key, (executable, script) in scripts.items():
        total_durations = []
        for run in range(num_runs):
            if run>0: time.sleep(30) # Cool-down between runs
            
            output, duration = run_script_with_timing(executable, script)
            total_durations.append(duration)
            
            # Save the outputs to files
            save_output_to_file(f"{output_dir}/{key}_run{run + 1}.txt", output)
        
        # Log the execution times
        avg_duration = sum(total_durations) / num_runs
        with open(f"{log_dir}/{key}_timing.log", "w") as log_file:
            for i, duration in enumerate(total_durations, start=1):
                log_file.write(f"Run {i}: {duration:.4f} seconds\n")
            log_file.write(f"Average Duration: {avg_duration:.4f} seconds\n")

    # Print the timing results
    for key in scripts:
        with open(f"measurements-txt/logs/{key}_timing.log", "r") as log_file:
            print(f"\nTiming results for {key}:")
            print(log_file.read())

    if compare:
        # List of comparisons you want to perform
        comparisons = [
            ("python-singleCore", "pypy-singleCore"),
            ("python-singleCore", "polars-cpy"),
            ("python-singleCore", "duckdb"),
            ("python-singleCore", "dask"),
            ("python-singleCore", "pyspark"),
            ("python-singleCore", "sqlite-cpy"),
            ("python-singleCore", "sqlite-pypy"),
        ]

        # Compare the outputs
        for script1, script2 in comparisons:
            output1 = open(f"{output_dir}/{script1}_run1.txt").read()
            output2 = open(f"{output_dir}/{script2}_run1.txt").read()

            diff = compare_outputs(output1, output2)
            comparison_name = f"{script1}_vs_{script2}"
            if diff:
                print(f"\nDifference between {comparison_name.replace('_', ' and ')}:\n{diff}")
            else:
                print(f"\nNo difference between {comparison_name.replace('_', ' and ')}")

if __name__ == "__main__":
    main(compare=False)
