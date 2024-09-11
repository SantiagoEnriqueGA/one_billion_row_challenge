import pyarrow.parquet as pq
import multiprocessing as mp
import pyarrow as pa
import os
from gc import disable as gc_disable, enable as gc_enable

def get_parquet_chunks(file_name: str, max_cpu: int = 16):
    """Split the Parquet file into row group chunks for processing."""
    cpu_count = min(max_cpu, mp.cpu_count())  # Number of CPUs to use
    parquet_file = pq.ParquetFile(file_name)
    
    row_groups = parquet_file.num_row_groups  # Get the number of row groups in the file
    rows_per_group = parquet_file.metadata.row_group(0).num_rows  # Rows in each group
    chunks = [(file_name, rg, rows_per_group) for rg in range(row_groups)]
    
    return cpu_count, chunks

def _process_parquet_chunk(file_name: str, row_group: int, rows_per_group: int):
    """Process each Parquet file chunk (row group) in a different process."""
    parquet_file = pq.ParquetFile(file_name)
    result = dict()
    
    table = parquet_file.read_row_group(row_group)  # Read specific row group
    locations = table.column(0)
    measurements = table.column(1).to_pylist()

    for location, measurement in zip(locations, measurements):
        measurement = float(measurement)

        if location in result:
            _result = result[location]

            if measurement < _result[0]:  # Update min
                _result[0] = measurement
            if measurement > _result[1]:  # Update max
                _result[1] = measurement
            _result[2] += measurement  # Sum of measurements
            _result[3] += 1  # Count of measurements
        else:
            result[location] = [measurement, measurement, measurement, 1]
    
    return result

def process_parquet_file(cpu_count: int, chunks: list):
    """Process Parquet data file by distributing chunks across multiple CPUs."""
    with mp.Pool(cpu_count) as p:
        gc_disable()
        chunk_results = p.starmap(_process_parquet_chunk, chunks)  # Parallel processing
        gc_enable()
    
    result = dict()
    
    for chunk_result in chunk_results:
        for location, measurements in chunk_result.items():
            location = str(location)  # Convert pyarrow.lib.StringScalar to regular string
            _result = result.get(location)
            if _result:
                if measurements[0] < _result[0]:
                    _result[0] = measurements[0]
                if measurements[1] > _result[1]:
                    _result[1] = measurements[1]
                _result[2] += measurements[2]
                _result[3] += measurements[3]
            else:
                result[location] = measurements
    
    # Print final results
    # print("{", end="")
    # for location, measurements in sorted(result.items()):
    #     print(
    #         f"{location}={measurements[0]:.1f}/{(measurements[2] / measurements[3]) if measurements[3] != 0 else 0:.1f}/{measurements[1]:.1f}",
    #         end=", "
    #     )
    # print("\b\b}")


def main(timer=False):
    if timer:
        import time
        start_time = time.time()

    cpu_count, chunks = get_parquet_chunks("measurements.parquet")
    process_parquet_file(cpu_count, chunks)

    if timer:
        duration = time.time() - start_time
        print(f"Duration: {duration}")

if __name__ == "__main__":
    main(timer=True)
