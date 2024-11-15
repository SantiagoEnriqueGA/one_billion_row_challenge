import os
from gc import disable as gc_disable, enable as gc_enable
import multiprocessing as mp
import yappi

def get_file_chunks(file_name: str, max_cpu: int = 16):
    """Split file into chunks for processing by multiple CPUs."""
    cpu_count = min(max_cpu, mp.cpu_count())
    file_size = os.path.getsize(file_name)
    chunk_size = file_size // cpu_count

    start_end = list()
    with open(file_name, mode="r+b") as f:
        def is_new_line(position):
            if position == 0:
                return True
            else:
                f.seek(position - 1)
                return f.read(1) == b"\n"

        def next_line(position):
            f.seek(position)
            f.readline()
            return f.tell()

        chunk_start = 0
        while chunk_start < file_size:
            chunk_end = min(file_size, chunk_start + chunk_size)
            while not is_new_line(chunk_end):
                chunk_end -= 1
            if chunk_start == chunk_end:
                chunk_end = next_line(chunk_end)
            start_end.append((file_name, chunk_start, chunk_end))
            chunk_start = chunk_end

    return (cpu_count, start_end)

def _process_file_chunk(file_name: str, chunk_start: int, chunk_end: int):
    result = dict()
    with open(file_name, mode="rb") as f:
        f.seek(chunk_start)
        for line in f:
            chunk_start += len(line)
            if chunk_start > chunk_end:
                break
            location, _, measurement = line.partition(b";")
            measurement = float(measurement)
            if location in result:
                _result = result[location]
                if measurement < _result[0]:
                    _result[0] = measurement
                if measurement > _result[1]:
                    _result[1] = measurement
                _result[2] += measurement
                _result[3] += 1
            else:
                result[location] = [measurement, measurement, measurement, 1]
    return result  

def process_file(cpu_count: int, start_end: list):
    with mp.Pool(cpu_count) as p:
        chunk_results = p.starmap(_process_file_chunk, start_end)

    result = dict()
    for chunk_result in chunk_results:
        for location, measurements in chunk_result.items():
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

    print("{", end="") 
    for location, measurements in sorted(result.items()):
        print(
            f"{location.decode('utf8')}={measurements[0]:.1f}/{(measurements[2] / measurements[3]) if measurements[3] != 0 else 0:.1f}/{measurements[1]:.1f}",
            end=", "
        )
    print("\b\b} ")

def main(timer = False):
    if timer:
        import time
        start_time = time.time()

    # Start Yappi profiling
    yappi.start()

    cpu_count, start_end = get_file_chunks("measurements.txt")
    process_file(cpu_count, start_end)

    # Stop Yappi profiling
    yappi.stop()

    # Print Yappi stats
    yappi.get_func_stats().print_all()

    if timer:
        duration = time.time() - start_time
        print(f"Duration :{duration}")

    # Save Yappi stats to a file (optional)
    yappi.get_func_stats().save('yappi.prof', type='callgrind')

if __name__ == "__main__":
    main(timer=True)
