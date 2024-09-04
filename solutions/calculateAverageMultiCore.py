import os
from gc import disable as gc_disable, enable as gc_enable
import multiprocessing as mp

def get_file_chunks(file_name: str, max_cpu: int = 16):
    """Split file into chunks for processing by multiple CPUs."""
    cpu_count = min(max_cpu, mp.cpu_count())    # Determine the number of CPU cores to use

    file_size = os.path.getsize(file_name)      # Get the total size of the file
    chunk_size = file_size // cpu_count         # Calculate the size of each chunk based on the number of CPU cores

    start_end = list()                          # List to store the start and end positions of each chunk
    
    with open(file_name, mode="r+b") as f:
        # Function to check if the position is at the start of a new line   
        def is_new_line(position):
            if position == 0:
                return True
            else:
                f.seek(position - 1)        # Move the file pointer to one byte before the current position
                return f.read(1) == b"\n"   # Read the byte to check if it is a newline character

        # Function to find the start of the next line from the given position
        def next_line(position):
            f.seek(position)    # Move the file pointer to the given position
            f.readline()        # Read the line to move to the end of it
            return f.tell()     # Return the current position, which is now the start of the next line

        chunk_start = 0
        while chunk_start < file_size:
            chunk_end = min(file_size, chunk_start + chunk_size)    # End of the current chunk

            while not is_new_line(chunk_end):                       # Adjust chunk_end to the start of the next line if necessary
                chunk_end -= 1

            if chunk_start == chunk_end:                            # If the chunk size is very small, ensure it is moved to the start of the next line
                chunk_end = next_line(chunk_end)

            start_end.append((file_name, chunk_start, chunk_end))   # Append the current chunk information (file name, start, end) to the list
            chunk_start = chunk_end                                 # Move to the next chunk

    return (cpu_count, start_end)


def _process_file_chunk(file_name: str, chunk_start: int, chunk_end: int):
    """Process each file chunk in a different process."""
    result = dict()                         # Dictionary to store the results for each location

    with open(file_name, mode="rb") as f:
        f.seek(chunk_start)                 # Move the file pointer to the start of the chunk

        for line in f:
            chunk_start += len(line)        # Update chunk_start to the end of the current line
            if chunk_start > chunk_end:     # Stop processing if the end of the chunk is reached
                break

            location, _, measurement = line.partition(b";")     # Split the line into location and measurement parts
            measurement = float(measurement)                    # Convert the measurement to a float
            
            
            if location in result:              # If the location is already in the results dictionary
                _result = result[location]
                
                if measurement < _result[0]:    # Update the minimum measurement if the current measurement is smaller
                    _result[0] = measurement
                
                if measurement > _result[1]:    # Update the maximum measurement if the current measurement is larger
                    _result[1] = measurement
                
                _result[2] += measurement       # Accumulate the sum of measurements
                _result[3] += 1                 # Increment the count of measurements
            else:                               # Initialize the results for a new location
                result[location] = [measurement, measurement, measurement, 1]
    
    # Return the dictionary containing the results for the chunk
    return result  

def process_file(cpu_count: int, start_end: list):
    """Process data file by distributing chunks across multiple CPUs."""
    with mp.Pool(cpu_count) as p:
        chunk_results = p.starmap(_process_file_chunk, start_end)   # Run chunks in parallel using multiprocessing pool

    result = dict()                                                 # Dictionary to store combined results

    # Combine all results from all chunks
    for chunk_result in chunk_results:
        for location, measurements in chunk_result.items():
            _result = result.get(location)
            if _result:
                if measurements[0] < _result[0]:    # Update the minimum measurement if the new measurement is smaller
                    _result[0] = measurements[0]
                
                if measurements[1] > _result[1]:    # Update the maximum measurement if the new measurement is larger
                    _result[1] = measurements[1]
                
                _result[2] += measurements[2]       # Accumulate the sum of measurements
                _result[3] += measurements[3]       # Increment the count of measurements
            else:
                result[location] = measurements     # Add new location with its measurements to the result dictionary

    # Print final results in required format
    # Start printing the result dictionary
    print("{", end="") 
    for location, measurements in sorted(result.items()):
        # Print each location with its min, average, and max measurements
        print(
            f"{location.decode('utf8')}={measurements[0]:.1f}/{(measurements[2] / measurements[3]) if measurements[3] != 0 else 0:.1f}/{measurements[1]:.1f}",
            end=", "
        )
    # Remove trailing comma and space, then close the bracket
    print("\b\b} ")

def main(timer = False):
    if timer:
        import time
        start_time = time.time()

    cpu_count, start_end = get_file_chunks("measurements.txt")
    process_file(cpu_count, start_end)


    if timer:
        duration = time.time() - start_time
        print(f"Duration :{duration}")

if __name__ == "__main__":
    main(timer=True)
    
    
