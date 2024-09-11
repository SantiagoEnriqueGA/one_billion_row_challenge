import sqlite3
import csv
import time

# Function to load CSV data into SQLite3 in-memory database
def load_csv_to_sqlite(db_conn, csv_file):
    start_time = time.time()  # Start timer
    
    # Optimize SQLite PRAGMAs
    db_conn.execute('PRAGMA journal_mode = OFF;')
    db_conn.execute('PRAGMA synchronous = 0;')
    db_conn.execute('PRAGMA cache_size = 1000000;')
    db_conn.execute('PRAGMA locking_mode = EXCLUSIVE;')
    db_conn.execute('PRAGMA temp_store = MEMORY;')

    db_conn.execute("CREATE TEMP TABLE measurements_temp (station_name TEXT, measurement REAL)")

    with open(csv_file, 'r') as f:
        reader = csv.reader(f, delimiter=';')
        db_conn.executemany("INSERT INTO measurements_temp (station_name, measurement) VALUES (?, ?)", reader)
    
    # Bulk insert from temp table to final table
    db_conn.execute("CREATE TEMP TABLE measurements AS SELECT * FROM measurements_temp")
    
    end_time = time.time()  # End timer
    print(f"load_csv_to_sqlite took {end_time - start_time:.4f} seconds")

# Function to calculate min, mean, max per station and print in the required format
def calculate_and_print_results(db_conn):
    start_time = time.time()  # Start timer
    
    cursor = db_conn.execute(
        """
        SELECT
            station_name,
            MIN(measurement) AS min_measurement,
            ROUND(AVG(measurement), 1) AS mean_measurement,
            MAX(measurement) AS max_measurement
        FROM measurements
        GROUP BY station_name
        """
    )

    # Printing the results directly
    results = cursor.fetchall()
    result_str = ", ".join([f"{row[0]}={row[1]}/{row[2]}/{row[3]}" for row in results])
    # print(f"{{{result_str}}}")

    end_time = time.time()  # End timer
    print(f"calculate_and_print_results took {end_time - start_time:.4f} seconds")

# Main execution
def main():
    start_time = time.time()  # Start timer for overall execution

    with sqlite3.connect(":memory:") as conn:
        load_csv_to_sqlite(conn, 'measurements.txt')
        calculate_and_print_results(conn)
    
    end_time = time.time()  # End timer for overall execution
    print(f"Overall execution took {end_time - start_time:.4f} seconds")

if __name__ == "__main__":
    main()
