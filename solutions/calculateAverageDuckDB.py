import duckdb

def main():
    with duckdb.connect() as conn:
        # Import CSV in memory using DuckDB
        data = conn.sql(
            """
            SELECT
                station_name,
                MIN(measurement) AS min_measurement,
                ROUND(AVG(measurement), 1) AS mean_measurement,
                MAX(measurement) AS max_measurement
            FROM READ_CSV(
                'measurements_X5.txt',
                header=false,
                columns={'station_name':'VARCHAR','measurement':'DECIMAL(8,1)'},
                delim=';',
                parallel=true
            )
            GROUP BY
                station_name
            """
        )

        # Print final results
        print("{", end="")
        for row in sorted(data.fetchall()):
            print(f"{row[0]}={row[1]}/{row[2]}/{row[3]}",end=", ",)
        print("\b\b} ")

if __name__ == "__main__":
    main()