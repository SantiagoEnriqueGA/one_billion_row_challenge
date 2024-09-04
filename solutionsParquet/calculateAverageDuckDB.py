import duckdb

def main():
    with duckdb.connect() as conn:
        # Import CSV in memory using DuckDB
        data = conn.sql("""
            select
                station_name,
                min(measurement) as min_measurement,
                cast(avg(measurement) as decimal(8, 1)) as mean_measurement,
                max(measurement) as max_measurement
            from parquet_scan('measurements.parquet')
            group by station_name
            order by station_name
        """)

        # Print final results
        print("{", end="")
        for row in sorted(data.fetchall()):
            print(f"{row[0]}={row[1]}/{row[2]}/{row[3]}",end=", ",)
        print("\b\b} ")
    
if __name__ == "__main__":
    main()