import dask.dataframe as dd

def main():
    # Read the parquet file with Dask
    df = dd.read_parquet("measurements.parquet")

    # Group by station_name and calculate min, mean, and max
    grouped = df.groupby("station_name").agg({
        'measurement': ['min', 'mean', 'max']
    })

    # Flatten the column names
    grouped.columns = grouped.columns.get_level_values(1)
    grouped = grouped.reset_index()
    grouped.columns = ["station_name", "min_measurement", "mean_measurement", "max_measurement"]

    # Sort by station_name
    grouped = grouped.sort_values("station_name").compute()  # Trigger computation with .compute()

    # Print the results
    print("{", end="")
    for row in grouped.itertuples(index=False):
        print(
            f"{row.station_name}={row.min_measurement:.1f}/{row.mean_measurement:.1f}/{row.max_measurement:.1f}",
            end=", "
        )
    print("\b\b} ")

if __name__ == "__main__":
    main()
