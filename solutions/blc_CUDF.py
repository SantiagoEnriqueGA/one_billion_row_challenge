import cudf

def main():
    # Read the file into a cuDF DataFrame
    df = (
        cudf.read_csv("measurements.txt", sep=";", header=None, names=["station_name", "measurement"])
            .groupby("station_name")
            .agg({"measurement": ["min", "mean", "max"]})
    )
    
    # Flatten the multi-level column index
    df.columns = df.columns.get_level_values(1)
    
    # Reset index and rename columns
    df = df.reset_index()
    df.columns = ["station_name", "min_measurement", "mean_measurement", "max_measurement"]
    
    # Sort the DataFrame by station_name
    df = df.sort_values("station_name")
    
    # Print the results
    print("{", end="")
    for row in df.itertuples(index=False):
        print(
            f"{row.station_name}={row.min_measurement:.1f}/{row.mean_measurement:.1f}/{row.max_measurement:.1f}",
            end=", "
        )
    print("\b\b} ")

if __name__ == "__main__":
    main()
