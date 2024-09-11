import polars as pl

def main():
    df = pl.scan_parquet("measurements.parquet")

    # Group data
    grouped = (df.group_by("station_name")
        .agg(pl.min("measurement").alias("min_measurement"),
            pl.mean("measurement").alias("mean_measurement"),
            pl.max("measurement").alias("max_measurement"),
        )
        .sort("station_name")
        .collect(streaming=True)
    )

    # Print final results
    print("{", end="")
    for data in grouped.iter_rows():
        print(f"{data[0]}={data[1]:.1f}/{data[2]:.1f}/{data[3]:.1f}",end=", ",)
    print("\b\b} ")

if __name__ == "__main__":
    main()