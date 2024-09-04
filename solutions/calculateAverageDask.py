import dask.dataframe as dd

def process_measurements():
    # Load the data with Dask DataFrame
    df = dd.read_csv(
        "measurements.txt",
        sep=";",
        header=None,
        names=["station", "measure"],
        engine="pyarrow",
        dtype_backend="pyarrow"
    )

    # Aggregate the data: calculate min, max, and mean for each station
    aggregated_df = df.groupby("station").agg({"measure": ["min", "mean", "max"]})
    
    # Flatten the column names
    aggregated_df.columns = aggregated_df.columns.droplevel()

    # Sort the results alphabetically by station name
    result = aggregated_df.sort_values("station").compute()

    # Prepare the output format
    output = ", ".join(
        f"{station}={row['min']:.1f}/{row['mean']:.1f}/{row['max']:.1f}"
        for station, row in result.iterrows()
    )

    # Print the results in the desired format
    print(f"{{ {output} }}")

if __name__ == "__main__":
    process_measurements()
