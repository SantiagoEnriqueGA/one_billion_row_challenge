import vaex

def main():
    # Read the CSV file into a Vaex DataFrame
    df = (
        vaex.from_csv("measurements.txt", sep=";", header=None, names=["station_name", "measurement"], convert=False)
        .groupby(by='station_name', agg={'min_measurement': vaex.agg.min('measurement'),
                                         'mean_measurement': vaex.agg.mean('measurement'),
                                         'max_measurement': vaex.agg.max('measurement')})
        .sort('station_name')
    )

    # Print results directly from Vaex DataFrame
    print("{", end="")
    for row in df.to_records():
        # Access the dictionary items directly
        print(
            f"{row['station_name']}={row['min_measurement']:.1f}/{row['mean_measurement']:.1f}/{row['max_measurement']:.1f}",
            end=", "
        )
    print("\b\b} ")

if __name__ == "__main__":
    main()
