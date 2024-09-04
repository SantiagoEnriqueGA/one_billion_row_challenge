import pandas as pd

def main():
    df = (
        pd.read_csv("measurements.txt", sep=";", header=None, names=["station_name", "measurement"])
            .groupby("station_name")
            .agg(["min", "mean", "max"])
    )
    df.columns = df.columns.get_level_values(level=1)
    df = df.reset_index()
    df.columns = ["station_name", "min_measurement", "mean_measurement", "max_measurement"]
    df = df.sort_values("station_name")

    print("{", end="")
    for row in df.itertuples(index=False):
        print(
            f"{row.station_name}={row.min_measurement:.1f}/{row.mean_measurement:.1f}/{row.max_measurement:.1f}",
            end=", "
        )
    print("\b\b} ")

if __name__ == "__main__":
    main()