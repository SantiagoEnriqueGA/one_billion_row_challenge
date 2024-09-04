import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import min, max, avg, format_number
from pyspark.sql.types import StructType, StructField, FloatType, StringType

def create_spark_session():
    config = {'adaptive': True, 'adaptive_coalesce': True, 'shuffle_partitions': 200, 'parallelism': 100, 'off_heap': True, 'off_heap_size': '10g', 'repartition': False, 'num_partitions': 400, 'cache': False}
    return SparkSession.builder \
        .appName("Temperature Analysis") \
        .master("local[*]") \
        .config("spark.sql.execution.arrow.enabled", "true") \
        .config("spark.sql.adaptive.enabled", config['adaptive']) \
        .config("spark.sql.adaptive.coalescePartitions.enabled", config['adaptive_coalesce']) \
        .config("spark.sql.shuffle.partitions", config['shuffle_partitions']) \
        .config("spark.default.parallelism", config['parallelism']) \
        .config("spark.memory.offHeap.enabled", config['off_heap']) \
        .config("spark.memory.offHeap.size", config['off_heap_size']) \
        .getOrCreate()

def read_data(spark):
    schema = StructType([
        StructField("station", StringType(), True),
        StructField("measure", FloatType(), True),
    ])
    return spark.read.options(delimiter=";", header=False).schema(schema).csv("measurements.txt")

def analyze_temperatures(df):
    return df.groupBy("station") \
             .agg(min("measure").alias("min"),
                  avg("measure").alias("mean"),
                  max("measure").alias("max")) \
             .orderBy("station")

def format_results(df):
    return df.select("station",
                     format_number("min", 1).alias("min"),
                     format_number("mean", 1).alias("mean"),
                     format_number("max", 1).alias("max"))

def print_results(df):
    results = df.collect()
    print("{", end="")
    print(", ".join(f"{row.station}={row.min}/{row.mean}/{row.max}" for row in results), end="")
    print("}")

def main():
    spark = create_spark_session()
    try:
        df = read_data(spark)
        result_df = analyze_temperatures(df)
        formatted_df = format_results(result_df)
        print_results(formatted_df)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()