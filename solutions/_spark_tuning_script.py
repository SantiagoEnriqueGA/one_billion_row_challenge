import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import min, max, avg, format_number, col
from pyspark.sql.types import StructType, StructField, FloatType, StringType
import time
import itertools
import logging
from functools import wraps

# Set up logging
logging.basicConfig(filename='spark_benchmark.log', level=logging.INFO,
                    format='%(asctime)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

def timer(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        duration = end_time - start_time
        return result, duration
    return wrapper

def create_spark_session(config):
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

@timer
def read_data(spark, config):
    schema = StructType([
        StructField("station", StringType(), False),
        StructField("measure", FloatType(), False),
    ])
    df = spark.read.options(delimiter=";", header=False) \
               .schema(schema) \
               .csv("measurements.txt")
    
    if config['repartition']:
        df = df.repartition(config['num_partitions'], col("station"))
    
    return df

@timer
def analyze_temperatures(df, config):
    result = df.groupBy("station") \
             .agg(min("measure").alias("min"),
                  avg("measure").alias("mean"),
                  max("measure").alias("max")) \
             .orderBy("station")
    
    if config['cache']:
        result = result.cache()
    
    return result

@timer
def format_results(df):
    return df.select("station",
                     format_number("min", 1).alias("min"),
                     format_number("mean", 1).alias("mean"),
                     format_number("max", 1).alias("max"))

def run_job(config):
    spark = create_spark_session(config)
    try:
        df, read_time = read_data(spark, config)
        result_df, analyze_time = analyze_temperatures(df, config)
        formatted_df, format_time = format_results(result_df)
        formatted_df.collect()  # Force evaluation
        total_time = read_time + analyze_time + format_time
        return total_time, read_time, analyze_time, format_time
    except Exception as e:
        logging.error(f"Error running job with config {config}: {str(e)}")
        return None
    finally:
        spark.stop()

def generate_configs():
    base_config = {
        'adaptive': True,
        'adaptive_coalesce': True,
        'shuffle_partitions': 200,
        'parallelism': 200,
        'off_heap': True,
        'off_heap_size': '10g',
        'repartition': True,
        'num_partitions': 200,
        'cache': True
    }
    
    # Parameters to tune
    params_to_tune = {
        'shuffle_partitions': [100, 200, 400],
        'parallelism': [100, 200, 400],
        'off_heap_size': ['5g', '10g', '15g'],
        'num_partitions': [100, 200, 400],
        'cache': [True, False],
        'repartition': [True, False]
    }
    
    for values in itertools.product(*params_to_tune.values()):
        config = base_config.copy()
        config.update(dict(zip(params_to_tune.keys(), values)))
        yield config

def benchmark():
    results = []
    for config in generate_configs():
        result = run_job(config)
        if result is not None:
            total_time, read_time, analyze_time, format_time = result
            results.append((config, total_time, read_time, analyze_time, format_time))
            log_result(config, total_time, read_time, analyze_time, format_time)
        else:
            logging.warning(f"Skipping failed configuration: {config}")
    
    return results

def log_result(config, total_time, read_time, analyze_time, format_time):
    log_msg = f"""
    Configuration:
    {config}
    Performance:
    Total time: {total_time:.2f}s
    Read time: {read_time:.2f}s
    Analyze time: {analyze_time:.2f}s
    Format time: {format_time:.2f}s
    """
    logging.info(log_msg)

def find_best_config(results):
    if not results:
        return None
    return min(results, key=lambda x: x[1])

if __name__ == "__main__":
    print("Starting benchmark...")
    results = benchmark()
    best_result = find_best_config(results)
    if best_result:
        best_config, best_time, _, _, _ = best_result
        print(f"Best configuration found:")
        print(f"Configuration: {best_config}")
        print(f"Total execution time: {best_time:.2f}s")
    else:
        print("No successful configurations found.")
    print("Check spark_benchmark.log for full results.")