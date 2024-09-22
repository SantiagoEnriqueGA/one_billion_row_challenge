
# One Billion Row Challenge

The One Billion Row Challenge (1BRC) is an exploration of modern data processing capabilities by aggregating temperature data from one billion rows. This project implements the challenge in Python, focusing on performance optimization through various libraries and techniques.

## Overview

The task involves reading a text file containing temperature measurements for various weather stations. Each row is formatted as follows:

```
<station_name>;<measurement>
```

For example:

```
Hamburg;12.0
Bulawayo;8.9
Palembang;38.8
```

The goal is to compute the minimum, mean, and maximum temperature values per weather station and display the results in a sorted format:

```
{station_name=min/mean/max,
station_name=min/mean/max,
...,
station_name=min/mean/max}
```

## Project Structure

- **solutions/**: Contains implementations of the challenge using different libraries and techniques.
- **timeAndCompare.py**: Runs implementations and averages performance from the different runs.

## Performance Results

![alt text](https://raw.githubusercontent.com/SantiagoEnriqueGA/one_billion_row_challenge/refs/heads/main/timeVisualized.png?token=GHSAT0AAAAAACPDF6ZO6AL3PTRUY7VZDMYMZXQSLOQ)


**Note**: All tests were conducted on a Windows 10 system with an AMD Ryzen 9 4900HS @ 3.00 GHz and 16.0 GB RAM.


## Conclusion

The performance results from this challenge illustrate the significant advantages of parallel processing and optimized libraries in handling large datasets.

1. **Single-threaded Performance**:
   - The `python-singleCore` implementation demonstrates the limitations of single-threaded processing, taking over **659 seconds**. This is the most striaghtforward implementation, it serves as a baseline and highlights the inefficiencies inherent in processing large datasets sequentially.
   - The `pypy-singleCore` implementation demonstrates how leveraging just-in-time compilation with pypy can lead to considerable performance gains over standard Python cutting total time in half from **659 seconds** to **313 seconds**.

2. **Optimized Libraries**:
   - Using `pandas` substantial improvements over the baseline, with times dropping to **479 seconds** for `pandas-pypy` and **347 seconds** for `pandas-cpy`. These results are an improvement but leave a lot of perforamnce on the table.

3. **Concurrent Processing**:
   - Transitioning to multi-core processing dramatically reduces execution time. The `python-multiCore-multiprocess` implementation completes in **119 seconds**
   - The `python-multiCore-concurrent` implementation is slightly faster at **114 seconds**. This clearly demonstrates the benefits of concurrent execution in a multi-core environment.

4. **Frameworks Designed for Big Data**:
   - Tools like `pyspark` and `dask` further enhance performance, with runtimes of **74 seconds** and **41.5 seconds**, respectively.
   - These frameworks are optimized for distributed data processing and effectively manage large volumes of data, showcasing their suitability for big data tasks.
   - I suspect further optamizations can be performed on the the `pyspark` implementation.

5. **Concurrent Processing 2**:
   - The best-performing "base python" implementations utilize `pypy` in conjunction with multi-core processing.
   - The `pypy-multiCore-concurrent` runs in **42.7 seconds**, while `pypy-multiCore-multiprocess` finishes in **40.8 seconds**, demonstrating that combining JIT compilation with parallel processing is highly effective.

6. **High-Performance Libraries**:
   - Finally, implementations using `polars` and `duckdb` show remarkable performance, with `polars-pypy` finishing in **26.8 seconds** and `duckdb` at **20.7 seconds**.
   - These libraries are specifically designed for high-performance data manipulation and retrieval, making them ideal for large-scale data tasks.

### Conclusion

Overall, the results emphasize the importance of selecting the right tools and techniques when working with large datasets. 
By leveraging concurrent processing and optimized libraries, the runtime can be reduced significantly, allwoing for efficient processing this billion row dataset. 


