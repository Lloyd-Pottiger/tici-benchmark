import time
import mysql.connector
from mysql.connector import Error
from statistics import mean
import multiprocessing
from itertools import repeat
from tabulate import tabulate

from . import config


def worker(host, port, user, database, query_template, word, duration):
    """
    This function is executed by each worker process.
    It connects to the DB, runs queries for a set duration, and returns its performance metrics.
    """
    connection = None
    latencies = []
    query_count = 0

    # Each process must create its own connection.
    # Connections cannot be shared across processes.
    try:
        connection = mysql.connector.connect(
            host=host,
            port=port,
            user=user,
            database=database,
            # Set a timeout for the connection attempt
            connection_timeout=10
        )
        cursor = connection.cursor()

        start_test_time = time.time()

        # Run queries until the test duration has elapsed
        while time.time() - start_test_time < duration:
            query = query_template.replace("xxxx", word)

            start_query_time = time.time()
            cursor.execute(query)
            # Fetching is important to ensure the query is fully processed by the DB
            cursor.fetchall()
            end_query_time = time.time()

            latencies.append(end_query_time - start_query_time)
            query_count += 1

        return (query_count, latencies)

    except Error as e:
        # If a worker fails to connect or execute, it returns 0 results.
        print(f"[Process-{multiprocessing.current_process().pid}] Error: {e}")
        return (0, [])

    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()


def get_qps(host, port, user, database, query_template, word, matched_rows, concurrency):
    with multiprocessing.Pool(processes=concurrency) as pool:
        worker_args = repeat(
            (host, port, user, database, query_template, word, config.TEST_DURATION), concurrency)
        start_time = time.time()
        worker_results = pool.starmap(worker, worker_args)
        end_time = time.time()

    total_queries = sum(res[0] for res in worker_results)
    all_latencies = [latency for res in worker_results for latency in res[1]]

    actual_duration = end_time - start_time
    qps = total_queries / actual_duration if actual_duration > 0 else 0
    avg_latency_ms = mean(all_latencies) * 1000 if all_latencies else 0

    print(
        f"Concurrency: {concurrency}, QPS: {qps:.2f}, Avg Latency: {avg_latency_ms:.2f} ms")
    return {
        "matched": f"({word}: {matched_rows})",
        "concurrency": concurrency,
        "qps": qps,
        "avg_latency": avg_latency_ms
    }


def get_peak_qps(host, port, user, database, query_template, word, matched_rows):
    multiprocessing.set_start_method("spawn", force=True)
    all_results = []

    for concurrency in config.CONCURRENCY_LEVELS:
        all_results.append(get_qps(host, port, user, database,
                           query_template, word, matched_rows, concurrency))

    # Find the best performing concurrency level (highest QPS)
    best_result = max(
        all_results, key=lambda x: x['qps']) if all_results else None

    return {
        "matched_rows": matched_rows,
        "best_qps": best_result['qps'] if best_result else 0,
        "best_avg_latency": best_result['avg_latency'] if best_result else 0,
        "best_concurrency": best_result['concurrency'] if best_result else 0
    }


if __name__ == "__main__":
    final_results = []
    host = "127.0.0.1"
    port = 4000
    user = "root"
    database = "test"

    for word, rows in config.WORD_LIST:
        print(
            f"\n🚀 Starting concurrent benchmark for word: '{word}', matched rows: {rows}")
        print("-" * 50)

        result = get_peak_qps(host, port, user, database,
                              config.QUERY_TEMPLATE, word, rows)
        final_results.append(result)

    # Format and print the final table
    table_data = []
    for res in final_results:
        table_data.append([
            f"{res['matched_rows']:,}",
            f"{res['best_qps']:.2f}",
            f"{res['best_avg_latency']:.2f}",
            res['best_concurrency']
        ])

    headers = ["Matched rows", "QPS", "Average latency (ms)", "Concurrency"]
    print("\n📊 Final Benchmark Results:")
    print(tabulate(table_data, headers=headers, tablefmt="pipe"))
