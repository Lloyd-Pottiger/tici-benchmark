import time
from statistics import mean
import multiprocessing
from itertools import repeat

from . import config
from . import utils


def worker(host, port, user, database, query_template, word, duration):
    """
    This function is executed by each worker process.
    It connects to the DB, runs queries for a set duration, and returns its performance metrics.
    """
    latencies = []
    query_count = 0

    # Each process must create its own connection.
    # Connections cannot be shared across processes.
    try:
        with utils.mysql_connection(host, port, user, database) as connection:
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

            cursor.close()
            return (query_count, latencies)

    except Exception as e:
        # If a worker fails to connect or execute, it returns 0 results.
        print(f"[Process-{multiprocessing.current_process().pid}] Error: {e}")
        return (0, [])


def get_qps(host, port, user, database, query_template, word, matched_rows, concurrency):
    with multiprocessing.Pool(processes=concurrency) as pool:
        worker_args = repeat((host, port, user, database, query_template, word, config.TEST_DURATION), concurrency)
        start_time = time.time()
        worker_results = pool.starmap(worker, worker_args)
        end_time = time.time()

    total_queries = sum(res[0] for res in worker_results)
    all_latencies = [latency for res in worker_results for latency in res[1]]

    actual_duration = end_time - start_time
    qps = total_queries / actual_duration if actual_duration > 0 else 0
    avg_latency_ms = mean(all_latencies) * 1000 if all_latencies else 0

    print(f"Concurrency: {concurrency}, QPS: {qps:.2f}, Avg Latency: {avg_latency_ms:.2f} ms")
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
        all_results.append(get_qps(host, port, user, database, query_template, word, matched_rows, concurrency))

    # Find the best performing concurrency level (highest QPS)
    best_result = max(all_results, key=lambda x: x['qps']) if all_results else None

    return {
        "matched_rows": matched_rows,
        "best_qps": best_result['qps'] if best_result else 0,
        "best_avg_latency": best_result['avg_latency'] if best_result else 0,
        "best_concurrency": best_result['concurrency'] if best_result else 0
    }
