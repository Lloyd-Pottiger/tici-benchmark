import time
from statistics import mean

from . import config
from . import utils


def run_query_benchmark(host, port, user, database, query_template, word, iterations=10):
    total_times = []

    try:
        # Connect to MySQL server using utils
        with utils.mysql_connection(host, port, user, database) as connection:
            cursor = connection.cursor()

            for _ in range(iterations):
                query = query_template.replace("xxxx", word[0])

                start_time = time.time()
                cursor.execute(query)
                results = cursor.fetchall()
                # Convert to milliseconds
                elapsed_time = (time.time() - start_time) * 1000
                total_times.append(elapsed_time)
                time.sleep(0.01)

            cursor.close()
            return {
                'matched_rows': word[1],
                'min': min(total_times),
                'max': max(total_times),
                'avg': mean(total_times)
            }

    except Exception as e:
        print(f"Error: {e}")
        return None


if __name__ == "__main__":
    # Configuration
    host = "127.0.0.1"
    port = 4000
    user = "root"
    database = "test"

    # Collect all results
    results = []
    for word in config.WORD_LIST:
        print(f"Running benchmark for word: '{word[0]}', matched rows: {word[1]}")
        result = run_query_benchmark(
            host=host,
            port=port,
            user=user,
            database=database,
            query_template=config.QUERY_TEMPLATE,
            word=word,
            iterations=100
        )
        if result:
            results.append(result)

    # Format and print the table
    utils.format_latency_results(results)
