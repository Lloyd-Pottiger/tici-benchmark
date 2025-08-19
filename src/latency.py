import time
import mysql.connector
from mysql.connector import Error
from statistics import mean
from tabulate import tabulate

from . import config


def run_query_benchmark(host, port, user, database, query_template, word, iterations=10):
    connection = None
    total_times = []

    try:
        # Connect to MySQL server
        connection = mysql.connector.connect(
            host=host,
            port=port,
            user=user,
            database=database
        )

        if connection.is_connected():
            cursor = connection.cursor()

            for _ in range(iterations):
                query = query_template.replace("xxxx", word[0])

                start_time = time.time()
                cursor.execute(query)
                results = cursor.fetchall()
                elapsed_time = (time.time() - start_time) * \
                    1000  # Convert to milliseconds
                total_times.append(elapsed_time)
                time.sleep(0.01)

            return {
                'matched_rows': word[1],
                'min': min(total_times),
                'max': max(total_times),
                'avg': mean(total_times)
            }

    except Error as e:
        print(f"Error: {e}")
        return None

    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()


if __name__ == "__main__":
    # Configuration
    host = "127.0.0.1"
    port = 4000
    user = "root"
    database = "test"

    # Collect all results
    results = []
    for word in config.WORD_LIST:
        print(
            f"Running benchmark for word: '{word[0]}', matched rows: {word[1]}")
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
    table_data = []
    for res in results:
        table_data.append([
            f"{res['matched_rows']:,}",
            f"{res['min']:.2f}",
            f"{res['max']:.2f}",
            f"{res['avg']:.2f}"
        ])

    headers = ["Matched rows", "Min (ms)", "Max (ms)", "Avg (ms)"]
    print("\nBenchmark Results:")
    print(tabulate(table_data, headers=headers, tablefmt="pipe"))
