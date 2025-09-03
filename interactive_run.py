import argparse
import signal
import sys

from src import config
from src.runner import TICIBenchmarkRunner
from src.signal_handler import signal_handler


def parse_args():
    # Configure argument parser
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    # Add arguments
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help=f"Number of TICI worker nodes (default: 1)",
    )
    parser.add_argument(
        "--tiflash",
        type=int,
        default=1,
        help=f"Number of TiFlash instances (default: 1)",
    )
    parser.add_argument(
        '--max_rows',
        type=int,
        default=1000000,
        help='Maximum number of rows to process (default: 1000000)'
    )
    parser.add_argument(
        "--size",
        type=str,
        default="32MB",
        choices=config.TEST_SIZES,
        help=f"Shard max size to test (choices: {', '.join(config.TEST_SIZES)}).",
    )

    # Parse arguments
    return parser.parse_args()


def main():
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    args = parse_args()
    worker_count = args.workers
    tiflash_count = args.tiflash
    shard_size = args.size
    max_rows = args.max_rows

    try:
        print(
            f"🎯 Starting test with shard.max_size = {shard_size}, tiflash_num = {tiflash_count}, worker_num = {worker_count}, max_rows = {max_rows}"
        )

        mysql_info = input("Input mysql host and port (host:port) or press Enter to start new cluster: ").strip()
        if mysql_info:
            if ':' not in mysql_info:
                raise ValueError("Invalid format. Please enter in 'host:port' format.")
            host, port_str = mysql_info.split(':', 1)
            runner = TICIBenchmarkRunner(
                worker_count,
                tiflash_count,
                max_rows,
                shard_size,
                mysql_host=host,
                mysql_port=int(port_str)
            )
        else:
            runner = TICIBenchmarkRunner(worker_count, tiflash_count, max_rows, shard_size)

        # Create table
        runner.create_table()
        table_id, index_id = None, None

        while True:
            input_choice = input(
                "What would you like to do next? (1: Create index, 2: Insert data, 3: Run QPS benchmark, 4: Run latency benchmark, 5: Stop cluster, q: Quit): "
            ).strip()
            if input_choice == '1':
                # Create index
                try:
                    sql = input("Enter SQL statement to create fulltext index: ")
                    if sql:
                        table_id, index_id = runner.create_fulltext_index(sql)
                    else:
                        table_id, index_id = runner.create_fulltext_index()
                except Exception as e:
                    print(f"❌ Error creating index: {e}")
            elif input_choice == '2':
                # Insert data
                runner.insert_test_data()
                if table_id and index_id:
                    runner.verify_index_creation(table_id, index_id)
            elif input_choice == '3':
                # Run QPS benchmark
                runner.run_qps_benchmark()
            elif input_choice == '4':
                # Run latency benchmark
                runner.run_latency_benchmark()
            elif input_choice == '5':
                # Stop cluster
                runner.stop_tiup_cluster()
                runner.cleanup()
                break
            elif input_choice == 'q':
                # Quit
                break
            else:
                print("Invalid choice, please try again.")

    except KeyboardInterrupt:
        print("\n⚠️ Test interrupted by user")
        return 1
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return 1
    finally:
        # Ensure cleanup
        runner.stop_tiup_cluster()
        runner.cleanup()


if __name__ == "__main__":
    sys.exit(main())
