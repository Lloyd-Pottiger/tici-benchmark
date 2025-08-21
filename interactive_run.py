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
        help=f"Number of TICI worker nodes (default: 1)"
    )
    parser.add_argument(
        "--tiflash",
        type=int,
        default=1,
        help=f"Number of TiFlash instances (default: 1)"
    )
    parser.add_argument(
        "--size",
        type=str,
        default="16MB",
        choices=config.TEST_SIZES,
        help=f"Shard max size to test (choices: {', '.join(config.TEST_SIZES)})."
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

    try:
        print(
            f"🎯 Starting test with shard.max_size = {shard_size}, tiflash_num = {tiflash_count}, worker_num = {worker_count}")
        runner = TICIBenchmarkRunner(
            worker_count, tiflash_count, shard_size=shard_size)
        # Step 1: Modify config
        runner.modify_config()
        # Step 2: Start cluster
        runner.start_tiup_cluster()
        # Step 3: Insert data
        runner.insert_test_data()

        if input("Do you want to create a fulltext index? (y/n): ").strip().lower() == 'y':
            # Step 4: Create index
            runner.create_fulltext_index()
            # Step 5: Verify index
            runner.verify_index_creation()

        if input("Do you want to run QPS benchmark? (y/n): ").strip().lower() == 'y':
            # Step 6: Run QPS benchmark
            runner.run_qps_benchmark()

        if input("Do you want to run latency benchmark? (y/n): ").strip().lower() == 'y':
            # Step 7: Run latency benchmark
            runner.run_latency_benchmark()

    except KeyboardInterrupt:
        print("\n⚠️ Test interrupted by user")
        return 1
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return 1
    finally:
        # Ensure cleanup
        runner.cleanup()
        runner.stop_tiup_cluster()


if __name__ == "__main__":
    sys.exit(main())
