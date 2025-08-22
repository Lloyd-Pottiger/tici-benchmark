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
        '--max_rows',
        type=int,
        default=1000000,
        help='Maximum number of rows to process (default: 1000000)'
    )
    parser.add_argument(
        "size",
        nargs="?",
        choices=config.TEST_SIZES,
        help=f"Shard max size to test (choices: {', '.join(config.TEST_SIZES)}). If not provided, all sizes will be tested."
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
        if shard_size:
            runner = TICIBenchmarkRunner(worker_count, tiflash_count, max_rows, shard_size)
            runner.run()
        else:
            # Run tests for all sizes
            print(f"ℹ️ Running tests for all sizes: {', '.join(config.TEST_SIZES)}")
            for size in config.TEST_SIZES:
                runner = TICIBenchmarkRunner(worker_count, tiflash_count, max_rows, shard_size=size)
                runner.run()
                runner.stop_tiup_cluster()
                runner.cleanup()
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
