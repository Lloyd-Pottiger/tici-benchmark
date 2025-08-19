import signal
import sys
import argparse

from src import config
from src.runner import TICIBenchmarkRunner


def signal_handler(signum, frame):
    """Handle interrupt signals gracefully"""
    print(f"\n⚠️ Received signal {signum}, shutting down...")
    # The cleanup will be handled by the finally block in main
    sys.exit(1)


def main():
    # Set up signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

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
        "size",
        nargs="?",
        choices=config.TEST_SIZES,
        help=f"Shard max size to test (choices: {', '.join(config.TEST_SIZES)}). If not provided, all sizes will be tested."
    )

    # Parse arguments
    args = parser.parse_args()

    # Extract arguments
    worker_count = args.workers
    tiflash_count = args.tiflash
    test_size = args.size

    try:
        if test_size:
            runner = TICIBenchmarkRunner(worker_count, tiflash_count)
            runner.run(test_size)
        else:
            # Run tests for all sizes
            print(
                f"ℹ️ Running tests for all sizes: {', '.join(config.TEST_SIZES)}")
            for size in config.TEST_SIZES:
                runner = TICIBenchmarkRunner(worker_count, tiflash_count)
                runner.run(size)
                runner.stop_tiup_cluster()
    except KeyboardInterrupt:
        print("\n⚠️ Test interrupted by user")
        return 1
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return 1
    finally:
        # Ensure cleanup
        runner.stop_tiup_cluster()


if __name__ == "__main__":
    sys.exit(main())
