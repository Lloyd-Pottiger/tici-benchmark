import sys


def signal_handler(signum, frame):
    """Handle interrupt signals gracefully"""
    print(f"\n⚠️ Received signal {signum}, shutting down...")
    sys.exit(1)
