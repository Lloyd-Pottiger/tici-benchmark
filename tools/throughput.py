#!/usr/bin/env python3
"""
Throughput calculator for TICI shard writer logs.
Parses log lines and calculates throughput in MiB/s.
"""

import re
import sys
import argparse
from typing import List, Tuple, Optional


def parse_log_line(line: str) -> Optional[Tuple[str, int, float, float]]:
    """
    Parse a log line and extract relevant information.

    Args:
        line: Log line to parse

    Returns:
        Tuple of (frag_path, docs, bytes, elapsed_ms) or None if parsing fails
    """
    # Pattern to match the log format
    pattern = r'frag_path=([^,]+),.*docs=(\d+), bytes=(\d+.\d+)/\d+.\d+\w+, elapsed=(\d+)/\d+\w+'

    match = re.search(pattern, line)
    if match:
        frag_path = match.group(1)
        docs = int(match.group(2))
        mb_written = float(match.group(3))
        elapsed_ms = int(match.group(4))
        return frag_path, docs, mb_written, elapsed_ms

    return None


def calculate_throughput(mb_written: float, elapsed_ms: int) -> float:
    """
    Calculate throughput in MiB/s.

    Args:
        mb_written: Number of megabytes written
        elapsed_ms: Elapsed time in milliseconds

    Returns:
        Throughput in MiB/s
    """
    if elapsed_ms == 0:
        return 0.0

    # Convert milliseconds to seconds
    elapsed_s = elapsed_ms / 1000.0

    # Convert bytes to MiB and calculate throughput
    throughput_mibs = mb_written / elapsed_s

    return throughput_mibs


def process_log_file(file_path: str) -> List[dict]:
    """
    Process a log file and extract throughput data.

    Args:
        file_path: Path to the log file

    Returns:
        List of dictionaries containing throughput data
    """
    results = []

    try:
        with open(file_path, 'r') as file:
            for line_num, line in enumerate(file, 1):
                line = line.strip()
                if 'switch trigger by' in line:
                    parsed = parse_log_line(line)
                    if parsed:
                        frag_path, docs, mb_written, elapsed_ms = parsed
                        throughput = calculate_throughput(mb_written, elapsed_ms)

                        results.append({
                            'line_number': line_num,
                            'frag_path': frag_path,
                            'docs': docs,
                            'mb_written': mb_written,
                            'elapsed_ms': elapsed_ms,
                            'throughput_mibs': throughput,
                            'trigger_type': 'size_limit' if 'size_limit' in line else 'timeout'
                        })
                    else:
                        print(f"Warning: Could not parse line {line_num}: {line}")

    except FileNotFoundError:
        print(f"Error: File '{file_path}' not found")
        return []
    except Exception as e:
        print(f"Error reading file: {e}")
        return []

    return results


def process_log_lines(lines: List[str]) -> List[dict]:
    """
    Process log lines from stdin or a list.

    Args:
        lines: List of log lines to process

    Returns:
        List of dictionaries containing throughput data
    """
    results = []

    for line_num, line in enumerate(lines, 1):
        line = line.strip()
        if 'FragWriter switch trigger' in line:
            parsed = parse_log_line(line)
            if parsed:
                frag_path, docs, bytes_written, elapsed_ms = parsed
                throughput = calculate_throughput(bytes_written, elapsed_ms)

                results.append({
                    'line_number': line_num,
                    'frag_path': frag_path,
                    'docs': docs,
                    'bytes': bytes_written,
                    'elapsed_ms': elapsed_ms,
                    'throughput_mibs': throughput,
                    'trigger_type': 'size_limit' if 'size_limit' in line else 'timeout'
                })
            else:
                print(f"Warning: Could not parse line {line_num}: {line}")

    return results


def print_results(results: List[dict], show_details: bool = False):
    """
    Print throughput calculation results.

    Args:
        results: List of throughput data
        show_details: Whether to show detailed information
    """
    if not results:
        print("No valid log entries found.")
        return

    print(f"{'='*80}")
    print(f"Throughput Analysis - Found {len(results)} entries")
    print(f"{'='*80}")

    if show_details:
        print(f"{'Line':<6} {'Fragment Path':<35} {'Docs':<8} {'Bytes':<12} {'Time(ms)':<10} {'Throughput':<12} {'Trigger'}")
        print(f"{'-'*6} {'-'*35} {'-'*8} {'-'*12} {'-'*10} {'-'*12} {'-'*10}")

        for result in results:
            print(f"{result['line_number']:<6} "
                  f"{result['frag_path'][-35:]:<35} "
                  f"{result['docs']:<8} "
                  f"{result['bytes']:<12} "
                  f"{result['elapsed_ms']:<10} "
                  f"{result['throughput_mibs']:<12.2f} "
                  f"{result['trigger_type']}")

    # Calculate statistics
    throughputs = [r['throughput_mibs'] for r in results]
    size_limit_throughputs = [r['throughput_mibs'] for r in results if r['trigger_type'] == 'size_limit']
    timeout_throughputs = [r['throughput_mibs'] for r in results if r['trigger_type'] == 'timeout']

    print(f"\n{'Summary Statistics':<20}")
    print(f"{'-'*40}")
    print(f"{'Total entries:':<20} {len(results)}")
    print(f"{'Size limit triggers:':<20} {len(size_limit_throughputs)}")
    print(f"{'Timeout triggers:':<20} {len(timeout_throughputs)}")
    print(f"{'Average throughput:':<20} {sum(throughputs)/len(throughputs):.2f} MiB/s")
    print(f"{'Max throughput:':<20} {max(throughputs):.2f} MiB/s")
    print(f"{'Min throughput:':<20} {min(throughputs):.2f} MiB/s")

    if size_limit_throughputs:
        print(f"{'Avg (size_limit):':<20} {sum(size_limit_throughputs)/len(size_limit_throughputs):.2f} MiB/s")
    if timeout_throughputs:
        print(f"{'Avg (timeout):':<20} {sum(timeout_throughputs)/len(timeout_throughputs):.2f} MiB/s")


def main():
    parser = argparse.ArgumentParser(description='Calculate throughput from TICI shard writer logs')
    parser.add_argument('file', nargs='?', help='Log file to process (if not specified, reads from stdin)')
    parser.add_argument('-d', '--details', action='store_true', help='Show detailed information for each entry')
    parser.add_argument('--example', action='store_true', help='Show example with provided log lines')

    args = parser.parse_args()

    if args.example:
        # Example with the provided log lines
        example_lines = [
            "[2025-09-01T08:02:00Z INFO  tici_shard::writer::cdc_log_frag_writer] t_124/i_2/s_1 switch trigger by timeout, frag_path=fragments/t_124/i_2/f_460511958895951876, watermark=, stats: docs=1, bytes=0/5MiB, elapsed=10308/5000ms.",
            "[2025-09-01T08:02:00Z INFO  tici_shard::writer::cdc_log_frag_writer] t_124/i_2/s_1 switch trigger by size_limit, frag_path=fragments/t_124/i_2/f_460511961596035077, watermark=, stats: docs=23405, bytes=5/5MiB, elapsed=98/5000ms.",
            "[2025-09-01T08:02:00Z INFO  tici_shard::writer::cdc_log_frag_writer] t_124/i_2/s_1 switch trigger by size_limit, frag_path=fragments/t_124/i_2/f_460511961621987334, watermark=, stats: docs=23490, bytes=5/5MiB, elapsed=113/5000ms.",
            "[2025-09-01T08:02:06Z INFO  tici_shard::writer::cdc_log_frag_writer] t_124/i_2/s_1 switch trigger by timeout, frag_path=fragments/t_124/i_2/f_460511961648201738, watermark=qiuyang_test/cdc/test/hdfs_10w/460511958895951884/2025-09-01/CDC00000000000000000001.json, stats: docs=3105, bytes=0/5MiB, elapsed=5314/5000ms.",
            "[2025-09-01T08:02:06Z INFO  tici_shard::writer::cdc_log_frag_writer] t_124/i_2/s_1 switch trigger by size_limit, frag_path=fragments/t_124/i_2/f_460511963050672129, watermark=qiuyang_test/cdc/test/hdfs_10w/460511958895951884/2025-09-01/CDC00000000000000000001.json, stats: docs=23351, bytes=5/5MiB, elapsed=141/5000ms.",
            "[2025-09-01T08:02:06Z INFO  tici_shard::writer::cdc_log_frag_writer] t_124/i_2/s_1 switch trigger by size_limit, frag_path=fragments/t_124/i_2/f_460511963076886539, watermark=qiuyang_test/cdc/test/hdfs_10w/460511958895951884/2025-09-01/CDC00000000000000000001.json, stats: docs=23342, bytes=5/5MiB, elapsed=141/5000ms.",
        ]

        print("Example calculation with provided log lines:")
        print("=" * 80)

        results = process_log_lines(example_lines)
        print_results(results, show_details=True)

        # Show manual calculation for verification
        print(f"\nManual calculation verification:")
        print(f"Line 1: 5243014 bytes / 2025 ms / 1024 / 1024 = {5243014 / (2025/1000) / 1024 / 1024:.2f} MiB/s")
        print(f"Line 2: 4683865 bytes / 7153 ms / 1024 / 1024 = {4683865 / (7153/1000) / 1024 / 1024:.2f} MiB/s")
        return

    if args.file:
        # Process file
        results = process_log_file(args.file)
    else:
        # Read from stdin
        try:
            lines = sys.stdin.readlines()
            results = process_log_lines(lines)
        except KeyboardInterrupt:
            print("\nInterrupted by user")
            return

    print_results(results, show_details=args.details)


if __name__ == "__main__":
    main()
