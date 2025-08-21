#!/usr/bin/env python3
"""
Common utility functions for TICI benchmark.
"""

import sys
import json
import mysql.connector
from mysql.connector import Error
import boto3
from botocore.client import Config
from tabulate import tabulate
from contextlib import contextmanager
from typing import Dict, List, Any, Optional, Tuple


@contextmanager
def mysql_connection(host: str, port: int, user: str = "root", database: Optional[str] = None):
    """
    Context manager for MySQL database connections.

    Args:
        host: Database host
        port: Database port
        user: Database user
        database: Database name (optional)

    Yields:
        mysql.connector.MySQLConnection: Database connection

    Raises:
        RuntimeError: If connection fails
    """
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host,
            port=port,
            user=user,
            database=database,
            connection_timeout=10
        )
        yield connection
    except Error as e:
        raise RuntimeError(f"Database connection failed: {e}")
    finally:
        if connection and connection.is_connected():
            connection.close()


def execute_sql(connection, sql: str, params: Optional[tuple] = None) -> Optional[Any]:
    """
    Execute SQL query with proper cursor management.

    Args:
        connection: Database connection
        sql: SQL query string
        params: Query parameters (optional)

    Returns:
        Query result or None

    Raises:
        RuntimeError: If query execution fails
    """
    cursor = None
    try:
        cursor = connection.cursor()
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)

        if sql.strip().upper().startswith('SELECT'):
            return cursor.fetchall()
        else:
            connection.commit()
            return cursor.rowcount
    except Error as e:
        raise RuntimeError(f"SQL execution failed: {e}")
    finally:
        if cursor:
            cursor.close()


def load_toml_config(config_file: str) -> Dict[str, Any]:
    """
    Load TOML configuration file.

    Args:
        config_file: Path to TOML configuration file

    Returns:
        Parsed configuration as dictionary

    Raises:
        RuntimeError: If config loading fails
    """
    # Try Python 3.11+ built-in tomllib first
    if sys.version_info >= (3, 11):
        import tomllib
    else:
        # For older Python versions, try to use tomli
        try:
            import tomli as tomllib
        except ImportError:
            raise RuntimeError(
                "This function requires either Python 3.11+ or the tomli package. "
                "Install tomli with: pip install tomli"
            )

    try:
        with open(config_file, "rb") as f:
            return tomllib.load(f)
    except Exception as e:
        raise RuntimeError(f"Error reading config file {config_file}: {e}")


def create_s3_client(endpoint: str, access_key: str, secret_key: str):
    """
    Create S3/MinIO client with standard configuration.

    Args:
        endpoint: S3 endpoint URL (without http://)
        access_key: S3 access key
        secret_key: S3 secret key

    Returns:
        boto3.client: Configured S3 client

    Raises:
        RuntimeError: If client creation fails
    """
    try:
        session = boto3.session.Session()
        return session.client(
            's3',
            endpoint_url=f'http://{endpoint}',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=Config(signature_version='s3v4'),
        )
    except Exception as e:
        raise RuntimeError(f"Failed to create S3 client: {e}")


def get_s3_config(config_file: str) -> Tuple[str, str, str, str, str]:
    """
    Extract S3 configuration from TOML config file.

    Args:
        config_file: Path to TOML configuration file

    Returns:
        Tuple of (endpoint, access_key, secret_key, bucket, prefix)

    Raises:
        RuntimeError: If S3 configuration not found or invalid
    """
    config_data = load_toml_config(config_file)

    if 's3' not in config_data:
        raise RuntimeError("No S3 configuration found in config file")

    s3_config = config_data['s3']
    endpoint = s3_config.get(
        'endpoint', '127.0.0.1:9000').replace('http://', '')
    access_key = s3_config.get('access_key', 'minioadmin')
    secret_key = s3_config.get('secret_key', 'minioadmin')
    bucket = s3_config.get('bucket', 'logbucket')
    prefix = s3_config.get('prefix', 'qiuyang_test')

    return endpoint, access_key, secret_key, bucket, prefix


def list_s3_files_with_prefix(s3_client, bucket: str, prefix: str) -> List[Dict[str, Any]]:
    """
    List all files in S3 bucket with given prefix.

    Args:
        s3_client: S3 client instance
        bucket: S3 bucket name
        prefix: File prefix to filter by

    Returns:
        List of file dictionaries with 'key' and 'last_modified' fields

    Raises:
        RuntimeError: If S3 operation fails
    """
    try:
        files = []
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

        for page in pages:
            if 'Contents' in page:
                for obj in page['Contents']:
                    files.append({
                        'key': obj['Key'],
                        'last_modified': obj['LastModified']
                    })

        return files
    except Exception as e:
        raise RuntimeError(f"Failed to list S3 files: {e}")


def print_benchmark_results(results: List[Dict[str, Any]], headers: List[str],
                            title: str, number_format: str = ".2f"):
    """
    Print benchmark results in a formatted table.

    Args:
        results: List of result dictionaries
        headers: Table column headers
        title: Table title
        number_format: Format string for numeric values
    """
    if not results:
        print(f"\n{title}: No results to display")
        return

    table_data = []

    # Dynamic table generation based on result keys
    for res in results:
        row = []
        for key, value in res.items():
            if isinstance(value, (int, float)):
                if key.endswith('_rows') or key == 'matched_rows':
                    row.append(f"{value:,}")
                elif isinstance(value, float):
                    row.append(f"{value:{number_format}}")
                else:
                    row.append(str(value))
            else:
                row.append(str(value))
        table_data.append(row)

    print(f"\n{title}:")
    print(tabulate(table_data, headers=headers, tablefmt="pipe"))


def format_qps_results(results: List[Dict[str, Any]]) -> None:
    """
    Format and print QPS benchmark results.

    Args:
        results: List of QPS result dictionaries
    """
    table_data = []
    for res in results:
        table_data.append([
            f"{res['matched_rows']:,}",
            f"{res['best_qps']:.2f}",
            f"{res['best_avg_latency']:.2f}",
            res['best_concurrency']
        ])

    headers = ["Matched rows", "QPS", "Average latency (ms)", "Concurrency"]
    print("\n📊 Final QPS Benchmark Results:")
    print(tabulate(table_data, headers=headers, tablefmt="pipe"))


def format_latency_results(results: List[Dict[str, Any]]) -> None:
    """
    Format and print latency benchmark results.

    Args:
        results: List of latency result dictionaries
    """
    table_data = []
    for res in results:
        table_data.append([
            f"{res['matched_rows']:,}",
            f"{res['min']:.2f}",
            f"{res['max']:.2f}",
            f"{res['avg']:.2f}"
        ])

    headers = ["Matched rows", "Min (ms)", "Max (ms)", "Avg (ms)"]
    print("\n📈 Latency Benchmark Results:")
    print(tabulate(table_data, headers=headers, tablefmt="pipe"))


def parse_mysql_connection_from_tiup_output(line: str) -> Optional[Tuple[str, int]]:
    """
    Parse MySQL connection info from TiUP output line.

    Args:
        line: TiUP output line

    Returns:
        Tuple of (host, port) or None if not found
    """
    if "Connect TiDB:" not in line:
        return None

    try:
        parts = line.split()
        host_index = parts.index("--host") + 1
        port_index = parts.index("--port") + 1

        host = parts[host_index]
        port = int(parts[port_index])

        return host, port
    except (ValueError, IndexError):
        return None


def modify_toml_config_value(config_path: str, key_path: str, new_value: str) -> None:
    """
    Modify a specific value in a TOML configuration file.

    Args:
        config_path: Path to the TOML file
        key_path: Key to modify (e.g., "max_size")
        new_value: New value to set

    Raises:
        RuntimeError: If file operations fail
    """
    try:
        # Read the current config
        with open(config_path, 'r') as f:
            lines = f.readlines()

        # Modify the specific line
        with open(config_path, 'w') as f:
            for line in lines:
                if line.strip().startswith(f'{key_path} ='):
                    f.write(f'{key_path} = "{new_value}"\n')
                else:
                    f.write(line)

    except Exception as e:
        raise RuntimeError(f"Failed to modify config file: {e}")


def validate_cdc_file_sequence(s3_client, bucket: str, directory_prefix: str,
                               expected_last_file: str) -> Tuple[bool, str, int]:
    """
    Validate that the expected CDC file is actually the latest in the S3 directory.

    Args:
        s3_client: S3 client instance
        bucket: S3 bucket name
        directory_prefix: Directory prefix to search in
        expected_last_file: Expected last file path

    Returns:
        Tuple of (is_valid, actual_last_file, total_files_count)

    Raises:
        RuntimeError: If S3 operations fail
    """
    try:
        # Get all CDC files in the directory and subdirectories
        all_files = list_s3_files_with_prefix(
            s3_client, bucket, directory_prefix + '/')

        # Filter only CDC*.json files
        cdc_files = [
            f for f in all_files
            if f['key'].endswith('.json') and '/CDC' in f['key']
        ]

        if not cdc_files:
            raise RuntimeError(f"No CDC files found under {directory_prefix}/")

        # Sort files by last modified time (descending)
        cdc_files.sort(key=lambda x: x['last_modified'], reverse=True)

        actual_last_file = cdc_files[0]['key']
        is_valid = actual_last_file == expected_last_file

        return is_valid, actual_last_file, len(cdc_files)

    except Exception as e:
        raise RuntimeError(f"Failed to validate CDC file sequence: {e}")


def safe_json_parse(json_str: str) -> Dict[str, Any]:
    """
    Safely parse JSON string with error handling.

    Args:
        json_str: JSON string to parse

    Returns:
        Parsed JSON as dictionary

    Raises:
        RuntimeError: If JSON parsing fails
    """
    try:
        return json.loads(json_str)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"Failed to parse JSON: {e}")
