#!/usr/bin/env python3
"""
Clean up script for TICI benchmark environment.
This script:
1. Reads S3 configuration from ci/test-meta.toml
2. Deletes all files under the configured bucket/prefix
"""

import os
import sys
import boto3
from botocore.client import Config
from .config import PROJECT_DIR


def load_toml_config(config_file):
    """
    Load the TOML configuration file using the appropriate library.
    Returns the parsed config as a dictionary.
    """
    # Try Python 3.11+ built-in tomllib first
    if sys.version_info >= (3, 11):
        import tomllib
    else:
        # For older Python versions, try to use tomli
        try:
            import tomli as tomllib
        except ImportError:
            print("Error: This script requires either Python 3.11+ or the tomli package")
            print("Install tomli with: pip install tomli")
            sys.exit(1)

    try:
        with open(config_file, "rb") as f:
            return tomllib.load(f)
    except Exception as e:
        print(f"Error reading config file {config_file}: {e}")
        sys.exit(1)


def delete_all_files_in_prefix(endpoint, access_key, secret_key, bucket, prefix):
    """
    Delete all files under the specified prefix in the S3/MinIO bucket
    """
    try:
        # Initialize S3 client
        session = boto3.session.Session()
        s3_client = session.client(
            's3',
            endpoint_url=f'http://{endpoint}',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=Config(signature_version='s3v4'),
        )

        print(f"Connected to S3 endpoint: {endpoint}")
        print(f"Deleting objects from bucket: {bucket}, prefix: {prefix}")

        # List all objects under the prefix
        objects_to_delete = []

        # Paginate through all objects in case there are many
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

        for page in pages:
            if 'Contents' in page:
                for obj in page['Contents']:
                    objects_to_delete.append({'Key': obj['Key']})

        if not objects_to_delete:
            print(f"No files found under s3://{bucket}/{prefix}")
            return

        # Delete all objects in batches (S3 allows up to 1000 objects per delete)
        for i in range(0, len(objects_to_delete), 1000):
            batch = objects_to_delete[i:i+1000]
            response = s3_client.delete_objects(
                Bucket=bucket,
                Delete={'Objects': batch}
            )
            print(f"Deleted {len(batch)} files")

        print(f"Successfully deleted all files under s3://{bucket}/{prefix}")

    except Exception as e:
        print(f"Error deleting files: {str(e)}")


def cleanup_s3_files(config_file):
    """Main function to coordinate cleanup"""
    print("🧹 Starting cleanup process...")

    # Load configuration from TOML file
    config = load_toml_config()

    # Extract S3 configuration
    if 's3' in config:
        s3_config = config['s3']
        endpoint = s3_config.get(
            'endpoint', '127.0.0.1:9000').replace('http://', '')
        region = s3_config.get('region', 'us-east-1')
        access_key = s3_config.get('access_key', 'minioadmin')
        secret_key = s3_config.get('secret_key', 'minioadmin')
        bucket = s3_config.get('bucket', 'logbucket')
        prefix = s3_config.get('prefix', 'qiuyang_test')

        print(f"Using S3 config from {config_file}:")
        print(f"  Endpoint: {endpoint}")
        print(f"  Region: {region}")
        print(f"  Bucket: {bucket}")
        print(f"  Prefix: {prefix}")

        # Delete files in S3/MinIO
        delete_all_files_in_prefix(
            endpoint, access_key, secret_key, bucket, prefix)
    else:
        print(f"Error: No S3 configuration found in {config_file}")

    print("✅ Cleanup completed")


if __name__ == '__main__':
    cleanup_s3_files(os.path.join(PROJECT_DIR, 'config', 'test-meta.toml'))
