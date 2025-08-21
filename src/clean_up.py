#!/usr/bin/env python3
"""
Clean up script for TICI benchmark environment.
This script:
1. Reads S3 configuration from ci/test-meta.toml
2. Deletes all files under the configured bucket/prefix
"""

import os
from .config import PROJECT_DIR
from . import utils


def delete_all_files_in_prefix(s3_client, bucket: str, prefix: str):
    """
    Delete all files under the specified prefix in the S3/MinIO bucket
    """
    try:
        print(f"Deleting objects from bucket: {bucket}, prefix: {prefix}")

        # Get all files under the prefix
        files = utils.list_s3_files_with_prefix(s3_client, bucket, prefix)

        if not files:
            print(f"No files found under s3://{bucket}/{prefix}")
            return

        # Prepare objects for deletion
        objects_to_delete = [{'Key': f['key']} for f in files]

        # Delete all objects in batches (S3 allows up to 1000 objects per delete)
        for i in range(0, len(objects_to_delete), 1000):
            batch = objects_to_delete[i:i+1000]
            s3_client.delete_objects(
                Bucket=bucket,
                Delete={'Objects': batch}
            )
            print(f"Deleted {len(batch)} files")

        print(f"Successfully deleted all files under s3://{bucket}/{prefix}")

    except Exception as e:
        raise RuntimeError(f"Error deleting files: {str(e)}")


def cleanup_s3_files(config_file):
    """Main function to coordinate cleanup"""
    try:
        # Get S3 configuration and create client
        endpoint, access_key, secret_key, bucket, prefix = utils.get_s3_config(
            config_file)
        s3_client = utils.create_s3_client(endpoint, access_key, secret_key)

        print(
            f"Using S3 config from {config_file}: s3://{bucket}/{prefix} in {endpoint}")

        # Delete files in S3/MinIO
        delete_all_files_in_prefix(s3_client, bucket, prefix)

    except Exception as e:
        print(f"Error during cleanup: {e}")

    print("✅ Cleanup completed")


if __name__ == '__main__':
    cleanup_s3_files(os.path.join(PROJECT_DIR, 'config', 'test-meta.toml'))
