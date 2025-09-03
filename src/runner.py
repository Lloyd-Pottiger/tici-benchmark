#!/usr/bin/env python3
"""
Complete benchmark automation script for TICI FTS testing.

This script performs the following steps:
1. Modifies config/test-meta.toml with different shard.max_size values
2. Starts TiDB cluster with tiup playground
3. Inserts test data using read_hdfs.py
4. Creates fulltext index
5. Verifies index creation
6. Runs concurrent QPS benchmark (fts_qps.py)
7. Runs latency benchmark (fts_run.py)
8. Cleans up resources
"""

import os
import time
import subprocess
import signal
from . import config
from . import insert_data
from . import qps
from . import latency
from . import clean_up
from . import utils


class TICIBenchmarkRunner:
    def __init__(
        self, worker_count=1, tiflash_count=1, max_rows=1000000, shard_size="32MB", mysql_host=None, mysql_port=None
    ):
        self.tiup_process = None
        self.shard_size = shard_size
        self.worker_count = worker_count
        self.tiflash_count = tiflash_count
        self.max_rows = max_rows
        if mysql_host is None and mysql_port is None:
            self.modify_config()
            self.start_tiup_cluster()
        else:
            self.mysql_host = mysql_host
            self.mysql_port = mysql_port

    def modify_config(self):
        """Modify config/test-meta.toml with the specified shard.max_size"""
        config_path = os.path.join(
            config.PROJECT_DIR, "config", "test-meta.toml")
        print(f"📝 Modifying config: shard.max_size = {self.shard_size}")

        utils.modify_toml_config_value(
            config_path, "max_size", self.shard_size)
        print(f"✅ Config updated: max_size = {self.shard_size}")

    def start_tiup_cluster(self):
        """Start TiDB cluster using tiup playground"""
        print(
            f"🚀 Starting TiUP cluster (workers: {self.worker_count}, tiflash: {self.tiflash_count})"
        )

        # Stop any existing cluster
        self.stop_tiup_cluster()

        cmd = [
            "tiup", f"playground:{config.TIUP_VERSION}",
            f"{config.TIDB_VERSION}",
            "--ticdc", "1",
            "--tici.meta", "1",
            "--tici.worker", str(self.worker_count),
            "--tiflash", str(self.tiflash_count),
            "--tici.config", "./config",
        ]

        print(f"Command: {' '.join(cmd)}")

        # Change to project directory
        os.chdir(config.PROJECT_DIR)

        # Start cluster in background
        self.tiup_process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            preexec_fn=os.setsid,  # Create new process group
        )

        # Wait for cluster to start and extract MySQL connection info
        print("⏳ Waiting for cluster to start...")
        self.extract_mysql_info_from_tiup_output()
        print(f"✅ TiUP cluster is ready!")

    def stop_tiup_cluster(self):
        """Stop the TiUP cluster"""
        print("🛑 Stopping TiUP cluster...")

        # Kill the tiup process group
        if self.tiup_process:
            try:
                os.killpg(os.getpgid(self.tiup_process.pid), signal.SIGTERM)
                self.tiup_process.wait(timeout=10)
            except BaseException:
                try:
                    os.killpg(os.getpgid(self.tiup_process.pid), signal.SIGKILL)
                except BaseException:
                    pass
            self.tiup_process = None

        print("✅ TiUP cluster stopped")

    def create_table(self, table_name="hdfs_10w"):
        """Create a table for HDFS logs with tenant_id encoded in primary key"""
        try:
            with utils.mysql_connection(self.mysql_host, self.mysql_port, database="test") as connection:
                # Drop table if exists
                utils.execute_sql(connection, f"DROP TABLE IF EXISTS {table_name};")
                print(f"Table {table_name} dropped successfully")

                # Create new table
                create_sql = f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        id BIGINT AUTO_INCREMENT,
                        timestamp BIGINT,
                        severity_text VARCHAR(50),
                        body TEXT,
                        tenant_id INT,
                        PRIMARY KEY (tenant_id, id)
                    )AUTO_INCREMENT = 1000;
                """
                utils.execute_sql(connection, create_sql)
                print(f"Table {table_name} created successfully")
        except Exception as e:
            print(f"Error creating table: {e}")
            raise

    def insert_test_data(self, table_name="hdfs_10w"):
        """Insert test data using read_hdfs.py"""
        print("📊 Inserting test data...")

        insert_data.process_hdfs_logs(
            table_name=table_name,
            max_rows=self.max_rows,
            tidb_host=self.mysql_host,
            tidb_port=self.mysql_port,
        )

        print("✅ Test data inserted successfully")

    def create_fulltext_index(self, sql="ALTER TABLE test.hdfs_10w ADD FULLTEXT INDEX ft_index (body) WITH PARSER standard;"):
        """Create fulltext index on the test table"""
        print("🔍 Creating fulltext index...")

        try:
            with utils.mysql_connection(self.mysql_host, self.mysql_port) as connection:
                utils.execute_sql(connection, sql)
                table_name, index_name = utils.parse_information_from_sql(sql)
                result = utils.execute_sql(
                    connection,
                    f"SELECT index_id FROM information_schema.tidb_indexes WHERE table_schema = 'test' and table_name = '{table_name}' and key_name = '{index_name}';"
                )
                index_id = result[0][0] if result else None
                result = utils.execute_sql(
                    connection,
                    f"SELECT TIDB_TABLE_ID FROM information_schema.tables WHERE table_schema = 'test' and table_name = '{table_name}';"
                )
                table_id = result[0][0] if result else None
                print(f"✅ Fulltext index created successfully, index_id: {index_id}, table_id: {table_id}")
                return table_id, index_id
        except Exception as e:
            raise RuntimeError(f"Index creation failed: {e}")

    def verify_index_creation(self, table_id=None, index_id=None):
        """Verify that the index was created successfully"""
        print("🔍 Verifying index creation...")
        config_path = os.path.join(config.PROJECT_DIR, "config", "test-meta.toml")
        endpoint, access_key, secret_key, bucket, prefix = utils.get_s3_config(config_path)
        s3_client = utils.create_s3_client(endpoint, access_key, secret_key)

        is_valid = False
        while not is_valid:
            time.sleep(5)  # Wait before next check
            with utils.mysql_connection(self.mysql_host, self.mysql_port, timeout=60) as connection:
                sql = "SELECT distinct progress FROM tici.tici_shard_meta"
                if index_id is not None and table_id is not None:
                    sql += f" WHERE index_id = {index_id} AND table_id = {table_id};"
                result = utils.execute_sql(connection, sql)
                for row in result:
                    progress = utils.safe_json_parse(row[0])
                    cdc_s3_last_file = progress.get("cdc_s3_last_file")
                    is_valid = utils.validate_cdc_file_sequence(
                        s3_client,
                        bucket,
                        f"{prefix}/cdc/test/hdfs_10w",
                        cdc_s3_last_file,
                    )
                    if is_valid:
                        print(f"✅ Index verified successfully with last file: {cdc_s3_last_file}")
                        break

        with utils.mysql_connection(self.mysql_host, self.mysql_port) as connection:
            result = utils.execute_sql(connection, "SELECT count(*) FROM tici.tici_shard_meta;")
            print(f"Shard meta count: {result[0][0]}")

    def run_qps_benchmark(self):
        """Run the concurrent QPS benchmark"""
        print("⚡ Running QPS benchmark...")

        try:
            # Use direct function call instead of subprocess
            final_results = []

            for word, rows in config.WORD_LIST:
                print(f"\n🚀 Starting concurrent benchmark for word: '{word}', matched rows: {rows}")
                print("-" * 50)

                result = qps.get_peak_qps(
                    host=self.mysql_host,
                    port=self.mysql_port,
                    user="root",
                    database="test",
                    query_template=config.QUERY_TEMPLATE,
                    word=word,
                    matched_rows=rows,
                )
                final_results.append(result)

            # Print results using utility function
            utils.format_qps_results(final_results)
            print("✅ QPS benchmark completed")

        except Exception as e:
            raise RuntimeError(f"QPS benchmark failed: {e}")

    def run_latency_benchmark(self):
        """Run the latency benchmark"""
        print("📈 Running latency benchmark...")

        try:
            # Use direct function call instead of subprocess
            results = []

            for word in config.WORD_LIST:
                print(f"Running benchmark for word: '{word[0]}', matched rows: {word[1]}")
                result = latency.run_query_benchmark(
                    host=self.mysql_host,
                    port=self.mysql_port,
                    user="root",
                    database="test",
                    query_template=config.QUERY_TEMPLATE,
                    word=word,
                    iterations=100,
                )
                if result:
                    results.append(result)

            # Print results using utility function
            utils.format_latency_results(results)
            print("✅ Latency benchmark completed")

        except Exception as e:
            raise RuntimeError(f"Latency benchmark failed: {e}")

    def cleanup(self):
        """Clean up resources"""
        print("🧹 Cleaning up resources...")

        try:
            # Use direct function call instead of subprocess
            clean_up.cleanup_s3_files(os.path.join(config.PROJECT_DIR, "config", "test-meta.toml"))
        except Exception as e:
            print(f"⚠️ Cleanup encountered error: {e}")

    def extract_mysql_info_from_tiup_output(self, timeout=180):
        """
        Extract MySQL host and port from tiup playground output.
        Also waits for cluster to be ready.
        """
        start_time = time.time()
        output_lines = []

        while time.time() - start_time < timeout:
            # Check if process has terminated unexpectedly
            if self.tiup_process.poll() is not None:
                print("❌ TiUP process has exited unexpectedly")
                print("Last output lines:")
                for line in output_lines[-10:]:
                    print(line.strip())
                raise RuntimeError("TiUP process exited unexpectedly")

            # Read output line by line
            line = self.tiup_process.stdout.readline().strip()
            if line:
                output_lines.append(line)
                if "Connect TiDB:" in line:
                    parts = line.split()
                    self.mysql_host = parts[5]
                    self.mysql_port = int(parts[7])
                    print(line)
                if "TiDB Dashboard:" in line:
                    print(line)
                if "Grafana:" in line:
                    print(line)
                    return

            time.sleep(0.1)
        raise TimeoutError(f"Cluster didn't start within {timeout} seconds")

    def run(self):
        """Run a complete test cycle for a given max_size"""
        print(f"\n{'='*60}")
        print(
            f"🎯 Starting test with shard.max_size = {self.shard_size}, tiflash_num = {self.tiflash_count}, worker_num = {self.worker_count}"
        )
        print(f"{'='*60}")

        try:
            # Step 1: Create table
            self.create_table()

            # Step 2: Insert data
            self.insert_test_data()

            # Step 3: Create index
            table_id, index_id = self.create_fulltext_index()

            # Step 4: Verify index
            self.verify_index_creation(table_id, index_id)

            # Step 5: Run QPS benchmark
            self.run_qps_benchmark()

            # Step 6: Run latency benchmark
            self.run_latency_benchmark()

        except Exception:
            raise

        finally:
            # Step 8: Always cleanup
            self.stop_tiup_cluster()
            self.cleanup()
