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


class TICIBenchmarkRunner:
    def __init__(self, worker_count=1, tiflash_count=1, max_rows=1000000):
        self.tiup_process = None
        self.current_test_size = None
        self.worker_count = worker_count
        self.tiflash_count = tiflash_count
        self.mysql_host = "127.0.0.1"
        self.mysql_port = 4000  # Default value, will be updated from tiup output
        self.max_rows = max_rows

    def modify_config(self, max_shard_size):
        """Modify config/test-meta.toml with the specified shard.max_size"""
        config_path = os.path.join(config.PROJECT_DIR, "config/test-meta.toml")
        print(f"📝 Modifying config: shard.max_size = {max_shard_size}")

        # Read the current config
        with open(config_path, 'r') as f:
            lines = f.readlines()

        # Modify the max_size line
        with open(config_path, 'w') as f:
            for line in lines:
                if line.strip().startswith('max_size ='):
                    f.write(f'max_size = "{max_shard_size}"\n')
                else:
                    f.write(line)

        print(f"✅ Config updated: max_size = {max_shard_size}")

    def start_tiup_cluster(self):
        """Start TiDB cluster using tiup playground"""
        print(
            f"🚀 Starting TiUP cluster (workers: {self.worker_count}, tiflash: {self.tiflash_count})")

        # Stop any existing cluster
        self.stop_tiup_cluster()

        cmd = [
            "tiup", f"playground:{config.TIUP_VERSION}",
            f"{config.TIDB_VERSION}",
            "--ticdc", "1",
            "--tici.meta", "1",
            "--tici.worker", str(self.worker_count),
            "--tiflash", str(self.tiflash_count),
            "--tici.config", "./config"
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
            preexec_fn=os.setsid  # Create new process group
        )

        # Wait for cluster to start and extract MySQL connection info
        print("⏳ Waiting for cluster to start...")
        self.extract_mysql_info_from_tiup_output()
        print(
            f"✅ TiUP cluster is ready! MySQL available at {self.mysql_host}:{self.mysql_port}")

    def stop_tiup_cluster(self):
        """Stop the TiUP cluster"""
        print("🛑 Stopping TiUP cluster...")

        # Kill the tiup process group
        if self.tiup_process:
            try:
                os.killpg(os.getpgid(self.tiup_process.pid), signal.SIGTERM)
                self.tiup_process.wait(timeout=10)
            except:
                try:
                    os.killpg(os.getpgid(self.tiup_process.pid),
                              signal.SIGKILL)
                except:
                    pass
            self.tiup_process = None

        print("✅ TiUP cluster stopped")

    def insert_test_data(self):
        """Insert test data using read_hdfs.py"""
        print("📊 Inserting test data...")

        insert_data.process_hdfs_logs(
            table_name="hdfs_10w",
            max_rows=self.max_rows,
            tidb_host=self.mysql_host,
            tidb_port=self.mysql_port
        )

        print("✅ Test data inserted successfully")

    def create_fulltext_index(self):
        """Create fulltext index on the test table"""
        print("🔍 Creating fulltext index...")

        try:
            import mysql.connector
            from mysql.connector import Error

            # Use direct database connection instead of subprocess
            connection = mysql.connector.connect(
                host=self.mysql_host,
                port=self.mysql_port,
                user="root"
            )

            cursor = connection.cursor()
            cursor.execute(
                "ALTER TABLE test.hdfs_10w ADD FULLTEXT INDEX ft_index (body) WITH PARSER standard;")
            connection.commit()

            print("✅ Fulltext index created successfully")

        except Error as e:
            raise RuntimeError(f"Index creation failed: {e}")
        finally:
            if connection and connection.is_connected():
                cursor.close()
                connection.close()

    # FIXME: chekc s3 file and progress
    def verify_index_creation(self):
        """Verify that the index was created successfully"""
        print("🔍 Verifying index creation...")

        time.sleep(60)  # Give some time for index to be created

        try:
            import mysql.connector
            from mysql.connector import Error

            # Use direct database connection instead of subprocess
            connection = mysql.connector.connect(
                host=self.mysql_host,
                port=self.mysql_port,
                user="root"
            )

            cursor = connection.cursor()
            cursor.execute("SELECT count(*) FROM tici.tici_shard_meta;")
            result = cursor.fetchone()

            print("✅ Index verification completed")
            print(f"Shards count: {result[0] if result else 0}")

        except Error as e:
            raise RuntimeError(f"⚠️ Could not verify index: {e}")
        finally:
            if connection and connection.is_connected():
                cursor.close()
                connection.close()

    def run_qps_benchmark(self):
        """Run the concurrent QPS benchmark"""
        print("⚡ Running QPS benchmark...")

        try:
            # Use direct function call instead of subprocess
            final_results = []

            for word, rows in config.WORD_LIST:
                print(
                    f"\n🚀 Starting concurrent benchmark for word: '{word}', matched rows: {rows}")
                print("-" * 50)

                result = qps.get_peak_qps(
                    host=self.mysql_host,
                    port=self.mysql_port,
                    user="root",
                    database="test",
                    query_template=config.QUERY_TEMPLATE,
                    word=word,
                    matched_rows=rows
                )
                final_results.append(result)

            # Print results (similar to what the original script would do)
            from tabulate import tabulate
            table_data = []
            for res in final_results:
                table_data.append([
                    f"{res['matched_rows']:,}",
                    f"{res['best_qps']:.2f}",
                    f"{res['best_avg_latency']:.2f}",
                    res['best_concurrency']
                ])

            headers = ["Matched rows", "QPS",
                       "Average latency (ms)", "Concurrency"]
            print("\n📊 Final QPS Benchmark Results:")
            print(tabulate(table_data, headers=headers, tablefmt="pipe"))
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
                print(
                    f"Running benchmark for word: '{word[0]}', matched rows: {word[1]}")
                result = latency.run_query_benchmark(
                    host=self.mysql_host,
                    port=self.mysql_port,
                    user="root",
                    database="test",
                    query_template=config.QUERY_TEMPLATE,
                    word=word,
                    iterations=100
                )
                if result:
                    results.append(result)

            # Print results (similar to what the original script would do)
            from tabulate import tabulate
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
            print("✅ Latency benchmark completed")

        except Exception as e:
            raise RuntimeError(f"Latency benchmark failed: {e}")

    def cleanup(self):
        """Clean up resources"""
        print("🧹 Cleaning up resources...")

        try:
            # Use direct function call instead of subprocess
            clean_up.cleanup_s3_files(os.path.join(
                config.PROJECT_DIR, 'config', 'test-meta.toml'))
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
                # Look for the MySQL connection string
                if "Connect TiDB:" in line:
                    try:
                        # Extract host and port from line like:
                        # "Connect TiDB:    mysql --comments --host 127.0.0.1 --port 44415 -u root"
                        parts = line.split()
                        host_index = parts.index("--host") + 1
                        port_index = parts.index("--port") + 1

                        self.mysql_host = parts[host_index]
                        self.mysql_port = int(parts[port_index])

                        print(
                            f"📊 Found MySQL connection: {self.mysql_host}:{self.mysql_port}")
                        return
                    except (ValueError, IndexError) as e:
                        print(f"⚠️ Could not parse MySQL connection info: {e}")

            time.sleep(0.1)
        raise TimeoutError(f"Cluster didn't start within {timeout} seconds")

    def run(self, max_size):
        """Run a complete test cycle for a given max_size"""
        self.current_test_size = max_size
        print(f"\n{'='*60}")
        print(f"🎯 Starting test with max_size = {max_size}")
        print(f"{'='*60}")

        try:
            # Step 1: Modify config
            self.modify_config(max_size)

            # Step 2: Start cluster
            self.start_tiup_cluster()

            # Step 3: Insert data
            self.insert_test_data()

            # Step 4: Create index
            self.create_fulltext_index()

            # Step 5: Verify index
            self.verify_index_creation()

            # Step 6: Run QPS benchmark
            self.run_qps_benchmark()

            # Step 7: Run latency benchmark
            self.run_latency_benchmark()

            print(f"✅ Test completed successfully for max_size = {max_size}")

        except Exception as e:
            print(f"❌ Test failed for max_size = {max_size}: {e}")
            raise

        finally:
            # Step 8: Always cleanup
            self.cleanup()
            self.stop_tiup_cluster()
