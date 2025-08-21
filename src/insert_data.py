import os
import argparse
from . import utils


def read_hdfs_logs(file_path, max_rows=None, batch_size=50000):
    """Read HDFS logs from a JSON file with optional row limit
    Returns a generator that yields batches of logs to avoid loading all into memory"""
    total_read = 0

    try:
        with open(file_path, 'r') as file:
            batch = []

            for i, line in enumerate(file):
                if max_rows is not None and i >= max_rows:
                    break

                try:
                    log_entry = utils.safe_json_parse(line.strip())
                    batch.append(log_entry)
                    total_read += 1

                    # Yield batch when it reaches the batch size
                    if len(batch) >= batch_size:
                        yield batch
                        batch = []

                except Exception as e:
                    print(f"Error parsing JSON at line {i+1}: {e}")

            # Yield the remaining batch if any
            if batch:
                yield batch

    except Exception as e:
        print(f"Error reading file {file_path}: {e}")

    print(f"Read {total_read} total log entries from {file_path}")


def create_hdfs_log_table(connection, table_name):
    """Create a table for HDFS logs with tenant_id encoded in primary key"""
    try:
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


def insert_hdfs_logs_batch(connection, table_name, logs):
    """Insert HDFS logs into database with tenant_id encoded in primary key"""
    try:
        total_logs = len(logs)

        sql = f"INSERT INTO {table_name} (timestamp, severity_text, body, tenant_id) VALUES"
        for _, log in enumerate(logs):
            # Extract the required fields
            timestamp = log.get('timestamp')
            severity_text = log.get('severity_text')
            body = log.get('body')
            tenant_id = log.get('tenant_id')

            sql += f" {(timestamp, severity_text, body, tenant_id)},"

        utils.execute_sql(connection, sql[:-1])  # Remove trailing comma
        connection.commit()
        print(f"{total_logs} logs from batch inserted successfully into {table_name}")
    except Exception as e:
        print(f"Error inserting logs: {e}")
        raise


def write_logs_to_csv(logs, outfile):
    """Write logs to a CSV file"""
    import csv
    try:
        fieldnames = ['timestamp', 'severity_text', 'body', 'tenant_id']
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        for log in logs:
            writer.writerow({
                'timestamp': log.get('timestamp'),
                'severity_text': log.get('severity_text'),
                'body': log.get('body'),
                'tenant_id': log.get('tenant_id')
            })
        outfile.flush()
        print(f"Logs written to file successfully")
    except Exception as e:
        print(f"Error writing logs to CSV: {e}")


def process_hdfs_logs(table_name, max_rows=None, batch_size=50000, tidb_host="localhost", tidb_port=4000, out=None):
    """Process HDFS logs and insert them into the database in batches"""
    outfile = None
    if out:
        if not out.endswith('.csv'):
            raise ValueError(
                "Output file must end with .csv if --out is specified")
        else:
            outfile = open(out, 'w', newline='')

    try:
        total_inserted = 0
        assert_dir = os.getenv('ASSET_DIR', "").rstrip('/')
        infilename = '{}/hdfs-logs-multitenants.json'.format(assert_dir)
        print(f"Processing logs from '{infilename}' in batches of {batch_size}")

        with utils.mysql_connection(tidb_host, tidb_port, database='test') as connection:
            # Create table if it doesn't exist
            create_hdfs_log_table(connection, table_name)
            # Process logs in batches using the generator
            for _, batch in enumerate(read_hdfs_logs(infilename, max_rows, batch_size)):
                if out:
                    write_logs_to_csv(batch, outfile)
                else:
                    insert_hdfs_logs_batch(connection, table_name, batch)
                total_inserted += len(batch)
                print(f"Total logs inserted so far: {total_inserted}")
    finally:
        if outfile:
            outfile.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Process HDFS logs and insert into database')
    parser.add_argument('table_name', type=str,
                        help='Name of the database table')
    parser.add_argument('--max_rows', type=int, default=None,
                        help='Maximum number of rows to process (optional)')
    parser.add_argument('--batch_size', type=int, default=50000,
                        help='Number of rows to process in each batch (default: 50000)')
    parser.add_argument('--tidb_host', type=str, default='localhost',
                        help='TiDB address to connect to (default: localhost)')
    parser.add_argument('--tidb_port', type=int, default=4000,
                        help='TiDB port to connect to (default: 4000)')
    parser.add_argument('--out', type=str, default=None,
                        help='Output file for CSV mode')

    args = parser.parse_args()

    print(f"\nProcessing HDFS logs into database table '{args.table_name}':")
    process_hdfs_logs(args.table_name, args.max_rows, args.batch_size, args.tidb_host, args.tidb_port, args.out)
    print("\nData processing complete.")
