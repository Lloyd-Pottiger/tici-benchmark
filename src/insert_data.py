import os
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
