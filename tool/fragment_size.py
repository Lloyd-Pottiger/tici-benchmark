import json
import mysql.connector
from mysql.connector import errorcode


def print_detailed_shard_info(row: tuple):
    """Formats and prints the complete details for a single shard row."""

    # Assign columns from the row tuple for readability
    (table_id, index_id, shard_id, manifest_json) = row

    # --- Print Shard Metadata ---
    print("## Shard Metadata")
    print("-" * 25)
    print(f"**Table ID**: `{table_id}`")
    print(f"**Index ID**: `{index_id}`")
    print(f"**Shard ID**: `{shard_id}`")

    # --- Process and Print Manifest Details ---
    print("\n## Manifest Details")
    print("-" * 25)
    total_count = 0
    total_size = 0
    manifest = json.loads(manifest_json)
    fragments = manifest.get('fragments', [])

    for i, frag in enumerate(fragments):
        num_segs = len(frag.get('f', {}).get('segs', []))
        path = frag.get('f', {}).get('frag_path', 'N/A').split('/')[-1]
        prop = frag.get('f', {}).get('property', {})
        p_value = frag.get('p', 1.0)
        count = prop.get('count', 0)
        total_count += count * p_value
        size = prop.get('size', 0)
        size_mb = size / 1024**2
        total_size += size * p_value
        print(f"  {i+1}. **Path**: `./{path}` | **Size**: {size_mb:.2f} MB | **Rows**: {count:,} | **Segments**: {num_segs} | **P-Value**: {p_value:.2f}")

    print(f"**Total Rows (Count)**: {total_count:,.0f}")
    print(f"**Total Size**: {total_size / 1024**2:,.2f} MB")
    print(f"**Individual Fragments**: {len(fragments)}")


def main():
    """Main function to connect to the DB and process shards."""
    db_config = {
        'user': 'root',
        'host': '127.0.0.1',
        'port': 4000,
        'database': 'tici'
    }

    cnx = None
    try:
        cnx = mysql.connector.connect(**db_config)
        cursor = cnx.cursor()

        # Query for all columns from the table
        query = "SELECT table_id, index_id, shard_id, manifest FROM tici_shard_meta;"
        cursor.execute(query)

        results = cursor.fetchall()
        if not results:
            print("❌ No data found in the tici_shard_meta table.")
            return

        print(f"✅ Found {len(results)} shard(s). Processing...\n")
        for i, row in enumerate(results):
            if i > 0:
                print("\n" + "="*50 + "\n")  # Separator for multiple shards
            print_detailed_shard_info(row)

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Authentication error: Check your username and password.")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print(f"Database '{db_config['database']}' does not exist.")
        else:
            print(f"An error occurred: {err}")
    finally:
        if cnx and cnx.is_connected():
            cursor.close()
            cnx.close()
            print("\nDatabase connection closed.")


if __name__ == "__main__":
    main()
