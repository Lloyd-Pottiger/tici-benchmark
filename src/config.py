from pathlib import Path

# --- Benchmark Settings ---
# How long each concurrency test should run (in seconds).
TEST_DURATION = 30
# The list of concurrent connections to test.
CONCURRENCY_LEVELS = [5, 10, 15, 20, 25, 30, 35, 40, 50]
# The list of shard sizes to test.
TEST_SIZES = ["32MB", "64MB", "128MB", "256MB"]
# The template for the SQL query to run.
QUERY_TEMPLATE = """SELECT count(id) FROM test.hdfs_10w WHERE fts_match_word("xxxx", body);"""
# The list of words to match in the FTS query.
WORD_LIST = [
    ("error", 0),
    ("1073837169", 7),
    ("45", 55),
    ("614cb9d92271", 6048),
    ("36", 38462),
    ("LAST", 104908),
]

# --- TiDB Cluster Settings ---
TIUP_VERSION = "v1.16.2-feature.fts"
TIDB_VERSION = "v9.0.0-feature.fts"

PROJECT_DIR = Path(__file__).parent.parent.resolve()
