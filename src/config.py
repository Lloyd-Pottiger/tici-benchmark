from pathlib import Path


WORD_LIST = [
    ("error", 0),
    ("1073837169", 7),
    ("45", 55),
    ("614cb9d92271", 6048),
    ("36", 38462),
    ("LAST", 104908),
]

QUERY_TEMPLATE = """SELECT id FROM test.hdfs_10w WHERE fts_match_word("xxxx", body) LIMIT 100;"""

# --- Benchmark Settings ---
# How long each concurrency test should run (in seconds).
TEST_DURATION = 30
# The list of concurrent connections to test.
CONCURRENCY_LEVELS = [5, 10, 15, 20, 25, 30, 35, 40, 50]

TEST_SIZES = ["16MB", "32MB", "64MB", "128MB"]
TIUP_VERSION = "v1.16.2-feature.fts"
TIDB_VERSION = "v9.0.0-feature.fts"

PROJECT_DIR = Path(__file__).parent.parent.resolve()
