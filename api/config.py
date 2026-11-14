"""API settings, read from the environment (same variable names as the collectors)."""
import os
from pathlib import Path

try:  # local development only; Lambda gets its configuration from the environment
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent.parent / ".env")
except ImportError:
    pass

BUCKET = os.getenv("S3_BUCKET_NAME", "")
PARQUET_PATH = os.getenv("S3_PARQUET_PATH", "data/parquet").strip("/")
RESULTS_PREFIX = os.getenv("RESULTS_PREFIX", "results/").strip("/") + "/"
REGION = os.getenv("AWS_REGION", "eu-north-1")
API_KEY = os.getenv("API_KEY", "")  # empty = no key required (local development)
DUCKDB_EXT_DIR = os.getenv("DUCKDB_EXTENSION_DIR", "/tmp/duckdb_extensions")

# Longest time range a single request may cover; keeps queries cheap and responses small.
MAX_RANGE_DAYS = 31
