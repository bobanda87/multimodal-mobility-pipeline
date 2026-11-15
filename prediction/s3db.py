"""DuckDB connection with the S3 credentials of the local AWS setup (.env or profile), memory-limited for a laptop."""
import os

import boto3
import duckdb
from dotenv import load_dotenv

load_dotenv()
BUCKET = os.getenv("S3_BUCKET_NAME", "phdprojectdata")
REGION = os.getenv("AWS_REGION", "eu-north-1")
ROOT = f"s3://{BUCKET}"


def connect(memory_limit: str = "9GB", threads: int = 6, temp_dir: str = "/tmp/duck_spill") -> duckdb.DuckDBPyConnection:
    os.makedirs(temp_dir, exist_ok=True)
    con = duckdb.connect(":memory:", config={"memory_limit": memory_limit, "threads": threads,
                                              "temp_directory": temp_dir, "max_temp_directory_size": "6GB"})
    con.execute("INSTALL httpfs")
    con.execute("LOAD httpfs")
    frozen = boto3.Session().get_credentials().get_frozen_credentials()
    parts = ["TYPE S3", f"KEY_ID '{frozen.access_key}'", f"SECRET '{frozen.secret_key}'", f"REGION '{REGION}'"]
    if frozen.token:
        parts.append(f"SESSION_TOKEN '{frozen.token}'")
    con.execute(f"CREATE OR REPLACE SECRET s3 ({', '.join(parts)})")
    return con
