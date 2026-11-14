"""Read-only DuckDB access to the Parquet files on S3."""
import threading
import time

import boto3
import duckdb

from . import config

_lock = threading.Lock()
_con = None
_created = 0.0
_REFRESH_SECONDS = 1800  # re-create the S3 secret before temporary Lambda credentials expire


def _connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(":memory:", config={
        "extension_directory": config.DUCKDB_EXT_DIR,
        "home_directory": "/tmp",
    })
    con.execute("INSTALL httpfs")  # no-op when the extension is baked into the image
    con.execute("LOAD httpfs")
    creds = boto3.Session().get_credentials()
    if creds is None:
        raise RuntimeError("No AWS credentials available")
    frozen = creds.get_frozen_credentials()
    parts = [
        "TYPE S3",
        f"KEY_ID '{frozen.access_key}'",
        f"SECRET '{frozen.secret_key}'",
        f"REGION '{config.REGION}'",
    ]
    if frozen.token:
        parts.append(f"SESSION_TOKEN '{frozen.token}'")
    con.execute(f"CREATE OR REPLACE SECRET s3_read ({', '.join(parts)})")
    return con


def _connection() -> duckdb.DuckDBPyConnection:
    global _con, _created
    with _lock:
        if _con is None or time.monotonic() - _created > _REFRESH_SECONDS:
            _con = _connect()
            _created = time.monotonic()
        return _con


def query(sql: str, params: list | None = None) -> list[dict]:
    """Run a query on its own cursor and return rows as dicts."""
    cur = _connection().cursor()
    try:
        cur.execute("SET TimeZone = 'UTC'")  # per connection: buckets are UTC whatever the host zone
        res = cur.execute(sql, params or [])
        cols = [d[0] for d in res.description]
        return [dict(zip(cols, row)) for row in res.fetchall()]
    finally:
        cur.close()
