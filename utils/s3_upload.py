"""
S3 upload utility functions for JSON and Parquet files.
"""
import os
import time

                                                                                                 
try:
    import certifi
    _ca = certifi.where()
    if not os.environ.get("SSL_CERT_FILE"):
        os.environ["SSL_CERT_FILE"] = _ca
    if not os.environ.get("AWS_CA_BUNDLE"):
        os.environ["AWS_CA_BUNDLE"] = _ca
except ImportError:
    pass

import json
import io
import fnmatch
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from dotenv import load_dotenv
from pathlib import Path

                                                              
PROJECT_ROOT = Path(__file__).parent.parent
ENV_FILE = PROJECT_ROOT / ".env"
load_dotenv(dotenv_path=ENV_FILE)

                                             
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "eu-north-1")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
S3_JSON_PATH = os.getenv("S3_JSON_PATH", "data/json")
S3_PARQUET_PATH = os.getenv("S3_PARQUET_PATH", "data/parquet")

                                                   
s3_client = None
if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY and S3_BUCKET_NAME:
    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION
        )
        print(f"S3 client initialized successfully. Bucket: {S3_BUCKET_NAME}")
    except Exception as e:
        print(f"Warning: Could not initialize S3 client: {e}")
        s3_client = None
else:
    missing = []
    if not AWS_ACCESS_KEY_ID:
        missing.append("AWS_ACCESS_KEY_ID")
    if not AWS_SECRET_ACCESS_KEY:
        missing.append("AWS_SECRET_ACCESS_KEY")
    if not S3_BUCKET_NAME:
        missing.append("S3_BUCKET_NAME")
    print(f"Warning: S3 client not initialized. Missing: {', '.join(missing)}. "
          f"Please configure .env file with AWS credentials.")

                                                                   
S3_RETRY_ATTEMPTS = 4
S3_RETRY_DELAY_SEC = 2


def _retry_s3(fn, *args, _attempt=1, **kwargs):
    """Run fn(*args, **kwargs); on connection/SSL error retry up to S3_RETRY_ATTEMPTS with delay."""
    try:
        return fn(*args, **kwargs)
    except Exception as e:
        err_str = str(e).lower()
        if _attempt < S3_RETRY_ATTEMPTS and (
            "certificate" in err_str or "ssl" in err_str or "could not connect" in err_str or "endpoint" in err_str
        ):
            time.sleep(S3_RETRY_DELAY_SEC)
            return _retry_s3(fn, *args, _attempt=_attempt + 1, **kwargs)
        raise


def upload_to_s3(local_file_path: str, s3_key: str, file_type: str = "parquet") -> bool:
    """
    Upload a file to S3.

    Args:
        local_file_path: Path to the local file to upload
        s3_key: S3 key (path) where the file should be stored
        file_type: Type of file ("json" or "parquet") to determine the base path

    Returns:
        True if upload was successful, False otherwise
    """
    if not s3_client:
        return False

    if not os.path.exists(local_file_path):
        print(f"Warning: File does not exist: {local_file_path}")
        return False

                                            
    if file_type.lower() == "json":
        base_path = S3_JSON_PATH.rstrip("/")
    elif file_type.lower() == "parquet":
        base_path = S3_PARQUET_PATH.rstrip("/")
    else:
        base_path = "data"

                           
    full_s3_key = f"{base_path}/{s3_key}".replace("//", "/")

    try:
        s3_client.upload_file(local_file_path, S3_BUCKET_NAME, full_s3_key)
        print(f"✓ Uploaded to S3: s3://{S3_BUCKET_NAME}/{full_s3_key}")
        return True
    except NoCredentialsError:
        print(
            f"Warning: AWS credentials not found. Skipping S3 upload for {local_file_path}")
        return False
    except ClientError as e:
        print(f"Error uploading {local_file_path} to S3: {e}")
        return False
    except Exception as e:
        print(f"Unexpected error uploading {local_file_path} to S3: {e}")
        return False


def upload_json_to_s3(local_file_path: str, relative_path: str = None) -> bool:
    """
    Upload a JSON file to S3 JSON path.

    Args:
        local_file_path: Path to the local JSON file
        relative_path: Optional relative path to use as S3 key. If not provided,
                      uses the filename from local_file_path

    Returns:
        True if upload was successful, False otherwise
    """
    if relative_path is None:
        relative_path = os.path.basename(local_file_path)

    return upload_to_s3(local_file_path, relative_path, file_type="json")


def upload_parquet_to_s3(local_file_path: str, relative_path: str = None) -> bool:
    """
    Upload a Parquet file to S3 Parquet path.

    Args:
        local_file_path: Path to the local Parquet file
        relative_path: Optional relative path to use as S3 key. If not provided,
                      uses the filename from local_file_path

    Returns:
        True if upload was successful, False otherwise
    """
    if relative_path is None:
        relative_path = os.path.basename(local_file_path)

    return upload_to_s3(local_file_path, relative_path, file_type="parquet")


def save_json_to_s3(data: dict | list, s3_key: str) -> bool:
    """
    Save JSON data directly to S3 without creating a local file.

    Args:
        data: JSON-serializable data (dict or list)
        s3_key: S3 key (path) where the file should be stored (relative to S3_JSON_PATH)

    Returns:
        True if upload was successful, False otherwise
    """
    if not s3_client:
        print(f"Warning: S3 client not initialized. Skipping upload for {s3_key}. "
              f"Check AWS credentials in .env file.")
        return False

    base_path = S3_JSON_PATH.rstrip("/")
    full_s3_key = f"{base_path}/{s3_key}".replace("//", "/")

    try:
        json_bytes = json.dumps(data, ensure_ascii=False,
                                indent=2).encode('utf-8')
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=full_s3_key,
            Body=json_bytes,
            ContentType='application/json'
        )
        print(f"✓ Saved JSON to S3: s3://{S3_BUCKET_NAME}/{full_s3_key}")
        return True
    except NoCredentialsError:
        print(
            f"Warning: AWS credentials not found. Skipping S3 upload for {s3_key}")
        return False
    except ClientError as e:
        print(f"Error saving JSON to S3: {e}")
        return False
    except Exception as e:
        print(f"Unexpected error saving JSON to S3: {e}")
        return False


def save_parquet_to_s3(df, s3_key: str) -> bool:
    """
    Save DataFrame directly to S3 as Parquet without creating a local file.

    Args:
        df: pandas DataFrame to save
        s3_key: S3 key (path) where the file should be stored (relative to S3_PARQUET_PATH)

    Returns:
        True if upload was successful, False otherwise
    """
    if not s3_client:
        print(f"Warning: S3 client not initialized. Skipping upload for {s3_key}. "
              f"Check AWS credentials in .env file.")
        return False

    base_path = S3_PARQUET_PATH.rstrip("/")
    full_s3_key = f"{base_path}/{s3_key}".replace("//", "/")

    try:
                                 
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False, engine='pyarrow')
        buffer.seek(0)

                      
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=full_s3_key,
            Body=buffer.getvalue(),
            ContentType='application/octet-stream'
        )
        print(f"✓ Saved Parquet to S3: s3://{S3_BUCKET_NAME}/{full_s3_key}")
        return True
    except NoCredentialsError:
        print(
            f"Warning: AWS credentials not found. Skipping S3 upload for {s3_key}")
        return False
    except ClientError as e:
        print(f"Error saving Parquet to S3: {e}")
        return False
    except Exception as e:
        print(f"Unexpected error saving Parquet to S3: {e}")
        return False


def list_parquet_files_from_s3(source: str, pattern: str = "*.parquet", exclude_normalized: bool = True, with_metadata: bool = False, date_prefix: str | None = None) -> list:
    """
    List Parquet files from S3 for a specific source.

    Args:
        source: Data source name (e.g., "avinor", "entur", "oslobysykkel", "vegvesen")
        pattern: Optional pattern to filter files (e.g., "*vehicle-positions*.parquet")
        exclude_normalized: If True, exclude files from normalized/ directory
        with_metadata: If True, return list of dicts with 'key' and 'last_modified', 
                      otherwise return list of keys only
        date_prefix: If set (e.g. "2026-01-29"), list only under data/parquet/{source}/{date_prefix}/

    Returns:
        List of S3 keys (full paths) sorted by LastModified time (oldest first),
        or list of dicts with 'key' and 'last_modified' if with_metadata=True
    """
    if not s3_client:
        print("Warning: S3 client not initialized. Cannot list files from S3.")
        return []

    base_path = S3_PARQUET_PATH.rstrip("/")
    prefix = f"{base_path}/{source}/"
    if date_prefix:
        prefix = f"{base_path}/{source}/{date_prefix}/"

    def _list_pages():
        files = []
        paginator = s3_client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=S3_BUCKET_NAME, Prefix=prefix):
            if 'Contents' in page:
                for obj in page['Contents']:
                    key = obj['Key']
                    if exclude_normalized and 'normalized' in key:
                        continue
                    if key.endswith('.parquet'):
                        filename = os.path.basename(key)
                        if fnmatch.fnmatch(filename, pattern):
                            files.append({'key': key, 'last_modified': obj['LastModified']})
        files.sort(key=lambda x: x['last_modified'])
        return files

    try:
        files = _retry_s3(_list_pages)
        if with_metadata:
            return files
        return [f['key'] for f in files]
    except ClientError as e:
        print(f"Error listing files from S3: {e}")
        return []
    except Exception as e:
        print(f"Unexpected error listing files from S3: {e}")
        return []


def list_normalized_parquet_files_from_s3() -> list:
    """
    List all normalized parquet files from S3 (under normalized/ prefix).

    Example S3 path (key):
        s3://<bucket>/data/parquet/normalized/2025-12-26/avinor_normalized_2026-01-25_002213.parquet
    Prefix used: {S3_PARQUET_PATH}/normalized/

    Returns:
        List of full S3 keys sorted by LastModified time (oldest first)
    """
    if not s3_client:
        print("Warning: S3 client not initialized. Cannot list files from S3.")
        return []

    base_path = S3_PARQUET_PATH.rstrip("/")
    prefix = f"{base_path}/normalized/"

    def _list_pages():
        files = []
        paginator = s3_client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=S3_BUCKET_NAME, Prefix=prefix):
            if 'Contents' in page:
                for obj in page['Contents']:
                    key = obj['Key']
                    if key.endswith('.parquet') and '_normalized_' in key:
                        files.append({'key': key, 'last_modified': obj['LastModified']})
        files.sort(key=lambda x: x['last_modified'])
        return [f['key'] for f in files]

    try:
        return _retry_s3(_list_pages)
    except ClientError as e:
        print(f"Error listing normalized files from S3: {e}")
        return []
    except Exception as e:
        print(f"Unexpected error listing normalized files from S3: {e}")
        return []


def read_parquet_from_s3(s3_key: str):
    """
    Read a Parquet file from S3 directly into a Polars DataFrame.
    Retries on connection/SSL errors up to S3_RETRY_ATTEMPTS.
    """
    if not s3_client:
        print("Warning: S3 client not initialized. Cannot read from S3.")
        return None

    def _get_bytes():
        response = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=s3_key)
        return response['Body'].read()

    try:
        parquet_bytes = _retry_s3(_get_bytes)
        import polars as pl
        return pl.read_parquet(io.BytesIO(parquet_bytes))
    except ClientError as e:
        print(f"Error reading {s3_key} from S3: {e}")
        return None
    except Exception as e:
        print(f"Unexpected error reading {s3_key} from S3: {e}")
        return None


def read_json_from_s3(s3_key: str) -> dict | list | None:
    """
    Read a JSON file from S3.

    Args:
        s3_key: Full S3 key (path) to the JSON file

    Returns:
        Parsed JSON data (dict or list), or None if error occurred
    """
    if not s3_client:
        print("Warning: S3 client not initialized. Cannot read from S3.")
        return None

    try:
                            
        response = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=s3_key)
        json_bytes = response['Body'].read()
        json_data = json.loads(json_bytes.decode('utf-8'))
        return json_data
    
    except ClientError as e:
                                                
        if e.response['Error']['Code'] == 'NoSuchKey':
            return {}
        print(f"Error reading {s3_key} from S3: {e}")
        return None
    except Exception as e:
        print(f"Unexpected error reading {s3_key} from S3: {e}")
        return None


def get_normalization_tracking_key() -> str:
    """Get S3 key for normalization tracking file."""
    base_path = S3_JSON_PATH.rstrip("/")
    return f"{base_path}/normalization_tracking.json"


def load_normalized_files(source: str) -> set:
    """
    Load set of already normalized file keys for a specific source.

    Args:
        source: Data source name (e.g., "avinor", "entur", "oslobysykkel", "vegvesen")

    Returns:
        Set of S3 keys that have already been normalized
    """
    tracking_key = get_normalization_tracking_key()
    tracking_data = read_json_from_s3(tracking_key)
    
    if tracking_data is None:
        return set()
    
    if not isinstance(tracking_data, dict):
        return set()
    
                                                    
    normalized_files = tracking_data.get(source, [])
    return set(normalized_files)


def mark_files_as_normalized(source: str, file_keys: list[str]) -> bool:
    """
    Mark files as normalized in the tracking file.

    Args:
        source: Data source name
        file_keys: List of S3 keys that were normalized

    Returns:
        True if update was successful, False otherwise
    """
    tracking_key = get_normalization_tracking_key()
    
                                 
    tracking_data = read_json_from_s3(tracking_key)
    if tracking_data is None:
        tracking_data = {}
    
    if not isinstance(tracking_data, dict):
        tracking_data = {}
    
                                                   
    existing_files = set(tracking_data.get(source, []))
    
                   
    existing_files.update(file_keys)
    
                          
    tracking_data[source] = sorted(list(existing_files))
    
                     
    return save_json_to_s3(tracking_data, "normalization_tracking.json")
