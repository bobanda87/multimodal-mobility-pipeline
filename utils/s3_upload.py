"""
S3 upload utility functions for JSON and Parquet files.
"""
import os
import json
import io
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from dotenv import load_dotenv
from pathlib import Path

# Load .env file from project root (parent of utils directory)
PROJECT_ROOT = Path(__file__).parent.parent
ENV_FILE = PROJECT_ROOT / ".env"
load_dotenv(dotenv_path=ENV_FILE)

# S3 Configuration from environment variables
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "eu-north-1")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
S3_JSON_PATH = os.getenv("S3_JSON_PATH", "data/json")
S3_PARQUET_PATH = os.getenv("S3_PARQUET_PATH", "data/parquet")

# Initialize S3 client if credentials are available
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

    # Determine base path based on file type
    if file_type.lower() == "json":
        base_path = S3_JSON_PATH.rstrip("/")
    elif file_type.lower() == "parquet":
        base_path = S3_PARQUET_PATH.rstrip("/")
    else:
        base_path = "data"

    # Construct full S3 key
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
        # Create in-memory buffer
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False, engine='pyarrow')
        buffer.seek(0)

        # Upload to S3
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
