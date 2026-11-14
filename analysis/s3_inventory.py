#!/usr/bin/env python3
"""
List S3 objects under data/, aggregate counts by calendar date (from path),
and write CSV summaries for planning analyses.

Skenira sekvencijalno: prvo data/json/* i data/parquet/* podprefiksi,
zatim jedan paginator po stablu (bez paralelizma).

Uses project-root .env (same credentials as utils.s3_upload).
"""
from __future__ import annotations

import argparse
import csv
import os
import re
import sys
from collections import defaultdict
from pathlib import Path
from typing import DefaultDict

# certifi before boto3
try:
    import certifi
    _ca = certifi.where()
    if not os.environ.get("SSL_CERT_FILE"):
        os.environ["SSL_CERT_FILE"] = _ca
    if not os.environ.get("AWS_CA_BUNDLE"):
        os.environ["AWS_CA_BUNDLE"] = _ca
except ImportError:
    pass

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(dotenv_path=PROJECT_ROOT / ".env")

import boto3
from botocore.exceptions import ClientError

DATE_IN_KEY = re.compile(r"/(\d{4}-\d{2}-\d{2})/")


def s3_client_from_env():
    key = os.getenv("AWS_ACCESS_KEY_ID")
    secret = os.getenv("AWS_SECRET_ACCESS_KEY")
    bucket = os.getenv("S3_BUCKET_NAME")
    region = os.getenv("AWS_REGION", "eu-north-1")
    if not (key and secret and bucket):
        raise SystemExit(
            "Nedostaju AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY ili S3_BUCKET_NAME u .env"
        )
    return boto3.client(
        "s3",
        aws_access_key_id=key,
        aws_secret_access_key=secret,
        region_name=region,
    ), bucket


def classify_key(key: str) -> str:
    if "/data/json/" in key or key.startswith("data/json/"):
        return "json"
    if "/data/parquet/" in key or key.startswith("data/parquet/"):
        return "parquet"
    return "other"


def source_bucket(key: str, area: str) -> str:
    if area == "json":
        prefix = "data/json/"
    elif area == "parquet":
        prefix = "data/parquet/"
    else:
        return "_other"
    if not key.startswith(prefix):
        return "_other"
    rest = key[len(prefix) :].strip("/")
    if not rest:
        return "_root"
    return rest.split("/", 1)[0]


def list_child_prefixes(client, bucket: str, parent: str) -> list[str]:
    """Immediate subfolders under parent (trailing slash)."""
    if parent and not parent.endswith("/"):
        parent = parent + "/"
    out: list[str] = []
    token = None
    while True:
        kwargs: dict = {"Bucket": bucket, "Prefix": parent, "Delimiter": "/"}
        if token:
            kwargs["ContinuationToken"] = token
        r = client.list_objects_v2(**kwargs)
        for cp in r.get("CommonPrefixes", []):
            out.append(cp["Prefix"])
        if not r.get("IsTruncated"):
            break
        token = r.get("NextContinuationToken")
    return out


def scan_prefix(
    client,
    bucket: str,
    tree_prefix: str,
    collect_keys: bool,
    by_date: DefaultDict[str, int],
    by_date_area: DefaultDict[tuple[str, str], int],
    by_date_source: DefaultDict[tuple[str, str, str], int],
    nodate_by_source: DefaultDict[tuple[str, str], int],
    all_keys: list[tuple[str, str, str, str]],
) -> int:
    """Paginate all keys under tree_prefix; mutate aggregates. Return object count."""
    n = 0
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=tree_prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            n += 1
            m = DATE_IN_KEY.search(key)
            date = m.group(1) if m else "_nodate"
            area = classify_key(key)
            src = source_bucket(key, area)
            by_date[date] += 1
            by_date_area[(date, area)] += 1
            by_date_source[(date, area, src)] += 1
            if date == "_nodate":
                nodate_by_source[(area, src)] += 1
            if collect_keys:
                all_keys.append((key, date, area, src))
    return n


def main() -> None:
    p = argparse.ArgumentParser(description="S3 inventar: broj fajlova po datumu.")
    p.add_argument(
        "--prefix",
        default="data/",
        help="Korenski S3 prefiks (podrazumevano: data/)",
    )
    p.add_argument(
        "--out-dir",
        type=Path,
        default=PROJECT_ROOT / "analysis" / "output",
        help="Izlazni direktorijum za CSV",
    )
    p.add_argument(
        "--keys-csv",
        action="store_true",
        help="Upisati s3_all_keys.csv (veliki fajl)",
    )
    args = p.parse_args()

    client, bucket = s3_client_from_env()
    root = args.prefix.lstrip("/")
    if root and not root.endswith("/"):
        root = root + "/"

    out_dir = args.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    scan_prefixes: list[str] = []
    for cp in list_child_prefixes(client, bucket, root):
        scan_prefixes.extend(list_child_prefixes(client, bucket, cp))

    if not scan_prefixes:
        scan_prefixes = [root]

    by_date: DefaultDict[str, int] = defaultdict(int)
    by_date_area: DefaultDict[tuple[str, str], int] = defaultdict(int)
    by_date_source: DefaultDict[tuple[str, str, str], int] = defaultdict(int)
    nodate_by_source: DefaultDict[tuple[str, str], int] = defaultdict(int)
    all_keys: list[tuple[str, str, str, str]] = []
    total = 0

    try:
        for i, pref in enumerate(scan_prefixes, start=1):
            print(f"[{i}/{len(scan_prefixes)}] {pref}", file=sys.stderr, flush=True)
            total += scan_prefix(
                client,
                bucket,
                pref,
                args.keys_csv,
                by_date,
                by_date_area,
                by_date_source,
                nodate_by_source,
                all_keys,
            )
    except ClientError as e:
        raise SystemExit(f"S3 greška: {e}") from e

    by_date_path = out_dir / "s3_by_date.csv"
    with by_date_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["date", "object_count"])
        for d in sorted(by_date.keys(), key=lambda x: (x == "_nodate", x)):
            w.writerow([d, by_date[d]])

    by_date_area_path = out_dir / "s3_by_date_area.csv"
    with by_date_area_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["date", "area", "object_count"])
        for (da, ar) in sorted(
            by_date_area.keys(), key=lambda t: (t[0] == "_nodate", t[0], t[1])
        ):
            w.writerow([da, ar, by_date_area[(da, ar)]])

    detail_path = out_dir / "s3_by_date_area_source.csv"
    with detail_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["date", "area", "source", "object_count"])
        for row in sorted(
            by_date_source.keys(),
            key=lambda t: (t[0] == "_nodate", t[0], t[1], t[2]),
        ):
            w.writerow([row[0], row[1], row[2], by_date_source[row]])

    nodate_path = out_dir / "s3_nodate_by_area_source.csv"
    with nodate_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["area", "source", "object_count"])
        for (a, s) in sorted(nodate_by_source.keys(), key=lambda t: (t[0], t[1])):
            w.writerow([a, s, nodate_by_source[(a, s)]])

    if args.keys_csv:
        keys_path = out_dir / "s3_all_keys.csv"
        with keys_path.open("w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["key", "date", "area", "source"])
            w.writerows(all_keys)
        print(f"Keys: {keys_path} ({len(all_keys)} redova)")

    dated = sum(c for d, c in by_date.items() if d != "_nodate")
    print(f"Bucket: {bucket}")
    print(f"Korenski prefiks: {root!r}  stabala: {len(scan_prefixes)}")
    print(
        f"Ukupno objekata: {total} (sa datumom u putanji: {dated}, bez: {by_date.get('_nodate', 0)})"
    )
    print(
        f"Zapisano: {by_date_path.name}, {by_date_area_path.name}, "
        f"{detail_path.name}, {nodate_path.name}"
    )


if __name__ == "__main__":
    main()
