#!/usr/bin/env python3

import os
import sys
import json
from datetime import datetime, timedelta

import duckdb
import pandas as pd
import boto3
from botocore.config import Config as BotoConfig
from loguru import logger
from dotenv import load_dotenv
from analysis import analyze_schema_changes


def list_data_hours(s3_client, bucket, prefix, start, end, data_filename):
    """
    List every object under `prefix` ending in data_filename between start/end.
    Returns a sorted list of (datetime, key).
    """
    logger.info(f"Listing {data_filename} under s3://{bucket}/{prefix} from {start} to {end}")
    paginator = s3_client.get_paginator("list_objects_v2")
    current = start.replace(day=1)
    keys = []

    while current <= end:
        month_prefix = f"{prefix}/{current.year:04d}/{current.month:02d}"
        full_dir = f"s3://{bucket}/{month_prefix}/"
        logger.info(f"  → Paginating {full_dir}")
        for page in paginator.paginate(Bucket=bucket, Prefix=month_prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                logger.debug(f"    • Found key: s3://{bucket}/{key}")
                if not key.endswith(data_filename):
                    continue

                parts = key.split("/")
                yyyy, mm, dd, hh = parts[-5:-1]
                dt_obj = datetime(int(yyyy), int(mm), int(dd), int(hh))
                if start <= dt_obj <= end:
                    keys.append((dt_obj, key))

        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)
    return sorted(keys, key=lambda x: x[0])


def main():
    load_dotenv()

    BUCKET = os.getenv("bucket")
    S3_ACCESS_KEY = os.getenv("s3_access_key_id")
    S3_SECRET_KEY = os.getenv("s3_secret_access_key")
    S3_ENDPOINT = os.getenv("s3_endpoint")
    S3_URL_STYLE = os.getenv("s3_url_style", "path")
    DATA_FILE = os.getenv("data_filename", "data_0.parquet")
    sources_raw = os.getenv("SOURCES_JSON")

    if not all([BUCKET, S3_ACCESS_KEY, S3_SECRET_KEY, S3_ENDPOINT, sources_raw]):
        logger.error(
            "Make sure bucket, s3_access_key_id, s3_secret_access_key, s3_endpoint and SOURCES_JSON are set in .env"
        )
        sys.exit(1)

    try:
        SOURCES = json.loads(sources_raw)
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse SOURCES_JSON: {e}")
        sys.exit(1)

    START = datetime(2025, 3, 21, 0)
    END = datetime(2025, 7, 2, 23)
    # END = datetime(2025, 3, 22, 0)
    OUTPUT_CSV = "schema_evolution_audit.csv"
    # OUTPUT_CSV = "schema_evolution_audit_test.csv"

    logger.remove()
    logger.add(
        sys.stderr,
        level="INFO",
        format=(
            "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
            "<level>{level: <8}</level> | "
            "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
            "{message}"
        ),
    )

    logger.info("Configuring DuckDB for S3")
    con = duckdb.connect()
    # DuckDB auto-prepends https:// - including this to make the .env flexible
    endpoint_host = S3_ENDPOINT.replace("https://", "").replace("http://", "")
    con.execute(f"SET s3_access_key_id='{S3_ACCESS_KEY}'")
    con.execute(f"SET s3_secret_access_key='{S3_SECRET_KEY}'")
    con.execute(f"SET s3_endpoint='{endpoint_host}'")
    con.execute(f"SET s3_url_style='{S3_URL_STYLE}'")

    logger.info("Initializing boto3 S3 client for Cloudflare R2")
    s3 = boto3.client(
        "s3",
        endpoint_url=f"https://{endpoint_host}",
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        config=BotoConfig(signature_version="s3v4"),
    )

    results = []
    logger.info(f"Starting schema audit for {len(SOURCES)} sources between {START} and {END}")

    for src, prefix in SOURCES.items():
        logger.info(f"Source '{src}': listing {DATA_FILE}")
        hours = list_data_hours(s3, BUCKET, prefix, START, END, DATA_FILE)
        logger.info(f"  → Found {len(hours)} files for '{src}'")

        for dt_obj, key in hours:
            dt_str = dt_obj.strftime("%Y-%m-%d %H:00")
            path = f"s3://{BUCKET}/{key}"
            try:
                logger.info(f"  → DESCRIBE {path}")
                schema_df = con.execute(f"DESCRIBE SELECT * FROM '{path}'").fetchdf()
                cols = [f"{r['column_name']} {r['column_type']}" for _, r in schema_df.iterrows()]
                results.append({"source": src, "datetime": dt_str, "columns": cols})
            except Exception as e:
                logger.warning(f"{src} @ {dt_str} DESCRIBE failed: {e}")
                results.append(
                    {"source": src, "datetime": dt_str, "columns": None, "error": str(e)}
                )

        logger.info(f"Completed '{src}'")

    if not results:
        logger.error("No files found in any source! Check your bucket/prefixes/date range.")
        sys.exit(1)

    df = pd.DataFrame(results)

    df["col_hash"] = df["columns"].apply(lambda c: hash(tuple(c)) if c else None)
    df = df.sort_values(["source", "datetime"]).reset_index(drop=True)
    df["prev_hash"] = df.groupby("source")["col_hash"].shift(1)
    df["is_change"] = (df["col_hash"] != df["prev_hash"]) & df["prev_hash"].notnull()
    schema_changes = df[df["is_change"]]

    logger.info(f"Detected {len(schema_changes)} true schema-change events:")
    if schema_changes.empty:
        logger.info("  → No schema changes detected within any source.")
    else:
        print(schema_changes[["source", "datetime", "columns"]].to_string(index=False))

        changes_df = analyze_schema_changes(df)
        print("\nDetailed Column Diffs:")
        print(changes_df.to_string(index=False))

    df.to_csv(OUTPUT_CSV, index=False)
    logger.success(f"Wrote full audit to '{OUTPUT_CSV}'")


if __name__ == "__main__":
    main()
