#!/usr/bin/env python3
"""Measure total size and object count of all R2 buckets.

Reads read-only credentials from the project .env (R2_READ_* keys).
Prints bucket name, object count, and total GB — never prints secrets.

Run: python jobs/r2_bucket_sizes.py
"""
from __future__ import annotations
import os
import sys

# Derived from this file, not a drive letter (R330). A stale path here does not raise — it
# yields an empty env, and the caller then authenticates with no credentials.
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ENV_PATH = os.environ.get("ECONDL_ENV") or os.path.join(ROOT, ".env")


def load_env(path: str) -> dict:
    vals = {}
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, _, v = line.partition("=")
            vals[k.strip()] = v.strip().strip('"').strip("'")
    return vals


def main():
    env = load_env(ENV_PATH)

    key_id = env.get("R2_READ_ACCESS_KEY_ID") or env.get("R2_ACCESS_KEY_ID")
    secret = env.get("R2_READ_SECRET_ACCESS_KEY") or env.get("R2_SECRET_ACCESS_KEY")
    endpoint = env.get("R2_READ_ENDPOINT") or env.get("R2_ENDPOINT")

    missing = [n for n, v in [("access key id", key_id), ("secret", secret), ("endpoint", endpoint)] if not v]
    if missing:
        print(f"MISSING in .env: {', '.join(missing)}")
        print(f"Keys present that start with R2: {[k for k in env if k.startswith('R2')]}")
        sys.exit(1)

    if not endpoint.startswith("http"):
        endpoint = "https://" + endpoint

    import boto3
    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=key_id,
        aws_secret_access_key=secret,
        region_name="auto",
    )

    try:
        buckets = [b["Name"] for b in client.list_buckets().get("Buckets", [])]
    except Exception as e:
        print(f"list_buckets failed ({type(e).__name__}) — falling back to known names")
        buckets = ["hfdatalibrary-data", "econdatalibrary-data"]

    print(f"Buckets visible: {buckets}\n")

    for name in buckets:
        total = 0
        count = 0
        try:
            paginator = client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=name):
                for obj in page.get("Contents", []):
                    total += obj["Size"]
                    count += 1
            print(f"{name}: {count:,} objects, {total/1e9:.2f} GB ({total/2**30:.2f} GiB)")
        except Exception as e:
            print(f"{name}: ERROR {type(e).__name__}: {str(e)[:120]}")


if __name__ == "__main__":
    main()
