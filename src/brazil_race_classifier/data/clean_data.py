#!/usr/bin/env python3
import argparse, io, os, re, requests, yaml, zipfile
import numpy as np
import pandas as pd
from google.cloud import storage
from typing import List, Optional
from unidecode import unidecode
from tqdm import tqdm


# Normalize bucket name
def normalize_bucket_name(bucket: str) -> str:
    """
    Accept gs://bucket or bucket and return plain bucket name.
    """
    return bucket.replace("gs://", "").strip("/")


# Read candidate data
def read_blob_csv_as_df(client: storage.Client, bucket_name: str, blob_name: str) -> pd.DataFrame:
    """Download a CSV blob as bytes and read into pandas with dtype=str."""
    bucket = client.bucket(bucket_name)
    blob   = bucket.blob(blob_name)
    data   = blob.download_as_bytes()
    return pd.read_csv(io.BytesIO(data), encoding="latin-1", dtype=str, low_memory=False)


# Apply a series of transformations from YAML schema to a pandas Series
def _apply_op(series: pd.Series, op: dict) -> pd.Series:
    """
    Apply a single transform operation from YAML to a pandas Series.
    Supported ops: lower, upper, strip, unidecode, cast(to=string|int)
    """
    name    = (op.get("op") or "").lower()
    skip_na = bool(op.get("skip_na", False))

    # choose NA-safe wrapper
    def _maybe(fn):
        if skip_na:
            return series.map(lambda x: fn(x) if pd.notna(x) else x)
        # pandas .str funcs already NA-safe; use them when possible
        return series

    if name == "lower":
        return _maybe(lambda x: str(x).lower()).str.lower()
    if name == "upper":
        return _maybe(lambda x: str(x).upper()).str.upper()
    if name == "strip":
        return _maybe(lambda x: str(x).strip()).str.strip()
    if name == "unidecode":
        return series.map(lambda x: unidecode(x) if pd.notna(x) else x)

    if name == "cast":
        to = (op.get("to") or "").lower()
        if to in ("string", "str"):
            # keep logical strings; preserve NA
            return series.astype("string")
        if to in ("int", "int64", "integer"):
            # robust int casting with NA support
            return pd.to_numeric(series, errors="coerce").astype("Int64")
        if to in ("float", "float64"):
            return pd.to_numeric(series, errors="coerce").astype("Float64")
        # default: return unchanged if unknown cast
        return series

    # unknown op → no-op
    return series


# Apply a series of transformations to a pandas Series
def _apply_transforms(series: pd.Series, transforms: list | None) -> pd.Series:
    if not transforms:
        return series
    out = series
    for op in transforms:
        out = _apply_op(out, op)
    return out


# Enforce schema type
def _enforce_dtype(series: pd.Series, dtype: str | None) -> pd.Series:
    if not dtype:
        return series
    d = dtype.lower()
    try:
        if d in ("string", "str"):
            return series.astype("string")
        if d in ("int", "int64", "integer"):
            return pd.to_numeric(series, errors="coerce").astype("Int64")
        if d in ("float", "float64"):
            return pd.to_numeric(series, errors="coerce").astype("Float64")
        if d in ("bool", "boolean"):
            # simple heuristic: 'true'/'false' (case-insensitive)
            return series.map(lambda x: str(x).strip().lower() if pd.notna(x) else x)\
                         .map({"true": True, "false": False, "1": True, "0": False, "yes": True, "no": False})\
                         .astype("boolean")
    except Exception:
        # if casting fails, fall back to original
        return series
    return series


# Apply cleaning schema to a pandas DataFrame
def clean_with_schema(df: pd.DataFrame, schema: dict) -> pd.DataFrame:
    """
    - Select & rename columns according to schema['columns'] mapping
    - Apply per-column transforms in order
    - Enforce target dtype
    """
    col_spec: dict = schema.get("columns", {})
    if not col_spec:
        raise ValueError("Schema has no 'columns' section.")

    # Ensure all source columns exist (create empty if missing)
    missing = [src for src in col_spec.keys() if src not in df.columns]
    for src in missing:
        df[src] = pd.Series([pd.NA] * len(df), dtype="string")

    # Order/limit to schema keys
    src_cols_ordered = list(col_spec.keys())
    df = df[src_cols_ordered].copy()

    # Per-column transforms & dtypes, then rename to target names
    cleaned = {}
    for src, spec in col_spec.items():
        tgt = spec.get("target", src)
        dtype = spec.get("dtype")
        transforms = spec.get("transforms", [])

        s = df[src]

        # prefer operating as "string" for text ops, then cast at the end
        if s.dtype.name != "string":
            s = s.astype("string")

        # apply transforms in order
        s = _apply_transforms(s, transforms)

        # enforce dtype at the end
        s = _enforce_dtype(s, dtype)

        cleaned[tgt] = s

    # Build final frame from cleaned columns in target order
    out_cols = [spec.get("target", src) for src, spec in col_spec.items()]
    out = pd.DataFrame({k: cleaned[k] for k in out_cols})

    # Return cleaned DataFrame
    return out


# Read page prefixes
def list_page_prefixes(client: storage.Client, bucket_name: str, project: str | None=None) -> list[str]:
    """
    List all prefixes in the bucket, optionally filtering by project.
    """
    it       = client.list_blobs(bucket_name, delimiter="/")
    prefixes = set()
    for page in it.pages:
        prefixes.update(page.prefixes)
    return sorted(prefixes)


# Read TSE candidate data from bucket path
def read_year_csv(client: storage.Client, bucket_name: str, page_prefix: str, project: str | None = None) -> pd.DataFrame:
    """
    Read the CSV for a given year prefix (e.g. '2024/') into a DataFrame.
    Tries the canonical name 'candidates_<year>.csv', falls back to the first *.csv under the prefix.
    """
    # Link the bucket
    bucket = client.bucket(bucket_name)
    year   = page_prefix.strip("/")

    # Try the canonical name
    candidate_name = f"{page_prefix}candidates_{year}.csv"
    blob = bucket.blob(candidate_name)
    if not blob.exists(client=client):
        # Fallback: first CSV under the prefix
        for b in client.list_blobs(bucket_name, prefix=page_prefix):
            if b.name.lower().endswith(".csv"):
                blob = b
                break
        else:
            raise FileNotFoundError(f"No CSV found under gs://{bucket_name}/{page_prefix}")

    # Get data and return pandas DataFrame
    data = blob.download_as_bytes()
    return pd.read_csv(io.BytesIO(data), encoding="latin-1",sep=",", dtype=str, low_memory=False)


# Upload candidates data to GCS bucket
def upload_csv_to_gcs(client: storage.Client, bucket_name: str, dest_path: str, encoding: str, df: pd.DataFrame) -> None:
    """
    Uploads a local CSV file to a specified GCS bucket and path.
    """
    # Initiate GCS client and get the bucket
    bucket = client.bucket(bucket_name)
    blob   = bucket.blob(dest_path)

    # Write DataFrame to bytes and upload to GCS
    csv_bytes = df.to_csv(index=False, encoding=encoding)
    blob.upload_from_string(csv_bytes, content_type="text/csv")


# Main function to clean data and upload TSE candidate data to GCS
def main():
    """
    Main function to download, clean and upload TSE candidate data to GCS.
    """
    # Parse command line arguments
    ap = argparse.ArgumentParser(description="Download TSE candidate ZIPs and upload raw CSVs to GCS.")
    ap.add_argument("--schema", default="configs/cleaning_schema.yaml", help="Path to YAML with cleaning schema")
    ap.add_argument("--project", help="GCP project ID (default: read from configs/project.yaml)")
    ap.add_argument("--raw-bucket", help="GCS raw candidates bucket (default: read from project.yaml)")
    ap.add_argument("--processed-bucket", help="GCS processed candidates bucket (default: read from project.yaml)")
    ap.add_argument("--output-name", default="candidates_clean_all.csv", help="Output filename in processed bucket")
    ap.add_argument("--states", default="AC AM AP MA MT PA RO RR TO", help="List of states to include in clean data (default Amazon states)")
    args = ap.parse_args()
    
    # Load cleaning schema
    with open(args.schema, "r") as f:
        schema = yaml.safe_load(f)
    
    # Define list of columns to keep with target names
    keep_columns = {col: schema["columns"][col]["target"] for col in schema["columns"]}

    # Get relevant variables
    encoding = schema["meta"]["output"]["encoding"]
    states   = [s.strip().upper() for s in args.states.split()]
    
    # Initiate GCS client
    client = storage.Client(project=args.project);

    # Get page prefixes from the raw bucket
    bucket_name   = normalize_bucket_name(args.raw_bucket)
    page_prefixes = list_page_prefixes(client, bucket_name, project=args.project)

    # Initiate list of dataframes to hold all years
    df_list = []

    # Go through each page prefix (year) and read the CSV
    for page_prefix in tqdm(page_prefixes, desc="Processing year CSVs"):

        # Read and clean CSV according to schema
        df = read_year_csv(client, bucket_name, page_prefix, project=args.project)
        df = clean_with_schema(df, schema)
        
        # Filter by states
        df = df.query("state in @states")
        
        # Append to list of DataFrames
        df_list.append(df)

    # Concatenate all DataFrames into one
    if not df_list:
        raise SystemExit("No data found to process. Check your raw bucket or schema.")
    df = pd.concat(df_list, ignore_index=True)

    # Upload to GCS processed bucket
    processed_bucket = normalize_bucket_name(args.processed_bucket)
    upload_csv_to_gcs(client, processed_bucket, "candidates_all_clean.csv", encoding, df)
    

# Run script directly
if __name__ == "__main__":
    main()