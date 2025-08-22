#!/usr/bin/env python3
import argparse, io, os, re, requests, yaml, zipfile
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from io import StringIO
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


# Upload candidates data to GCS bucket
def upload_parquet_to_gcs(client: storage.Client, bucket_name: str, dest_path: str, df: pd.DataFrame) -> None:
    """
    Uploads a parquet file to a specified GCS bucket and path.
    """
    # Initiate GCS client and get the bucket
    bucket = client.bucket(bucket_name)
    blob   = bucket.blob(dest_path)

    # Write DataFrame to a buffer in Parquet format
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)

    # Upload buffer to GCS
    blob.upload_from_file(buffer, content_type="application/octet-stream")


# Main function to clean data and upload TSE candidate data to GCS
def main():
    """
    Main function to download, clean and upload TSE candidate data to GCS.
    """
    # Parse command line arguments
    ap = argparse.ArgumentParser(description="Scrape indigenous names from ISA and upload CSV to GCS.")
    ap.add_argument("--schema", default="configs/cleaning/cleaning_schema.yaml", help="Path to YAML with cleaning schema")
    ap.add_argument("--project", help="GCP project ID (default: read from configs/project.yaml)")
    ap.add_argument("--processed_bucket", help="GCS processed candidates bucket (default: read from project.yaml)")
    ap.add_argument("--output_name", default="isa_names", help="Output filename in processed bucket")
    ap.add_argument("--isa_url", default="https://pib.socioambiental.org/pt/Quadro_Geral_dos_Povos", help="ISA URL with names of indigenous peoples")
    args = ap.parse_args()

    # Load cleaning schema
    with open(args.schema, "r") as f:
        schema = yaml.safe_load(f)

    # Request the ISA page and check for successful response
    res = requests.get(args.isa_url)
    if res.status_code != 200:
        raise Exception(f"Failed to download page from {args.isa_url}. Status code: {res.status_code}")
    
    # Get relevant variables
    encoding    = schema["meta"]["output"]["encoding"]
    output_name = f"{args.output_name}.parquet"

    # Parse tables into a list of DataFrames and pick first table
    tables = pd.read_html(StringIO(res.text))
    df     = tables[0]
    names  = []

    # Keep only relevant columns and rename them
    df = df[pd.to_numeric(df["#"], errors="coerce").notna()]
    df = df[["Nomes", "Outros nomes ou grafias"]]

    # Iterate over rows to clean names
    for i in df.index:
        name_raw = df.loc[i, "Nomes"]
        name_str = str(name_raw).strip().lower()
        name     = unidecode(name_str)

        # Find all text inside parentheses
        parens = re.findall(r"\((.*?)\)", name)
        
        # Append each parenthesis content (if any)
        for p in parens:
            names.append(p.strip())
            
        # Remove the parentheses (and their contents) from the original name
        name = re.sub(r"\(.*?\)", "", name).strip()
        names.append(name)

        # Go over alternative names
        if not pd.isna(df.loc[i, "Outros nomes ou grafias"]):
            for j in str(df.loc[i, "Outros nomes ou grafias"]).split(","):
                j = j.strip().lower()
                j = unidecode(j)
                if j:
                    names.append(j)

    # Remove duplicates and sort names
    names = set(names)

    # Create a DataFrame and upload to GCS
    df_names = pd.DataFrame({"name": sorted(names)})

    # Upload to GCS processed bucket
    client = storage.Client(project=args.project)
    processed_bucket = normalize_bucket_name(args.processed_bucket)
    upload_parquet_to_gcs(client, processed_bucket, output_name, df_names)
    
    # Save to data folder locally
    df.to_parquet(f"./data/tse/{output_name}", index=False, engine="pyarrow")

# Run script directly
if __name__ == "__main__":
    main()