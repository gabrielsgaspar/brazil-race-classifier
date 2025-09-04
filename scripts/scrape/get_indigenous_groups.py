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
    ap = argparse.ArgumentParser(description="Scrape indigenous groups names from ISA and MPE and upload Parquet to GCS.")
    ap.add_argument("--schema", default="configs/cleaning/cleaning_schema.yaml", help="Path to YAML with cleaning schema")
    ap.add_argument("--project", help="GCP project ID (default: read from configs/project.yaml)")
    ap.add_argument("--processed_bucket", help="GCS processed candidates bucket (default: read from project.yaml)")
    ap.add_argument("--output_name_isa", default="isa_groups", help="Output filename in processed bucket")
    ap.add_argument("--output_name_mpe", default="mpe_groups", help="Output filename in processed bucket")
    ap.add_argument("--isa_url", default="https://pib.socioambiental.org/pt/Quadro_Geral_dos_Povos", help="ISA URL with names of indigenous peoples")
    ap.add_argument("--mpe_url", default="https://www.gov.br/funai/pt-br/acesso-a-informacao/dados-abertos/base-de-dados/Lista_Etnias___Nota_Tecnica_Conjunta_1.2019___Funai.IBGE.SESAI.SAGI.csv/@@download/file", help="MPE URL with names of indigenous peoples")
    args = ap.parse_args()

    # Load cleaning schema
    with open(args.schema, "r") as f:
        schema = yaml.safe_load(f)

    # Request the ISA page and check for successful response
    res = requests.get(args.isa_url)
    if res.status_code != 200:
        raise Exception(f"Failed to download page from {args.isa_url}. Status code: {res.status_code}")
    
    # Get relevant variables
    encoding        = schema["meta"]["output"]["encoding"]
    output_name_isa = f"{args.output_name_isa}.parquet"
    output_name_mpe = f"{args.output_name_mpe}.parquet"

    # Parse tables into a list of DataFrames and pick first table
    tables = pd.read_html(StringIO(res.text))
    df     = tables[0]
    names  = []

    # Keep only relevant columns and rename them
    df_isa = df[pd.to_numeric(df["#"], errors="coerce").notna()].copy()
    df_isa = df_isa[["Nomes", "Outros nomes ou grafias", "Família linguística"]]
    
    # Upload to GCS processed bucket
    client = storage.Client(project=args.project)
    processed_bucket = normalize_bucket_name(args.processed_bucket)
    upload_parquet_to_gcs(client, processed_bucket, output_name_isa, df_isa)
    
    # Save to data folder locally
    df.to_parquet(f"./data/isa/{output_name_isa}", index=False, engine="pyarrow")

    # Download and save MPE data
    df_mpe = pd.read_csv(args.mpe_url, sep=";", encoding="latin-1", dtype=str)
    df.to_parquet(f"./data/mpe/{output_name_mpe}", index=False, engine="pyarrow")
    upload_parquet_to_gcs(client, processed_bucket, output_name_mpe, df_mpe)


# Run script directly
if __name__ == "__main__":
    main()