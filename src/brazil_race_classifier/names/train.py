#!/usr/bin/env python3
import argparse, io, os, re, requests, torch, yaml, zipfile
import numpy as np
import pandas as pd
from google.cloud import storage
from sentence_transformers import SentenceTransformer
from sklearn.model_selection import train_test_split
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
def read_blob_csv_as_df(client: storage.Client, bucket_name: str, blob_name: str, encoding: str="utf-8") -> pd.DataFrame:
    """Download a CSV blob as bytes and read into pandas with dtype=str."""
    bucket = client.bucket(bucket_name)
    blob   = bucket.blob(blob_name)
    data   = blob.download_as_bytes()
    return pd.read_csv(io.BytesIO(data), encoding=encoding, dtype=str, low_memory=False)


# Main function to train name classification models
def main():
    """
    Main function to retrieve, organize and train models to classify indigenous names.
    """
    # Parse command line arguments
    ap = argparse.ArgumentParser(description="Train models to classify indigenous strings.")
    ap.add_argument("--project", help="GCP project ID (default: read from configs/project.yaml)")
    ap.add_argument("--processed-bucket", help="GCS processed candidates bucket (default: read from project.yaml)")
    ap.add_argument("--sentence-transformer", default="PORTULAN/serafim-100m-portuguese-pt-sentence-encoder", help="Name of the SentenceTransformer model to use")
    ap.add_argument("--output-name", default="indigenous_names.csv", help="Output filename in processed bucket")
    args = ap.parse_args()

    pass
    

# Run script directly
if __name__ == "__main__":
    main()