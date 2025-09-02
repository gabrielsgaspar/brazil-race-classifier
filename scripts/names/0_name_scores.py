#!/usr/bin/env python3
import argparse, io, json, os, random, re, requests, torch, yaml
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from openai import OpenAI
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
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


# Read data from GCS bucket
def read_blob_parquet_as_df(client: storage.Client, bucket_name: str, blob_name: str) -> pd.DataFrame:
    """Download a Parquet blob as bytes and read into pandas."""
    bucket = client.bucket(bucket_name)
    blob   = bucket.blob(blob_name)
    data   = blob.download_as_bytes()
    return pd.read_parquet(io.BytesIO(data), engine="pyarrow")


# Main function to organize, train Logistic regression and use OpenAI API
def main():
    """
    Main function to get top indigenous names using ChatGPT and Logistic regression
    """
    # Parse command line arguments
    ap = argparse.ArgumentParser(description="Train Logistic regression and find top indigenous names with ChatGPT.")
    ap.add_argument("--schema", default="configs/prompts/classify_names.yaml", help="Path to YAML with prompt to classify names as indigenous")
    ap.add_argument("--topk", default=500, help="Top N indigenous associated tokens to use in prompt")
    ap.add_argument("--project", help="GCP project ID (default: read from configs/project.yaml)")
    ap.add_argument("--raw_bucket", help="GCS raw candidates bucket (default: read from project.yaml)")
    ap.add_argument("--processed_bucket", help="GCS processed candidates bucket (default: read from project.yaml)")
    ap.add_argument("--output_name", default="gpt_names_scores", help="Output filename in processed bucket")
    args = ap.parse_args()

    # Load prompts
    with open(args.schema, "r") as f:
        prompts = yaml.safe_load(f)

    # Set output name
    output_name = f"{args.output_name}.json"

    # Load environmental variables and initiate OpenAI client
    load_dotenv()
    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
    client         = OpenAI(api_key=OPENAI_API_KEY)

    # Load data

    # Train Logistic regression

    # Retrieve top names
    indigenous_name_list = []

    # Load prompts
    system_message = {"role": "system",
                      "content": (prompts["system"])
                      }
    user_message = {"role": "user",
                    "content": (prompts["user"].format(indigenous_name_list=indigenous_name_list))
                    }
    