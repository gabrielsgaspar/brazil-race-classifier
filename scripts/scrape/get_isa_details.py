#!/usr/bin/env python3
import argparse, io, json, os, re, requests, yaml, zipfile
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from io import StringIO
from google.cloud import storage
from openai import OpenAI
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



# Get text from URL
def get_isa_text(url: str) -> str:
    """"
    Visits URL and extracts full text from page.
    """

    # Request page and check success
    res  = requests.get(url, headers={"User-Agent": "Mozilla/5.0", "Accept-Language": "pt"}, timeout=30)
    if res.status_code==200:
        soup = BeautifulSoup(res.text, "html.parser")
        text = soup.get_text("\n", strip=True)
        return text
    else:
        return ""


# Main function to clean data and upload TSE candidate data to GCS
def main():
    """
    Main function to download, clean and upload TSE candidate data to GCS.
    """
    # Parse command line arguments
    ap = argparse.ArgumentParser(description="Scrape indigenous groups names from ISA and MPE and upload Parquet to GCS.")
    ap.add_argument("--schema", default="configs/cleaning/cleaning_schema.yaml", help="Path to YAML with cleaning schema")
    ap.add_argument("--project", help="GCP project ID (default: read from configs/project.yaml)")
    ap.add_argument("--prompts", default="prompts/scrape_isa.yaml", help="Path to YAML with prompts")
    ap.add_argument("--output_name_isa", default="isa_details", help="Output filename in processed bucket")
    ap.add_argument("--isa_url", default="https://pib.socioambiental.org/pt/Quadro_Geral_dos_Povos", help="ISA URL with names of indigenous peoples")
    args = ap.parse_args()

    # Load cleaning schema
    with open(args.schema, "r") as f:
        schema = yaml.safe_load(f)

    # Get relevant variables
    encoding        = schema["meta"]["output"]["encoding"]
    output_name_isa = f"{args.output_name_isa}.parquet"

    # Load prompts
    with open(args.propmts, "r") as f:
        prompts = yaml.safe_load(f)
    
    # Define OpenAI client and system prompt
    system_message = {"role": "system", "content": (prompts["system"])}
    model          = prompts["model"]["name"]
    temperature    = int(prompts["model"]["temperature"])
    seed           = int(prompts["model"]["seed"])

    # Request the ISA page and get details and URLs
    url_isa = "https://pib.socioambiental.org/pt/Quadro_Geral_dos_Povos"
    res     = requests.get(url_isa)
    soup    = BeautifulSoup(res.text, "html.parser")

    # Get list of all a handles and filter to those of people groups
    a_list = soup.find_all("a")
    a_list = [a for a in a_list if "pt/povo/" in a["href"]]

    # Populate dictionary with data
    df_isa = pd.DataFrame({"Nomes": [a.text.strip() for a in a_list],
                        "url"  : ["https://pib.socioambiental.org" + a["href"] for a in a_list]})

    # Initiate variables
    df_isa["name"] = None
    df_isa["other_names"] = None
    df_isa["summary"] = None
    df_isa["location"] = None

    # Go over rows
    for r in tqdm(df_isa.index):
        
        # Get ethnicity and url
        ethnicity    = df_isa.loc[r, "Nomes"]
        isa_url      = df_isa.loc[r, "url"]
        isa_url_text = get_isa_text(isa_url)

        # Define OpenAI client and system prompt
        user_message = {"role": "user", "content": (prompts["user"].format(ethnicity=ethnicity, isa_url_text=isa_url_text))}

        # Define overall message
        messages = [system_message, user_message]

        # Make API call
        response = client.chat.completions.create(model           = model,
                                                    response_format = {"type": "json_object"},
                                                    messages        = messages,
                                                    temperature     = temperature,
                                                    seed            = seed)
        # Load and populate
        res_dict = json.loads(response.choices[0].message.content)
        for k in res_dict.keys():
            df_isa.loc[r, k] = res_dict[k]

    
    # Upload to GCS processed bucket
    client = storage.Client(project=args.project)
    processed_bucket = normalize_bucket_name(args.processed_bucket)
    upload_parquet_to_gcs(client, processed_bucket, output_name_isa, df_isa)
    
    # Save to data folder locally
    df_isa.to_parquet(f"./data/isa/{output_name_isa}", index=False, engine="pyarrow")


# Run script directly
if __name__ == "__main__":
    main()