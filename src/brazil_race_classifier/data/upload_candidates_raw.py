#!/usr/bin/env python3
import argparse, io, os, re, requests, yaml, zipfile
import pandas as pd
from google.cloud import storage
from tqdm import tqdm


# Normalize bucket name
def normalize_bucket_name(bucket: str) -> str:
    """
    Accept gs://bucket or bucket and return plain bucket name.
    """
    return bucket.replace("gs://", "").strip("/")


# Download zip file from TSE url
def read_tse_zip(url: str, target: str="BRASIL") -> pd.DataFrame:
    """
    Downloads a zip file from the TSE candidate URL and extracts the content for all Brazil candidate data.
    """
    # Request the zip file and check for successful response
    res = requests.get(url)
    if res.status_code != 200:
        raise Exception(f"Failed to download file from {url}. Status code: {res.status_code}")
    
    # Extract file name structure from url and set target file name
    file_structure = re.search(r"(consulta_cand_\d{4})", url).group(1)
    target_file    = f"{file_structure}_{target.upper()}.csv"

    # Read target csv file from the zip archive
    with zipfile.ZipFile(io.BytesIO(res.content)) as z:

        # Check if target file exists in the zip list
        if target_file in z.namelist():
            with z.open(target_file) as f:
                # Read the CSV file into a DataFrame
                df = pd.read_csv(f, encoding="latin-1", sep=";", dtype=str)
                # Return pandas dataframe
                return df
        else:
            raise Exception(f"Target file '{target_file}' not found in the zip archive.")


# Upload candidates data to GCS bucket
def upload_csv_to_gcs(bucket_name: str, dest_path: str, df: pd.DataFrame, project_id: str) -> None:
    """
    Uploads a local CSV file to a specified GCS bucket and path.
    """
    # Initiate GCS client and get the bucket
    client = storage.Client(project=project_id)
    bucket = client.bucket(bucket_name)
    blob   = bucket.blob(dest_path)

    # Write DataFrame to bytes and upload to GCS
    csv_bytes = df.to_csv(index=False, encoding="latin-1").encode("latin-1")
    blob.upload_from_string(csv_bytes, content_type="text/csv")


# Function for CLI command to download TSE candidates and upload to GCS
def run_download_candidates(*, config: str = "configs/tse_urls.yaml", bucket: str, project: str, years: list[str] | None = None) -> int:
    """
    Same behavior as main(), but callable from code/CLI without argparse.
    Returns 0 on success, 1 if any year fails.
    """
    # Check required parameters
    if not bucket:
        raise ValueError("bucket is required")
    if not project:
        raise ValueError("project is required")

    # Load candidates URLs from the configuration file
    with open(config, "r", encoding="utf-8") as f:
        urls = yaml.safe_load(f) or {}

    # Get candidates URLs and keep only the specified years if provided
    candidates_urls = urls.get("candidates", {}) or {}
    if years:
        years_set = {str(y) for y in years}
        candidates_urls = {year: url for year, url in candidates_urls.items() if str(year) in years_set}

    # Normalize bucket and project
    bucket_name = normalize_bucket_name(bucket)
    project_id  = project
    any_failed  = False

    # Download and process each candidate URL by year
    for year, url in tqdm(candidates_urls.items(), desc="Downloading candidates data"):
        try:
            df        = read_tse_zip(url)  # your existing helper
            dest_path = f"{year}/candidates_{year}.csv"
            upload_csv_to_gcs(bucket_name, dest_path, df, project_id)
        except Exception as e:
            any_failed = True
            print(f"Error processing year {year}: {e}")
            continue
    
    # Return 1 if any year failed, otherwise 0
    return 1 if any_failed else 0


# Main function to download TSE candidate data and upload to GCS
def main():
    """
    Main function to download TSE candidate data and upload to GCS.
    """
    # Parse command line arguments
    ap = argparse.ArgumentParser(description="Download TSE candidate ZIPs and upload raw CSVs to GCS.")
    ap.add_argument("--config", default="configs/tse_urls.yaml", help="Path to YAML with candidates URLs")
    ap.add_argument("--bucket", required=True, help="GCS bucket name or gs://bucket for RAW candidates")
    ap.add_argument("--years", nargs="*", help="Optional list of years to process (e.g., 2016 2020 2024)")
    ap.add_argument("--project", required=True, help="GCP project ID (overrides env/default)")
    args = ap.parse_args()

    # Load candidates URLs from the configuration file
    with open(args.config, "r") as f:
        urls = yaml.safe_load(f)

    # Get candidates URLs and keep only the specified years if provided
    candidates_urls = urls.get("candidates", {})
    if args.years:
        candidates_urls = {year: url for year, url in candidates_urls.items() if str(year) in args.years}
    
    # Get bucket name from command line argument and project ID
    bucket_name = normalize_bucket_name(args.bucket)
    project_id  = args.project

    # Download and process each candidate URL by year
    for year, url in tqdm(candidates_urls.items(), desc="Downloading candidates data"):
        try:
            # Read the TSE zip file and extract the DataFrame
            df = read_tse_zip(url)

            # Define the destination path in GCS
            dest_path = f"{year}/candidates_{year}.csv"

            # Upload the DataFrame to GCS
            upload_csv_to_gcs(bucket_name, dest_path, df, project_id)
            
        except Exception as e:
            print(f"Error processing year {year}: {e}")
            continue
    

# Run script directly
if __name__ == "__main__":
    main()