#!/usr/bin/env python3
import argparse, io, os, re, requests, tempfile, yaml, zipfile
from google.cloud import storage
from pathlib import Path
from typing import Dict, List, Optional
from tqdm import tqdm


# Normalize bucket name
def normalize_bucket_name(bucket: str) -> str:
    """
    Accept gs://bucket or bucket and return plain bucket name.
    """
    return bucket.replace("gs://", "").strip("/")


# Clean up member name to a safe jpg basename
def clean_basename(name: str) -> str:
    """
    Normalize a member name to a safe jpg basename (strip dirs, force .jpg).
    """
    base = Path(name).name
    base = re.sub(r"\.(jpe?g)$", ".jpg", base, flags=re.IGNORECASE)
    return base


# Download zip file from TSE url to a temporary file
def download_zip_to_tempfile(url: str, timeout: int=180) -> str:
    """
    Stream a ZIP from `url` to a NamedTemporaryFile; return its path.
    """
    tmp = tempfile.NamedTemporaryFile(prefix="tse_zip_", suffix=".zip", delete=False)
    try:
        with requests.get(url, stream=True, timeout=timeout) as r:
            r.raise_for_status()
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    tmp.write(chunk)
        tmp.flush()
        return tmp.name
    finally:
        tmp.close()


# Upload files to GCS using streaming
def upload_member_streaming(client: storage.Client, bucket_name: str, dest_path: str, zf: zipfile.ZipFile, info: zipfile.ZipInfo):
    """
    Open a ZIP member and stream-upload it to GCS without loading into memory.
    """
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(dest_path)
    # ZipInfo.file_size gives the uncompressed size, which lets us avoid rewind().
    with zf.open(info) as fp:
        blob.upload_from_file(fp, size=info.file_size, content_type="image/jpeg", rewind=False)


# Main function to upload photos to GCS
def main():
    """
    Main function to upload photos to GCS.
    """
    # Parse command line arguments
    ap = argparse.ArgumentParser(description="Download TSE photo ZIPs per (year, UF) and upload extracted JPGs to GCS.")
    ap.add_argument("--config", default="configs/tse_urls.yaml", help="Path to YAML with image URLs")
    ap.add_argument("--bucket", required=True, help="GCS bucket name or gs://bucket for photos")
    ap.add_argument("--years", nargs="*", help="Optional list of years to process (e.g., 2016 2020 2024)")
    ap.add_argument("--states", nargs="*", help="Optional list of UFs (e.g., AC AM SP)")
    ap.add_argument("--project", required=True, help="GCP project ID (overrides env/default)")
    args = ap.parse_args()

    # Load candidates URLs from the configuration file
    with open(args.config, "r") as f:
        cfg = yaml.safe_load(f) or {}

    # Get photos configuration
    photos_cfg: Dict = cfg.get("photos", {}) or {}
    templates: Dict = photos_cfg.get("templates", {}) or {}
    states: List[str] = photos_cfg.get("states") or (cfg.get("defaults", {}) or {}).get("states", [])
    states = [s.strip().upper() for s in states]

    # Filter years/states if provided
    templates = {str(y): u for y, u in templates.items()}
    if args.years:
        yrset     = {str(y) for y in args.years}
        templates = {y: u for y, u in templates.items() if y in yrset}
    if args.states:
        stset  = {s.strip().upper() for s in args.states}
        states = [s for s in states if s in stset]

    if not templates:
        raise SystemExit("No photo templates to process (check --years or config).")
    if not states:
        raise SystemExit("No states to process (check --states or config).")

    # Bucket and project
    bucket_name = normalize_bucket_name(args.bucket)
    project_id  = args.project
    client      = storage.Client(project=project_id)

    # Download and process each template by year
    for year, url_tmpl in sorted(templates.items()):
        for uf in states:
            print(f"\nDownloading photos for state {uf} in year {year} ...")
            url = url_tmpl.format(UF=uf)
            tmp_zip = None
            try:
                # 1) Stream ZIP to a temp file
                tmp_zip = download_zip_to_tempfile(url)

                # 2) Iterate members and stream-upload JPGs
                uploaded = 0
                with zipfile.ZipFile(tmp_zip) as zf:
                    for info in tqdm(zf.infolist()):
                        if info.is_dir():
                            continue
                        if not info.filename.lower().endswith((".jpg", ".jpeg", ".png")):
                            continue
                        dest_name = clean_basename(info.filename)
                        dest_path = f"{year}/{uf}/{dest_name}"
                        upload_member_streaming(client, bucket_name, dest_path, zf, info)
                        uploaded += 1

            except Exception as e:
                print(f"Error in year {year} state {uf}: {e}\n")
            finally:
                if tmp_zip and os.path.exists(tmp_zip):
                    try:
                        os.remove(tmp_zip)
                    except OSError:
                        pass 


# Run script directly
if __name__ == "__main__":
    main()