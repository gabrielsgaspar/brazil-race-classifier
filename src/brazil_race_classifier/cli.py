# src/brazil_race_classifier/cli.py
import argparse
from typing import List, Optional
from . import __version__
from .data.upload_candidates_raw import run_download_candidates

def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(
        prog="brc",
        description="Brazil Race Classifier – download TSE candidates and upload to GCS",
    )
    p.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    p.add_argument("--config", default="configs/tse_urls.yaml", help="Path to YAML with candidates URLs")
    p.add_argument("--bucket", required=True, help="GCS bucket name or gs://bucket for RAW candidates")
    p.add_argument("--project", required=True, help="GCP project ID")
    p.add_argument("--years", nargs="*", help="Optional list of years (e.g., 2016 2020 2024)")
    args = p.parse_args(argv)

    # Download TSE candidate data and upload to GCS
    return run_download_candidates(config=args.config,
                                   bucket=args.bucket,
                                   project=args.project,
                                   years=args.years)

if __name__ == "__main__":
    raise SystemExit(main())
