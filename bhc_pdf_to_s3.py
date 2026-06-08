"""
BHC Judgments → MongoDB + PDFs → S3
====================================
Thin wrapper around bhc_scrapling.py with S3 upload forced ON.

PDF upload now lives inside bhc_scrapling.py and runs automatically whenever
S3_BUCKET is configured, so these two commands are equivalent:

    python bhc_scrapling.py        # uploads if S3_BUCKET is set (UPLOAD_TO_S3=0 to skip)
    python bhc_pdf_to_s3.py        # always uploads

It scrapes every listing and, in the same Scrapling session (fresh, non-expired
download tokens), downloads each PDF, uploads it to S3, and saves both
`s3_uri` and `public_url` into the MongoDB document. Idempotent and resumable.

Config & requirements: see bhc_scrapling.py.
"""

import os

# Force upload on before bhc_scrapling reads its config at import time.
os.environ["UPLOAD_TO_S3"] = "1"

import bhc_scrapling

if __name__ == "__main__":
    try:
        bhc_scrapling.main()
    except KeyboardInterrupt:
        bhc_scrapling.log.info("Interrupted – flushing JSON before exit")
        bhc_scrapling.flush_json()
