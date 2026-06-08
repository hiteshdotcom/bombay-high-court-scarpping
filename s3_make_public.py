"""
Make the BHC judgment PDFs publicly readable over HTTPS  (run ONCE)
===================================================================
Modern S3 buckets block public access by default and have ACLs disabled, so the
correct way to expose objects is a **bucket policy** scoped to our prefix.

This script:
  1. Relaxes the bucket's "Block Public Access" settings that would otherwise
     override a public policy (BlockPublicPolicy / RestrictPublicBuckets → False).
     The ACL-related toggles are left untouched.
  2. Adds an idempotent public-read statement (Sid: BHCPublicReadJudgments)
     granting s3:GetObject on  <bucket>/<S3_PREFIX>/*  to everyone.
     An existing policy is preserved; only our statement is inserted/updated.

After this, every object under the prefix is reachable at:
    https://<bucket>.s3.<region>.amazonaws.com/<key>

⚠️  This makes those PDFs world-readable. Bombay High Court judgments are public
    records, but be deliberate: anyone with the URL can download them. To undo,
    run with  --revoke  (removes the statement and re-blocks public access).

Requirements:  pip install boto3
Config (.env): S3_BUCKET (required), S3_PREFIX, AWS_REGION, AWS credentials.

Usage:
    python s3_make_public.py            # make the prefix public
    python s3_make_public.py --revoke   # undo
"""

import json
import os
import sys
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

SID = "BHCPublicReadJudgments"


def load_dotenv(path=".env"):
    p = Path(path)
    if not p.exists():
        return
    for line in p.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, _, v = line.partition("=")
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


def main():
    load_dotenv()
    revoke = "--revoke" in sys.argv

    bucket = os.getenv("S3_BUCKET")
    prefix = os.getenv("S3_PREFIX", "bhc-judgments").strip("/")
    region = os.getenv("AWS_REGION", "us-east-1")
    if not bucket:
        print("ERROR: S3_BUCKET not set in .env"); return

    endpoint = os.getenv("S3_ENDPOINT_URL")
    s3 = boto3.client("s3", region_name=region,
                      **({"endpoint_url": endpoint} if endpoint else {}))

    resource = f"arn:aws:s3:::{bucket}/{prefix}/*"

    # ── 1. Block Public Access ────────────────────────────────────────────────
    if revoke:
        bpa = dict(BlockPublicAcls=True, IgnorePublicAcls=True,
                   BlockPublicPolicy=True, RestrictPublicBuckets=True)
    else:
        # Only relax the policy-related toggles; keep ACL blocking on.
        try:
            cur = s3.get_public_access_block(Bucket=bucket)["PublicAccessBlockConfiguration"]
        except ClientError:
            cur = dict(BlockPublicAcls=True, IgnorePublicAcls=True,
                       BlockPublicPolicy=True, RestrictPublicBuckets=True)
        bpa = {**cur, "BlockPublicPolicy": False, "RestrictPublicBuckets": False}
    try:
        s3.put_public_access_block(Bucket=bucket,
                                   PublicAccessBlockConfiguration=bpa)
        print(f"Block Public Access updated: {bpa}")
    except ClientError as e:
        print(f"WARNING: could not update Block Public Access: {e}")

    # ── 2. Bucket policy ──────────────────────────────────────────────────────
    try:
        policy = json.loads(s3.get_bucket_policy(Bucket=bucket)["Policy"])
        statements = [s for s in policy.get("Statement", []) if s.get("Sid") != SID]
    except ClientError as e:
        if e.response["Error"]["Code"] in ("NoSuchBucketPolicy", "NoSuchBucket"):
            policy, statements = {"Version": "2012-10-17", "Statement": []}, []
        else:
            print(f"ERROR reading bucket policy: {e}"); return

    if not revoke:
        statements.append({
            "Sid": SID,
            "Effect": "Allow",
            "Principal": "*",
            "Action": "s3:GetObject",
            "Resource": resource,
        })

    if statements:
        policy["Statement"] = statements
        s3.put_bucket_policy(Bucket=bucket, Policy=json.dumps(policy))
    else:
        # No statements left → remove the policy entirely.
        try:
            s3.delete_bucket_policy(Bucket=bucket)
        except ClientError:
            pass

    if revoke:
        print(f"Revoked public access for s3://{bucket}/{prefix}/*")
        return

    sample = (f"https://{bucket}.s3.amazonaws.com/{prefix}/..."
              if region == "us-east-1"
              else f"https://{bucket}.s3.{region}.amazonaws.com/{prefix}/...")
    print(f"\nDONE. Objects under  s3://{bucket}/{prefix}/  are now public.")
    print(f"Public URL pattern:  {sample}")


if __name__ == "__main__":
    main()
