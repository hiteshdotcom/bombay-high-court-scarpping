"""
Bombay High Court Judgment Scraper  ─  Scrapling edition
=========================================================
Scrapes all 4 judgment categories from bombayhighcourt.gov.in/bhc/judgments
using the Scrapling framework (https://scrapling.readthedocs.io):

  • F.B. Judgment          → GET  /judgments/fullbench   (JSON → table HTML)
  • D.B. Ref. Judgments    → GET  /judgments/dbref       (JSON → table HTML)
  • F.B. Orders            → GET  /judgments/fullorders   (JSON → table HTML)
  • Rept. Judgment/Order   → POST /judgments/advocatename (date range, paginated)

Stores judgment metadata in MongoDB; mirrors to a local JSON file.

Stable identity
---------------
The PDF download link is an encrypted token that is re-generated on every fetch
and expires within hours, so it CANNOT be used to identify a judgment. Instead a
stable `uid` is derived from (collection, case_no, date, side, party, coram, seq)
where `seq` distinguishes the occasional case that has several PDFs sharing
identical metadata. `uid` is the dedupe / upsert key everywhere.

This module also exposes `iter_listings(session)`, a generator that yields
(collection, judgment_type, docs) for every listing. The S3 pipeline
(bhc_pdf_to_s3.py) reuses it so PDFs are downloaded with fresh tokens.

Requirements:
    pip install "scrapling[fetchers]" pymongo

Usage:
    python bhc_scrapling.py

Configuration (.env or environment variables):
    MONGO_URI, MONGO_DB, START_DATE (dd-mm-yyyy), END_DATE (dd-mm-yyyy)
"""

import hashlib
import json
import os
import re
import sys
import time
import random
import logging
from datetime import datetime, date, timedelta, timezone
from pathlib import Path

from scrapling.fetchers import FetcherSession
from scrapling import Selector

# ── Try MongoDB ───────────────────────────────────────────────────────────────
try:
    from pymongo import MongoClient, UpdateOne, ASCENDING
    PYMONGO_AVAILABLE = True
except ImportError:
    PYMONGO_AVAILABLE = False
    print("pymongo not installed – run:  pip install pymongo")
    print("Continuing with JSON-only mode.\n")

# ── Try boto3 (optional, only needed for S3 upload) ─────────────────────────────
try:
    import boto3
    from botocore.exceptions import ClientError
    BOTO3_AVAILABLE = True
except ImportError:
    BOTO3_AVAILABLE = False


# ─── Load .env (no external dependency required) ──────────────────────────────

def _load_dotenv(path=".env"):
    """Minimal .env loader. Real environment variables take precedence."""
    p = Path(path)
    if not p.exists():
        return
    for line in p.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        os.environ.setdefault(key.strip(), val.strip().strip('"').strip("'"))

_load_dotenv()

# ─── Configuration ────────────────────────────────────────────────────────────

MONGO_URI      = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB_NAME  = os.getenv("MONGO_DB",  "bhc_judgments")
BASE_URL       = "https://bombayhighcourt.gov.in/bhc"
JUDGMENTS_URL  = f"{BASE_URL}/judgments"

ADVOCATENAME_URL = f"{JUDGMENTS_URL}/advocatename"
FULLBENCH_URL    = f"{JUDGMENTS_URL}/fullbench"
DBREF_URL        = f"{JUDGMENTS_URL}/dbref"
FULLORDERS_URL   = f"{JUDGMENTS_URL}/fullorders"

# (endpoint, collection, human label) for the three single-GET tabs.
STATIC_TABS = [
    (FULLBENCH_URL,  "fb_judgments",     "F.B. Judgment"),
    (DBREF_URL,      "db_ref_judgments", "D.B. Ref. Judgments"),
    (FULLORDERS_URL, "fb_orders",        "F.B. Orders"),
]
REPT_COLLECTION = "rept_judgments"
ALL_COLLECTIONS = [REPT_COLLECTION] + [c for _, c, _ in STATIC_TABS]

START_DATE = os.getenv("START_DATE", "01-01-1960")  # dd-mm-yyyy
END_DATE   = os.getenv("END_DATE",   date.today().strftime("%d-%m-%Y"))

# Set REPT_ONLY=1 to scrape ONLY the Rept. Judgment/Order tab (skip the three
# static tabs: F.B. Judgment, D.B. Ref. Judgments, F.B. Orders).
REPT_ONLY  = os.getenv("REPT_ONLY", "0") == "1"

MIN_DELAY, MAX_DELAY = 2.0, 5.0
JSON_FILE = "bhc_judgments.json"

# ── S3 upload (optional) ──
# If S3_BUCKET is set (and boto3 is installed), each judgment PDF is downloaded
# in-session and uploaded to S3, and its links are saved to MongoDB. Set
# UPLOAD_TO_S3=0 to force metadata-only. DRY_RUN downloads but does not upload.
S3_BUCKET       = os.getenv("S3_BUCKET")
S3_PREFIX       = os.getenv("S3_PREFIX", "bhc-judgments").strip("/")
AWS_REGION      = os.getenv("AWS_REGION", "us-east-1")
S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL")
S3_ACL          = os.getenv("S3_ACL", "").strip()
UPLOAD_TO_S3    = os.getenv("UPLOAD_TO_S3", "1" if S3_BUCKET else "0") == "1"
DRY_RUN         = os.getenv("DRY_RUN", "0") == "1"
LIMIT           = int(os.getenv("LIMIT", "0"))

S3_FIELDS = ("s3_bucket", "s3_key", "s3_uri", "public_url", "pdf_size", "uploaded_at")

# ─── Logging ──────────────────────────────────────────────────────────────────

try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except (AttributeError, ValueError):
    pass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[
        logging.FileHandler("bhc_scrapling.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("bhc")
logging.getLogger("scrapling").setLevel(logging.WARNING)


def sleep_politely():
    time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))


# ─── MongoDB helpers ──────────────────────────────────────────────────────────

_mongo_client = None
_mongo_db     = None

def get_db():
    global _mongo_client, _mongo_db
    if _mongo_db is not None:
        return _mongo_db
    if not PYMONGO_AVAILABLE:
        return None
    try:
        _mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        _mongo_client.server_info()
        _mongo_db = _mongo_client[MONGO_DB_NAME]
        log.info(f"Connected to MongoDB: {MONGO_DB_NAME}")
        for col in ALL_COLLECTIONS:
            _mongo_db[col].create_index([("case_no", ASCENDING), ("date", ASCENDING)])
            try:
                _mongo_db[col].create_index("uid", unique=True)
            except Exception as e:
                log.warning(f"  Could not create unique uid index on {col}: {e}")
        return _mongo_db
    except Exception as e:
        log.warning(f"MongoDB unavailable ({e}) – falling back to JSON.")
        return None


def upsert_to_mongo(collection_name: str, docs: list):
    db = get_db()
    if db is None or not docs:
        return
    col = db[collection_name]
    ops = []
    for d in docs:
        s3f = {k: d[k] for k in S3_FIELDS if k in d}
        meta = {k: v for k, v in d.items() if k not in s3f}
        update = {"$setOnInsert": meta}
        if s3f:                       # add/refresh S3 links even on existing docs
            update["$set"] = s3f
        ops.append(UpdateOne({"uid": d["uid"]}, update, upsert=True))
    result = col.bulk_write(ops, ordered=False)
    log.info(f"  MongoDB [{collection_name}]: {result.upserted_count} inserted, "
             f"{result.matched_count} updated/existing")


# ─── JSON fallback ────────────────────────────────────────────────────────────

_json_buffer: dict = {}

def save_to_json_buffer(collection_name: str, docs: list):
    _json_buffer.setdefault(collection_name, []).extend(docs)


def _dedupe(records: list) -> list:
    seen, out = set(), []
    for d in records:
        key = d.get("uid")
        if key in seen:
            continue
        seen.add(key)
        out.append(d)
    return out


def flush_json():
    if not _json_buffer:
        return
    existing = {}
    if Path(JSON_FILE).exists():
        with open(JSON_FILE, encoding="utf-8") as f:
            existing = json.load(f)
    for k, v in _json_buffer.items():
        existing[k] = _dedupe(existing.get(k, []) + v)
    with open(JSON_FILE, "w", encoding="utf-8") as f:
        json.dump(existing, f, ensure_ascii=False, indent=2)
    total = sum(len(v) for v in existing.values())
    log.info(f"JSON saved → {JSON_FILE}  (total {total:,} records)")


def store(collection_name: str, docs: list):
    if not docs:
        return
    upsert_to_mongo(collection_name, docs)
    save_to_json_buffer(collection_name, docs)


# ─── Parsers ──────────────────────────────────────────────────────────────────

def _abs_url(href: str) -> str:
    if href and not href.startswith("http"):
        return BASE_URL + "/" + href.lstrip("/")
    return href


def _row_to_docs(row, judgment_type: str) -> list:
    """
    Parse a <tr> with 5 cells: [0] S.No [1] Coram [2] Party [3] Date+Side
    [4] CaseNo + PDF link(s). Emits ONE document per PDF link (rows normally have
    one); a row with no PDF yields a single metadata-only document.
    NOTE: pdf_url is a volatile token, valid only briefly after this fetch.
    """
    cells = row.css("td, th")
    if len(cells) < 5:
        return []

    coram = cells[1].get_all_text(strip=True).replace("\n", " | ")
    party = cells[2].get_all_text(strip=True).replace("\n", " ").strip()

    dt_raw = cells[3].get_all_text(strip=True)
    date_m = re.search(r"(\d{2}/\d{2}/\d{4})", dt_raw)
    side_m = re.search(r"\(([^)]+)\)",         dt_raw)
    date_str = date_m.group(1) if date_m else ""
    side     = side_m.group(1) if side_m else ""

    case_text = cells[4].get_all_text(strip=True)
    case_no   = case_text.split()[0] if case_text else ""

    if not (coram or case_no):
        return []

    hrefs = [
        _abs_url(h) for h in row.css("a::attr(href)").getall()
        if h and ("file/download" in h or h.lower().endswith(".pdf"))
    ]
    seen, pdf_urls = set(), []
    for h in hrefs:
        if h not in seen:
            seen.add(h)
            pdf_urls.append(h)

    base = {
        "judgment_type": judgment_type,
        "coram":         coram,
        "party":         party,
        "date":          date_str,
        "side":          side,
        "case_no":       case_no,
        "scraped_at":    datetime.now(timezone.utc).isoformat(),
    }
    if not pdf_urls:
        return [{**base, "pdf_url": None}]
    return [{**base, "pdf_url": u} for u in pdf_urls]


def assign_uids(docs: list, collection: str) -> list:
    """
    Assign a stable `seq` (occurrence among docs sharing identical metadata, in
    listing order) and `uid` (hash of collection + metadata + seq). Independent
    of the volatile pdf_url, so it is stable across runs.
    """
    counts: dict = {}
    for d in docs:
        mkey = (d["case_no"], d["date"], d["side"], d["party"], d["coram"])
        seq = counts.get(mkey, 0)
        counts[mkey] = seq + 1
        raw = f"{collection}|{d['case_no']}|{d['date']}|{d['side']}|{d['party']}|{d['coram']}|{seq}"
        d["seq"] = seq
        d["uid"] = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]
    return docs


def parse_page_html(page_html: str, judgment_type: str, collection: str) -> list:
    """Parse the table fragment from a JSON {"page": ...} envelope into docs."""
    if not page_html:
        return []
    sel = Selector(page_html)
    docs = []
    for row in sel.css("table tbody tr"):
        docs.extend(_row_to_docs(row, judgment_type))
    return assign_uids(docs, collection)


# ─── HTTP helpers (Scrapling) ────────────────────────────────────────────────

def get_json(session, url, method="GET", data=None, retries=3):
    """Call an AJAX endpoint and return its parsed JSON dict, or None."""
    for attempt in range(1, retries + 1):
        try:
            resp = session.post(url, data=data) if method == "POST" else session.get(url)
            if resp.status != 200:
                raise RuntimeError(f"HTTP {resp.status}")
            return resp.json()
        except Exception as e:
            log.warning(f"  Attempt {attempt}/{retries} failed for {url}: {e}")
            if attempt < retries:
                time.sleep(attempt * 4)
    return None


def get_csrf_and_secret(main_page):
    token  = main_page.css('#getJudByAdvocateName input[name="_token"]::attr(value)').get()
    secret = main_page.css('#getJudByAdvocateName input[name="form_secret"]::attr(value)').get()
    return token, secret


# ─── S3 upload helpers ────────────────────────────────────────────────────────

import re as _re
_SAFE = _re.compile(r"[^A-Za-z0-9._-]+")
_acl_unsupported = False
_processed = 0  # PDFs handled this run (for LIMIT)


def make_s3_client():
    kwargs = {"region_name": AWS_REGION}
    if S3_ENDPOINT_URL:
        kwargs["endpoint_url"] = S3_ENDPOINT_URL
    return boto3.client("s3", **kwargs)


def s3_key_for(collection: str, doc: dict) -> str:
    case = _SAFE.sub("_", (doc.get("case_no") or "unknown")).strip("_") or "unknown"
    date = (doc.get("date") or "nodate").replace("/", "-")
    return f"{S3_PREFIX}/{collection}/{case}_{date}_{doc.get('seq', 0)}_{doc['uid']}.pdf"


def public_url_for(key: str) -> str:
    if S3_ENDPOINT_URL:
        return f"{S3_ENDPOINT_URL.rstrip('/')}/{S3_BUCKET}/{key}"
    if AWS_REGION == "us-east-1":
        return f"https://{S3_BUCKET}.s3.amazonaws.com/{key}"
    return f"https://{S3_BUCKET}.s3.{AWS_REGION}.amazonaws.com/{key}"


def s3_object_exists(s3, key: str) -> bool:
    try:
        s3.head_object(Bucket=S3_BUCKET, Key=key)
        return True
    except ClientError as e:
        if e.response["ResponseMetadata"]["HTTPStatusCode"] == 404:
            return False
        raise


def download_pdf(session, url: str):
    try:
        resp = session.get(url)
    except Exception as e:
        log.warning(f"    download error: {e}")
        return None
    if resp.status != 200:
        log.warning(f"    download HTTP {resp.status}")
        return None
    body = resp.body
    if not body or not body.startswith(b"%PDF"):
        log.warning(f"    not a PDF (got {(body or b'')[:16]!r})")
        return None
    return body


def upload_one(session, s3, db, collection: str, doc: dict, stats: dict):
    """Download a judgment PDF and upload to S3; annotate doc with S3 links."""
    global _acl_unsupported
    uid = doc["uid"]
    tag = doc.get("case_no") or uid

    if doc.get("pdf_url") is None:
        stats["no_pdf"] += 1
        return "no_pdf"

    # Resume: skip if already uploaded on a prior run.
    if db is not None:
        prev = db[collection].find_one({"uid": uid, "s3_key": {"$exists": True}})
        if prev is not None:
            doc.update({k: prev[k] for k in S3_FIELDS if k in prev})
            stats["skipped"] += 1
            return "skipped"

    key = s3_key_for(collection, doc)
    if s3 is not None and s3_object_exists(s3, key):
        doc.update({"s3_bucket": S3_BUCKET, "s3_key": key,
                    "s3_uri": f"s3://{S3_BUCKET}/{key}", "public_url": public_url_for(key),
                    "uploaded_at": datetime.now(timezone.utc).isoformat()})
        stats["skipped"] += 1
        log.info(f"  = {tag}: already in S3")
        return "skipped"

    body = download_pdf(session, doc["pdf_url"])
    if body is None:
        stats["failed"] += 1
        return "failed"

    if DRY_RUN:
        log.info(f"  ~ {tag}: {len(body):,} bytes → would upload s3://{S3_BUCKET}/{key}")
        stats["uploaded"] += 1
        return "uploaded"

    put_kwargs = dict(Bucket=S3_BUCKET, Key=key, Body=body, ContentType="application/pdf",
                      Metadata={"uid": uid, "case_no": str(doc.get("case_no", ""))[:1024],
                                "judgment_type": str(doc.get("judgment_type", ""))[:1024],
                                "judgment_date": str(doc.get("date", ""))[:64]})
    if S3_ACL and not _acl_unsupported:
        put_kwargs["ACL"] = S3_ACL
    try:
        s3.put_object(**put_kwargs)
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") in ("AccessControlListNotSupported", "InvalidRequest") and "ACL" in put_kwargs:
            _acl_unsupported = True
            put_kwargs.pop("ACL")
            log.warning("  ACLs disabled on bucket; uploading without ACL (use a bucket policy for public access).")
            try:
                s3.put_object(**put_kwargs)
            except Exception as e2:
                stats["failed"] += 1; log.warning(f"  ! {tag}: upload failed: {e2}"); return "failed"
        else:
            stats["failed"] += 1; log.warning(f"  ! {tag}: upload failed: {e}"); return "failed"
    except Exception as e:
        stats["failed"] += 1; log.warning(f"  ! {tag}: upload failed: {e}"); return "failed"

    doc.update({"s3_bucket": S3_BUCKET, "s3_key": key,
                "s3_uri": f"s3://{S3_BUCKET}/{key}", "public_url": public_url_for(key),
                "pdf_size": len(body), "uploaded_at": datetime.now(timezone.utc).isoformat()})
    stats["uploaded"] += 1
    log.info(f"  ↑ {tag}: {len(body):,} bytes → {doc['public_url']}")
    return "uploaded"


def _save_doc(collection: str, doc: dict):
    """Upsert ONE doc to Mongo immediately (so rows appear live) + buffer JSON."""
    db = get_db()
    if db is not None:
        s3f = {k: doc[k] for k in S3_FIELDS if k in doc}
        meta = {k: v for k, v in doc.items() if k not in s3f}
        update = {"$setOnInsert": meta}
        if s3f:
            update["$set"] = s3f
        try:
            db[collection].update_one({"uid": doc["uid"]}, update, upsert=True)
        except Exception as e:
            log.warning(f"  Mongo save failed for {doc.get('uid')}: {e}")
    save_to_json_buffer(collection, [doc])


def upload_docs(session, s3, db, collection: str, docs: list, stats: dict) -> bool:
    """Upload every PDF in docs, saving each to Mongo right after. Returns True if LIMIT hit."""
    global _processed
    for doc in docs:
        if LIMIT and _processed >= LIMIT:
            return True
        _processed += 1
        status = upload_one(session, s3, db, collection, doc, stats)
        _save_doc(collection, doc)            # persist immediately, live
        if status == "uploaded" and not DRY_RUN:
            sleep_politely()                  # polite delay only after a real download
    return False


def month_windows(start: date, end: date):
    """Yield (from_str, to_str) monthly windows covering [start, end]."""
    current = start
    while current <= end:
        nxt = (current.replace(day=28) + timedelta(days=4)).replace(day=1)
        window_end = min(nxt - timedelta(days=1), end)
        yield current.strftime("%d-%m-%Y"), window_end.strftime("%d-%m-%Y")
        current = nxt


def _total_from_results_info(payload) -> int:
    info = (payload or {}).get("results_info") or ""
    m = re.search(r"of\s+([\d,]+)", info)
    return int(m.group(1).replace(",", "")) if m else 0


# ─── Listing iterator (shared by metadata scraper and S3 pipeline) ────────────

def iter_listings(session):
    """
    Yield (collection, judgment_type, docs) for every listing, using `session`
    so any pdf_url in `docs` is fresh and downloadable right away.

    The Rept. date-range windows are yielded FIRST so this date-driven tab is
    never starved by the large static tabs (F.B. Orders alone has ~1,700 PDFs)
    or cut off mid-run by LIMIT. The three single-GET static tabs follow.
    """
    # ── Rept. Judgment/Order: POST per monthly window, paginate ──
    log.info("=" * 55)
    log.info("Scraping: Rept. Judgment/Order (server-side pagination)")
    main_page = session.get(JUDGMENTS_URL)
    token, secret = get_csrf_and_secret(main_page)
    if not token:
        log.error("No CSRF token found – cannot scrape Rept. Judgments")
        return

    start = datetime.strptime(START_DATE, "%d-%m-%Y").date()
    end   = datetime.strptime(END_DATE,   "%d-%m-%Y").date()

    for from_str, to_str in month_windows(start, end):
        log.info(f"  Window: {from_str} -> {to_str}")
        payload = {"_token": token, "form_secret": secret,
                   "repfrmdate": from_str, "reptodate": to_str, "search": ""}
        page = 1
        window_docs = []
        total_records = None
        while True:
            url = ADVOCATENAME_URL if page == 1 else f"{ADVOCATENAME_URL}?page={page}"
            data = get_json(session, url, method="POST", data=payload)
            if data is None:
                log.warning(f"  POST failed for {from_str}->{to_str} (page {page})")
                break
            if data.get("status") is False:
                if data.get("error"):
                    log.info(f"    {data['error']}")
                break
            if total_records is None:
                total_records = _total_from_results_info(data)
                log.info(f"    {total_records} results in window")
            docs = parse_page_html(data.get("page", ""), "Rept. Judgment/Order",
                                   REPT_COLLECTION)
            if not docs:
                break
            window_docs.extend(docs)
            log.info(f"    Page {page}: {len(docs)} records (window total: {len(window_docs)})")
            if total_records and len(window_docs) >= total_records:
                break
            page += 1
            sleep_politely()

        # Re-assign uids across the whole window so seq is stable even when
        # identical-metadata duplicates straddle a page boundary.
        window_docs = assign_uids(window_docs, REPT_COLLECTION)
        yield REPT_COLLECTION, "Rept. Judgment/Order", window_docs
        sleep_politely()

    if REPT_ONLY:
        log.info("REPT_ONLY set — skipping static tabs "
                 "(F.B. Judgment, D.B. Ref., F.B. Orders).")
        return

    # ── Static tabs: one GET each ──
    for url, collection, jtype in STATIC_TABS:
        payload = get_json(session, url, method="GET")
        if payload is None:
            log.warning(f"[{jtype}]  request failed")
            continue
        docs = parse_page_html(payload.get("page", ""), jtype, collection)
        log.info(f"[{jtype}]  {len(docs):,} records found")
        yield collection, jtype, docs
        sleep_politely()


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    uploading = UPLOAD_TO_S3 or DRY_RUN
    mode = "metadata + PDFs → S3" if uploading else "metadata only"
    log.info("=" * 55)
    log.info(f"BHC Judgment Scraper  ─  Scrapling edition ({mode})")
    log.info(f"Date range (Rept.): {START_DATE} -> {END_DATE}")
    log.info("=" * 55)

    db = get_db()
    log.info("Storage: MongoDB + JSON mirror" if db is not None
             else "Storage: JSON only (MongoDB unavailable)")

    # ── Set up S3 if uploading ──
    s3 = None
    if UPLOAD_TO_S3 and not DRY_RUN:
        if not BOTO3_AVAILABLE:
            log.error("UPLOAD_TO_S3 set but boto3 is not installed (pip install boto3).")
            return
        if not S3_BUCKET:
            log.error("UPLOAD_TO_S3 set but S3_BUCKET is empty.")
            return
        s3 = make_s3_client()
        try:
            s3.head_bucket(Bucket=S3_BUCKET)
            log.info(f"S3 bucket reachable: {S3_BUCKET}  (prefix: {S3_PREFIX})")
        except ClientError as e:
            log.error(f"Cannot access bucket '{S3_BUCKET}': {e}")
            return
    elif DRY_RUN:
        log.info("DRY RUN — PDFs downloaded but NOT uploaded.")

    stats = {"uploaded": 0, "skipped": 0, "failed": 0, "no_pdf": 0}
    total = 0
    with FetcherSession(impersonate="chrome", timeout=60) as session:
        log.info("Fetching main judgments page...")
        if session.get(JUDGMENTS_URL).status != 200:
            log.error("Cannot reach BHC website.")
            return
        for collection, _jtype, docs in iter_listings(session):
            if uploading:
                # upload_docs persists each doc to Mongo + JSON as it goes.
                stop = upload_docs(session, s3, db, collection, docs, stats)
            else:
                store(collection, docs)
                stop = False
            total += len(docs)
            if stop:
                log.info(f"Reached LIMIT={LIMIT}; stopping.")
                break

    flush_json()

    log.info("=" * 55)
    log.info(f"DONE.  {total:,} records this run. Summary:")
    if uploading:
        log.info(f"  PDFs: uploaded={stats['uploaded']}  already-done={stats['skipped']}  "
                 f"download-failed={stats['failed']}  no-pdf={stats['no_pdf']}")
    db = get_db()
    if db is not None:
        for col in ALL_COLLECTIONS:
            log.info(f"  MongoDB [{col}]:  {db[col].count_documents({}):,} documents")
    log.info("=" * 55)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.info("Interrupted – flushing JSON before exit")
        flush_json()
