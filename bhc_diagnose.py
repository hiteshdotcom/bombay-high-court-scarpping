"""
BHC Website Diagnostic Tool
============================
Run this FIRST before the main scraper.
It fetches the judgments page, extracts form details, and identifies
the correct endpoint and field names so you can adjust bhc_scraper.py.

Usage:
    python bhc_diagnose.py
"""

import json
import requests
from bs4 import BeautifulSoup

BASE_URL      = "https://bombayhighcourt.gov.in/bhc"
JUDGMENTS_URL = f"{BASE_URL}/judgments"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

session = requests.Session()
session.headers.update(HEADERS)


def diagnose():
    print("=" * 60)
    print("BHC Judgment Page Diagnostic")
    print("=" * 60)

    # 1. Fetch the page
    print(f"\n1. Fetching: {JUDGMENTS_URL}")
    try:
        resp = session.get(JUDGMENTS_URL, timeout=30)
        resp.raise_for_status()
        print(f"   Status: {resp.status_code}")
        print(f"   Content length: {len(resp.text):,} chars")
    except Exception as e:
        print(f"   FAILED: {e}")
        return

    soup = BeautifulSoup(resp.text, "lxml")

    # 2. CSRF token
    print("\n2. CSRF / Auth tokens:")
    meta_csrf = soup.find("meta", {"name": "csrf-token"})
    print(f"   meta[csrf-token]: {meta_csrf['content'][:40] if meta_csrf else 'NOT FOUND'}")

    for inp in soup.find_all("input", {"type": "hidden"}):
        print(f"   hidden input name={inp.get('name')} value={str(inp.get('value',''))[:40]}")

    # 3. All forms
    print("\n3. Forms found:")
    forms = soup.find_all("form")
    print(f"   Total forms: {len(forms)}")
    for i, form in enumerate(forms):
        action = form.get("action", "(no action)")
        method = form.get("method", "GET").upper()
        fields = [(inp.get("name"), inp.get("type"), inp.get("value", ""))
                  for inp in form.find_all("input")]
        selects = [(sel.get("name"), [opt.get("value") for opt in sel.find_all("option")][:5])
                   for sel in form.find_all("select")]
        print(f"\n   Form {i+1}:")
        print(f"     Action: {action}")
        print(f"     Method: {method}")
        print(f"     Inputs: {fields}")
        print(f"     Selects: {selects}")

    # 4. Script tags (look for AJAX endpoints)
    print("\n4. JavaScript URLs (potential AJAX endpoints):")
    import re
    js_urls = set()
    for script in soup.find_all("script"):
        text = script.get_text()
        # Look for URL patterns
        urls = re.findall(r"""(?:url|action|endpoint)\s*[:=]\s*['"]([^'"]+)['"]""", text, re.IGNORECASE)
        for u in urls:
            if "judgment" in u.lower() or "search" in u.lower() or "order" in u.lower():
                js_urls.add(u)
        # Also look for fetch/axios/ajax calls
        fetches = re.findall(r"""fetch\(['"]([^'"]+)['"]""", text)
        js_urls.update(fetches)

    for u in sorted(js_urls):
        print(f"   {u}")
    if not js_urls:
        print("   None found (results may be loaded client-side by JS after page render)")

    # 5. External JS files
    print("\n5. External JavaScript files (may contain endpoints):")
    for script in soup.find_all("script", src=True):
        src = script["src"]
        if not any(skip in src for skip in ["jquery", "bootstrap", "cdn", "google"]):
            print(f"   {src}")

    # 6. Test POST to likely endpoints
    print("\n6. Testing likely POST endpoints with sample date range...")
    csrf = meta_csrf["content"] if meta_csrf else ""

    candidates = [
        f"{BASE_URL}/judgments/search",
        f"{BASE_URL}/judgments/order-judgment",
        f"{BASE_URL}/judgments",
        f"{BASE_URL}/front/judgments",
    ]

    test_payloads = [
        {"_token": csrf, "from_date": "01-01-2023", "to_date": "31-12-2023", "type": "All"},
        {"_token": csrf, "fromdate": "01-01-2023", "todate": "31-12-2023"},
        {"_token": csrf, "from": "01-01-2023", "to": "31-12-2023"},
    ]

    for url in candidates:
        for payload in test_payloads:
            try:
                r = session.post(url, data=payload, timeout=15)
                print(f"   POST {url} → {r.status_code} ({len(r.text):,} chars) payload_keys={list(payload.keys())}")
                if r.status_code == 200 and len(r.text) > 1000:
                    s = BeautifulSoup(r.text, "lxml")
                    snippet = s.get_text(strip=True)[:200]
                    print(f"      Preview: {snippet}")
                    # Save for inspection
                    with open(f"probe_{url.split('/')[-1]}_{list(payload.keys())[1]}.html", "w") as f:
                        f.write(r.text)
                    print(f"      Saved response HTML for inspection")
                break
            except Exception as e:
                print(f"   POST {url} → ERROR: {e}")

    print("\n" + "=" * 60)
    print("NEXT STEPS:")
    print("  1. Look at the form actions and field names above")
    print("  2. Check any saved probe_*.html files to see what a response looks like")
    print("  3. Update bhc_scraper.py's payload and form_endpoint to match")
    print("  4. Then run: python bhc_scraper.py")
    print("=" * 60)


if __name__ == "__main__":
    diagnose()
