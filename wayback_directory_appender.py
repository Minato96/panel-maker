#!/usr/bin/env python3

import pandas as pd
import requests
import time
from bs4 import BeautifulSoup
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# ================= CONFIG =================

INPUT_CSV = "new_directory.csv"
OUTPUT_CSV = "new_directory.csv"

CDX_API = "https://web.archive.org/cdx/search/cdx"
HEADERS = {"User-Agent": "DirectoryBot/FINAL"}

LIVE_WORKERS = 10
WAYBACK_WORKERS = 4

LIVE_TIMEOUT = 20
WAYBACK_TIMEOUT = 30

# ================= HELPERS =================

def fetch(url, timeout, retries=3):
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=HEADERS, timeout=timeout)
            if r.status_code == 200:
                return r.text
        except requests.exceptions.RequestException:
            pass
        time.sleep(1 + attempt)
    return None



def extract_name_release(html, snapshot_year=None):
    soup = BeautifulSoup(html, "lxml")

    # ---- NAME ----
    name = None
    h1 = soup.select_one("h1.title_inner")
    if h1:
        name = h1.get_text(" ", strip=True).split(" v")[0]

    # ---- RELEASE DATE ----
    dates = []

    if snapshot_year is None or snapshot_year >= 2025:
        for d in soup.select(".version .changelog_title"):
            try:
                dates.append(datetime.strptime(d.text.strip(), "%B %d, %Y").date())
            except:
                pass
    else:
        for d in soup.select("span.launch_date_top"):
            txt = d.text.strip()
            for fmt in ("%Y-%m-%d", "%d %b %Y"):
                try:
                    dates.append(datetime.strptime(txt, fmt).date())
                    break
                except:
                    pass

    release = min(dates).isoformat() if dates else None
    return name, release


def latest_wayback_snapshot(url):
    params = {
        "url": url,
        "output": "json",
        "filter": "statuscode:200",
        "filter": "mimetype:text/html",
        "fl": "timestamp",
        "limit": 1,
        "sort": "reverse",
    }

    r = requests.get(CDX_API, params=params, timeout=30)
    data = r.json()

    if len(data) <= 1:
        return None, None

    ts = data[1][0]
    year = int(ts[:4])
    snap_url = f"https://web.archive.org/web/{ts}/{url}"

    return snap_url, year


# ================= WORKERS =================

def process_live(tool_id):
    try:
        html = fetch(tool_id, LIVE_TIMEOUT)
        if not html:
            return tool_id, None, None, None

        name, release = extract_name_release(html)
        return tool_id, name, release, datetime.utcnow().date().isoformat()

    except Exception:
        return tool_id, None, None, None



def process_wayback(tool_id):
    try:
        snap_url, year = latest_wayback_snapshot(tool_id)
        if not snap_url:
            return tool_id, "0", None, None

        html = fetch(snap_url, WAYBACK_TIMEOUT)
        if not html:
            return tool_id, None, None, None

        name, release = extract_name_release(html, year)
        ts = snap_url.split("/web/")[1][:8]
        last = f"{ts[:4]}-{ts[4:6]}-{ts[6:8]}"

        return tool_id, name, release, last

    except Exception:
        return tool_id, None, None, None

# ================= MAIN =================

df = pd.read_csv(INPUT_CSV)
df["name"] = df["name"].replace("", pd.NA)
df.set_index("tool_id", inplace=True)

assert df.index.is_unique, "tool_id index is not unique"


targets = df[df["name"].isna() | df["release_date"].isna()]

live_ids = targets[targets["exited"] == 0].index.tolist()
wayback_ids = targets[targets["exited"] == 1].index.tolist()

results = {}

print(f"LIVE: {len(live_ids)} | WAYBACK: {len(wayback_ids)}")

# ---- LIVE ----
with ThreadPoolExecutor(max_workers=LIVE_WORKERS) as ex:
    for f in tqdm(as_completed([ex.submit(process_live, u) for u in live_ids]),
                  total=len(live_ids), desc="LIVE"):
        tid, name, release, last = f.result()
        results[tid] = (name, release, last)

# ---- WAYBACK ----
with ThreadPoolExecutor(max_workers=WAYBACK_WORKERS) as ex:
    for f in tqdm(as_completed([ex.submit(process_wayback, u) for u in wayback_ids]),
                  total=len(wayback_ids), desc="WAYBACK"):
        tid, name, release, last = f.result()
        results[tid] = (name, release, last)

# ---- APPLY ----
for tid, (name, release, last) in results.items():
    current_name = df.loc[tid, "name"]

    if name is not None and (
        pd.isna(current_name).all()
        if isinstance(current_name, pd.Series)
        else pd.isna(current_name)
    ):
        df.loc[tid, "name"] = name

    current_release = df.loc[tid, "release_date"]

    if release is not None and (
        pd.isna(current_release).all()
        if isinstance(current_release, pd.Series)
        else pd.isna(current_release)
    ):
        df.loc[tid, "release_date"] = release


df.reset_index().to_csv(OUTPUT_CSV, index=False)

print("✅ DONE")
