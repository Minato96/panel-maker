#!/usr/bin/env python3

import pandas as pd
import json
import re
from datetime import datetime
from tqdm import tqdm
from urllib.parse import urlparse

# ================= CONFIG =================

DIRECTORY_CSV = "new_directory.csv"
MISSING_INPUT = "missing_urls.csv"
THIRD_CSV = "ai_wayback_async_out_2024.csv"

OUTPUT_DIRECTORY = "new_directory.csv"      # overwrite safely
MISSING_OUTPUT = "missing_urls_pass2.csv"

CHUNKSIZE = 200_000

WAYBACK_RE = re.compile(r"https://web\.archive\.org/web/(\d{14})/(https://.+)")

# ================= HELPERS =================

def canonical_tool_url(url):
    if not isinstance(url, str):
        return None

    url = url.strip().lower()
    url = url.replace("http://", "https://")

    parsed = urlparse(url)
    path = parsed.path.rstrip("/")

    return f"https://{parsed.netloc}{path}"



def extract_wayback_info(url):
    if not isinstance(url, str):
        return None, None

    m = WAYBACK_RE.match(url)
    if not m:
        return None, None

    ts = m.group(1)
    original = canonical_tool_url(m.group(2))

    if not original:
        return None, None

    date = datetime.strptime(ts[:8], "%Y%m%d").date()
    return original, date


def safe_json_load(x):
    if pd.isna(x):
        return []
    try:
        return json.loads(x)
    except Exception:
        return []


def get_release_date(versions):
    dates = []
    for v in safe_json_load(versions):
        if "date" in v:
            try:
                dates.append(datetime.strptime(v["date"], "%Y-%m-%d").date())
            except Exception:
                pass
    return min(dates) if dates else None


# ================= LOAD BASE =================

directory_df = pd.read_csv(DIRECTORY_CSV)
missing_df = pd.read_csv(MISSING_INPUT)

missing_urls = set(
    canonical_tool_url(u)
    for u in missing_df["url"]
    if isinstance(u, str)
)


# index for fast updates
directory_df["tool_id"] = directory_df["tool_id"].apply(canonical_tool_url)
directory_df.set_index("tool_id", inplace=True)


found = {}
still_missing = set(missing_urls)

# ================= STREAM THIRD CSV =================

for chunk in tqdm(
    pd.read_csv(THIRD_CSV, chunksize=CHUNKSIZE, low_memory=False),
    desc="Scanning 3rd CSV"
):
    for _, row in chunk.iterrows():
        original, snap_date = extract_wayback_info(row.get("link"))
        if not original or original not in still_missing:
            continue

        # only keep latest snapshot
        if original in found and found[original]["snapshot_date"] >= snap_date:
            continue

        if original in still_missing and len(found) < 5:
            print("MATCH:", original)

        found[original] = {
            "row": row,
            "snapshot_date": snap_date
        }

# ================= APPLY UPDATES =================

for url, data in found.items():
    row = data["row"]

    directory_df.loc[url, "name"] = row.get("name")
    directory_df.loc[url, "release_date"] = get_release_date(row.get("versions"))
    directory_df.loc[url, "pricing_text"] = row.get("pricing_model")
    directory_df.loc[url, "description"] = row.get("description")
    directory_df.loc[url, "description_length"] = (
        len(row.get("description")) if isinstance(row.get("description"), str) else None
    )
    directory_df.loc[url, "saves"] = row.get("saves")
    directory_df.loc[url, "comments"] = row.get("comments_json")
    directory_df.loc[url, "comments_count"] = row.get("comments_count")
    directory_df.loc[url, "rating"] = row.get("rating")
    directory_df.loc[url, "ratings_count"] = row.get("number_of_ratings")
    directory_df.loc[url, "tasks"] = row.get("task_label_name")
    directory_df.loc[url, "last_date"] = data["snapshot_date"].isoformat()

    still_missing.discard(url)

# ================= WRITE OUTPUT =================

directory_df.reset_index().to_csv(OUTPUT_DIRECTORY, index=False)
pd.DataFrame({"url": sorted(still_missing)}).to_csv(MISSING_OUTPUT, index=False)

print("✅ Pass 2 complete")
print(f"Recovered: {len(found)}")
print(f"Still missing: {len(still_missing)}")
