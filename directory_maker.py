#!/usr/bin/env python3

import pandas as pd
import json
import re
from pathlib import Path
from datetime import datetime
from tqdm import tqdm

# ================= CONFIG =================

INPUT_URLS_CSV = "clean_urls_3.csv"

PRIMARY_CSV = "ai_tools_progress_14012026.csv"
SECONDARY_CSV = "ai_wayback_async_out_2025.csv"

OUTPUT_CSV = "new_directory.csv"
MISSING_CSV = "missing_urls.csv"

CURRENT_DATA_DATE = "2026-01-14"

# ================= HELPERS =================

WAYBACK_RE = re.compile(r"https://web\.archive\.org/web/(\d{14})/(https://.+)")

def extract_wayback_info(url):
    """
    Returns (original_url, snapshot_date) or (None, None)
    """
    if not isinstance(url, str):
        return None, None

    m = WAYBACK_RE.match(url)
    if not m:
        return None, None

    ts = m.group(1)
    original = m.group(2)

    date = datetime.strptime(ts[:8], "%Y%m%d").date()
    return original.rstrip("/"), date


def safe_json_load(x):
    if pd.isna(x):
        return []
    try:
        return json.loads(x)
    except Exception:
        return []


def get_release_date(versions_json):
    """
    versions: list of {version, date, changelog}
    → earliest date
    """
    dates = []
    for v in safe_json_load(versions_json):
        if "date" in v:
            try:
                dates.append(datetime.strptime(v["date"], "%Y-%m-%d").date())
            except Exception:
                pass
    return min(dates) if dates else None


def build_pricing_text(row):
    parts = []
    for col in ["pricing_model", "paid_options_from", "billing_frequency"]:
        val = row.get(col)
        if pd.notna(val) and str(val).strip():
            parts.append(str(val).strip())
    return " | ".join(parts) if parts else None


# ================= LOAD DATA =================

urls_df = pd.read_csv(INPUT_URLS_CSV)
primary_df = pd.read_csv(PRIMARY_CSV)
secondary_df = pd.read_csv(SECONDARY_CSV)

# Normalize URLs
urls_df["url"] = urls_df["url"].str.rstrip("/")
primary_df["link"] = primary_df["link"].str.rstrip("/")

# ================= INDEX PRIMARY =================

primary_map = primary_df.set_index("link")

# ================= INDEX SECONDARY (WAYBACK) =================

records = []

for _, row in secondary_df.iterrows():
    original, snap_date = extract_wayback_info(row["link"])
    if not original:
        continue

    records.append({
        "original": original,
        "snapshot_date": snap_date,
        "row": row
    })

secondary_grouped = {}

for r in records:
    secondary_grouped.setdefault(r["original"], []).append(r)

# ================= MAIN LOOP =================

output_rows = []
missing = []

for url in tqdm(urls_df["url"], desc="Processing tools"):

    row = None
    last_date = None

    # ---- PRIMARY ----
    if url in primary_map.index:
        row = primary_map.loc[url]
        last_date = CURRENT_DATA_DATE

    # ---- SECONDARY ----
    elif url in secondary_grouped:
        candidates = sorted(
            secondary_grouped[url],
            key=lambda x: x["snapshot_date"],
            reverse=True
        )
        best = candidates[0]
        row = best["row"]
        last_date = best["snapshot_date"].isoformat()

    # ---- NOT FOUND ----
    if row is None:
        missing.append({"url": url})
        output_rows.append({
            "tool_id": url,
            **{k: None for k in [
                "name", "release_date", "pricing_text", "description",
                "description_length", "saves", "comments", "comments_count",
                "views", "rating", "ratings_count",
                "input_modalities", "output_modalities",
                "tasks", "last_date"
            ]}
        })
        continue

    # ---- BUILD ROW ----
    release_date = get_release_date(row.get("versions"))
    description = row.get("description")

    output_rows.append({
        "tool_id": url,
        "name": row.get("name"),
        "release_date": release_date,
        "pricing_text": build_pricing_text(row),
        "description": description,
        "description_length": len(description) if isinstance(description, str) else None,
        "saves": row.get("saves"),
        "comments": row.get("comments"),
        "comments_count": row.get("comments_count"),
        "views": row.get("views"),
        "rating": row.get("rating"),
        "ratings_count": row.get("number_of_ratings"),
        "input_modalities": row.get("modalities_inputs"),
        "output_modalities": row.get("modalities_outputs"),
        "tasks": row.get("task_label_name"),
        "last_date": last_date
    })

# ================= WRITE OUTPUT =================

pd.DataFrame(output_rows).to_csv(OUTPUT_CSV, index=False)
pd.DataFrame(missing).to_csv(MISSING_CSV, index=False)

print("✅ Done")
print(f"Final rows: {len(output_rows)}")
print(f"Missing URLs: {len(missing)}")
