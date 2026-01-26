#!/usr/bin/env python3

import pandas as pd
import json
import re
from datetime import datetime
from tqdm import tqdm

# ================= CONFIG =================

DIRECTORY_CSV = "new_directory.csv"
LIVE_CSV = "ai_tools_missed.csv"

PRIMARY_CSV = "ai_tools_missed.csv"
SECONDARY_CSV = "ai_tools_missed.csv"

CURRENT_DATA_DATE = "2026-01-26"

# ================= HELPERS =================

WAYBACK_RE = re.compile(r"https://web\.archive\.org/web/(\d{14})/(https://.+)")

def norm(u):
    if not isinstance(u, str):
        return None
    return u.rstrip("/")

def extract_wayback_info(url):
    if not isinstance(url, str):
        return None, None

    m = WAYBACK_RE.match(url)
    if not m:
        return None, None

    ts = m.group(1)
    original = m.group(2).rstrip("/")
    date = datetime.strptime(ts[:8], "%Y%m%d").date()
    return original, date

def safe_json_load(x):
    if pd.isna(x):
        return []
    try:
        return json.loads(x)
    except Exception:
        return []

def get_release_date(versions_json):
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

# ================= LOAD =================

dir_df = pd.read_csv(DIRECTORY_CSV)
live_df = pd.read_csv(LIVE_CSV)

primary_df = pd.read_csv(PRIMARY_CSV)
secondary_df = pd.read_csv(SECONDARY_CSV)

# normalize
dir_df["tool_id"] = dir_df["tool_id"].apply(norm)
live_df["link"] = live_df["link"].apply(norm)
primary_df["link"] = primary_df["link"].apply(norm)

dir_df.set_index("tool_id", inplace=True)
primary_map = primary_df.set_index("link")

# ================= PREP SECONDARY =================

records = {}

for _, row in secondary_df.iterrows():
    original, snap_date = extract_wayback_info(row.get("link"))
    if not original:
        continue

    if original not in records or records[original]["snapshot_date"] < snap_date:
        records[original] = {
            "row": row,
            "snapshot_date": snap_date
        }

# ================= PROCESS LIVE CSV =================

for _, row in tqdm(live_df.iterrows(), total=len(live_df), desc="Updating directory"):
    url = row.get("link")
    name = row.get("name")

    if url not in dir_df.index:
        continue

    # ---- CASE 1: DEAD ----
    if not isinstance(name, str) or not name.strip():
        dir_df.loc[url, "exited"] = 1
        continue

    # ---- CASE 2: LIVE → UPDATE DATA ----
    data_row = None
    last_date = None

    if url in primary_map.index:
        data_row = primary_map.loc[url]
        last_date = CURRENT_DATA_DATE
    elif url in records:
        data_row = records[url]["row"]
        last_date = records[url]["snapshot_date"].isoformat()

    if data_row is None:
        continue

    # update fields in-place
    dir_df.loc[url, "name"] = data_row.get("name")
    dir_df.loc[url, "release_date"] = get_release_date(data_row.get("versions"))
    dir_df.loc[url, "pricing_text"] = build_pricing_text(data_row)
    dir_df.loc[url, "description"] = data_row.get("description")
    dir_df.loc[url, "description_length"] = (
        len(data_row.get("description"))
        if isinstance(data_row.get("description"), str)
        else None
    )
    dir_df.loc[url, "saves"] = data_row.get("saves")
    dir_df.loc[url, "comments"] = data_row.get("comments_json")
    dir_df.loc[url, "comments_count"] = data_row.get("comments_count")
    dir_df.loc[url, "views"] = data_row.get("views")
    dir_df.loc[url, "rating"] = data_row.get("rating")
    dir_df.loc[url, "ratings_count"] = data_row.get("number_of_ratings")
    dir_df.loc[url, "input_modalities"] = data_row.get("modalities_inputs")
    dir_df.loc[url, "output_modalities"] = data_row.get("modalities_outputs")
    dir_df.loc[url, "tasks"] = data_row.get("task_label_name")
    dir_df.loc[url, "last_date"] = last_date

# ================= WRITE =================

dir_df.reset_index().to_csv(DIRECTORY_CSV, index=False)

print("✅ Directory updated in-place from live CSV")
