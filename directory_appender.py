#!/usr/bin/env python3

import pandas as pd
import json
import re
from datetime import datetime
from tqdm import tqdm

# ================= CONFIG =================

DIRECTORY_CSV = "new_directory.csv"
WAYBACK_CSV = "still_missing_unified.csv"

OUTPUT_DIRECTORY = "new_directory.csv"
STILL_MISSING = "still_missing_2.csv"

WAYBACK_RE = re.compile(r"https://web\.archive\.org/web/(\d{14})/(https://.+)")

# ================= HELPERS =================

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
    original = norm(m.group(2))
    date = datetime.strptime(ts[:8], "%Y%m%d").date()
    return original, date

def safe_json(x):
    if pd.isna(x):
        return []
    try:
        return json.loads(x)
    except Exception:
        return []

def merge_pricing(row):
    parts = []
    for col in [
        "pricing_model",
        "paid_options_from",
        "billing_frequency",
        "tag_price"
    ]:
        val = row.get(col)
        if isinstance(val, str) and val.strip():
            parts.append(val.strip())
    return " | ".join(dict.fromkeys(parts)) if parts else None

# ================= LOAD =================

dir_df = pd.read_csv(DIRECTORY_CSV)
way_df = pd.read_csv(WAYBACK_CSV, low_memory=False)

dir_df["tool_id"] = dir_df["tool_id"].apply(norm)
dir_df["name"] = dir_df["name"].replace("", pd.NA)

dir_df.set_index("tool_id", inplace=True)

# ================= INDEX WAYBACK (LATEST SNAPSHOT ONLY) =================

latest = {}

for _, row in tqdm(way_df.iterrows(), total=len(way_df), desc="Indexing wayback"):
    original, snap_date = extract_wayback_info(row.get("link"))
    if not original:
        continue

    if (
        original not in latest
        or latest[original]["snapshot_date"] < snap_date
    ):
        latest[original] = {
            "row": row,
            "snapshot_date": snap_date
        }

# ================= FILL EXITED TOOLS =================

for tool_id, drow in tqdm(
    dir_df[
        (dir_df["exited"] == 1) &
        (dir_df["name"].isna())
    ].iterrows(),
    desc="Filling exited tools"
):
    if tool_id not in latest:
        continue

    w = latest[tool_id]["row"]

    dir_df.loc[tool_id, "name"] = w.get("name")
    dir_df.loc[tool_id, "description"] = w.get("description")
    dir_df.loc[tool_id, "description_length"] = (
        len(w.get("description"))
        if isinstance(w.get("description"), str)
        else None
    )
    dir_df.loc[tool_id, "pricing_text"] = merge_pricing(w)
    dir_df.loc[tool_id, "saves"] = w.get("saves")
    dir_df.loc[tool_id, "rating"] = w.get("rating")
    dir_df.loc[tool_id, "ratings_count"] = w.get("number_of_ratings")
    dir_df.loc[tool_id, "input_modalities"] = w.get("modalities_inputs")
    dir_df.loc[tool_id, "output_modalities"] = w.get("modalities_outputs")
    dir_df.loc[tool_id, "tasks"] = w.get("task_label_name")
    dir_df.loc[tool_id, "last_date"] = latest[tool_id]["snapshot_date"].isoformat()

# ================= WRITE DIRECTORY =================

dir_df.reset_index().to_csv(OUTPUT_DIRECTORY, index=False)

# ================= STILL MISSING =================

still_missing = dir_df[dir_df["name"].isna()].reset_index()

still_missing[["tool_id"]].to_csv(STILL_MISSING, index=False)

print("✅ Exited tools enriched")
print(f"Still missing: {len(still_missing)}")
