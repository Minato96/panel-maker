#!/usr/bin/env python3

import pandas as pd
import re
import os
import json
from tqdm import tqdm

# ================= CONFIG =================

URLS_CSV = "clean_urls_3.csv"

CSV_2023 = "ai_wayback_async_out_2023.csv"
CSV_2024 = "ai_wayback_async_out_2024.csv"
CSV_2025 = "ai_wayback_async_out_2025.csv"
CSV_UNIFIED = "ai_wayback_unified1.csv"

OUT_CSV = "final_output.csv"
CHECKPOINT = "checkpoint.json"

CHUNKSIZE = 2000   # safe for 2GB+

SRC_2025 = 1
SRC_2024 = 2
SRC_2023 = 3
SRC_UNIFIED = 4

# ================= OUTPUT SCHEMA =================

OUT_COLUMNS = [
    "tool_url",
    "tool_name",
    "description",
    "description_length",
    "pricing",
    "rating",
    "release_date",
    "views",
    "comments_count",
    "comments",
    "saves",
    "video_views",
    "tasks",
    "inputs",
    "outputs",
    "snapshot_url",
    "snapshot_timestamp",
    "source",
]

# ================= URL HELPERS =================

WAYBACK_RE = re.compile(r"/web/\d{14}/(https?://[^ ]+)")
TOOL_RE = re.compile(r"(https://theresanaiforthat\.com/ai/[^/?#]+)")

def extract_original_url(snapshot_url):
    if not isinstance(snapshot_url, str):
        return None
    m = WAYBACK_RE.search(snapshot_url)
    return m.group(1).rstrip("/") if m else None

def normalize_tool_url(url):
    if not isinstance(url, str):
        return None
    m = TOOL_RE.search(url)
    return m.group(1).rstrip("/") if m else None

# ================= SAFE ACCESS =================

def safe(row, *cols):
    for c in cols:
        if c in row and pd.notna(row[c]):
            return row[c]
    return None

# ================= PRICING (CORRECT) =================

def pricing_text_2025(row):
    parts = []
    if safe(row, "pricing_model"):
        parts.append(str(row["pricing_model"]))
    if safe(row, "paid_options_from"):
        parts.append(f"from {row['paid_options_from']}")
    if safe(row, "billing_frequency"):
        parts.append(str(row["billing_frequency"]))
    if safe(row, "refund_policy"):
        parts.append(f"refund: {row['refund_policy']}")
    return " | ".join(parts) if parts else None

def pricing_text(row, year):
    if year == "2023":
        return safe(row, "tag_price")
    if year == "2024":
        return safe(row, "pricing_model")
    if year == "2025":
        return pricing_text_2025(row)
    if year == "unified":
        schema = safe(row, "_schema")
        if schema == "2023":
            return safe(row, "tag_price")
        if schema == "2024":
            return safe(row, "pricing_model")
        if schema == "2025":
            return pricing_text_2025(row)
    return None

# ================= ROW NORMALIZATION =================

def normalize_row(row, tool_url, year, source):
    desc = safe(row, "description")
    return {
        "tool_url": tool_url,
        "tool_name": safe(row, "name"),
        "description": desc,
        "description_length": len(desc) if isinstance(desc, str) else None,
        "pricing": pricing_text(row, year),
        "rating": safe(row, "rating"),
        "release_date": safe(row, "author_date", "use_case_created_date"),
        "views": safe(row, "views"),
        "comments_count": safe(row, "comments_count", "number_of_comments"),
        "comments": safe(row, "comments_json"),
        "saves": safe(row, "saves"),
        "video_views": safe(row, "video_views", "video_views_number"),
        "tasks": safe(row, "task_label_name", "rank_task_name"),
        "inputs": safe(row, "modalities_inputs"),
        "outputs": safe(row, "modalities_outputs"),
        "snapshot_url": safe(row, "snapshot_url", "link"),
        "snapshot_timestamp": safe(row, "snapshot_timestamp"),
        "source": source,
    }

# ================= CORE SCAN =================

def scan_csv(csv_path, tool_url, year, source, writer):
    reader = pd.read_csv(csv_path, chunksize=CHUNKSIZE, dtype=str)

    for chunk in tqdm(
        reader,
        desc=f"  Scanning {os.path.basename(csv_path)}",
        unit="chunk",
        leave=False
    ):
        for _, row in chunk.iterrows():
            snap = safe(row, "snapshot_url", "link")
            original = normalize_tool_url(extract_original_url(snap))
            if original != tool_url:
                continue

            writer.append(normalize_row(row, tool_url, year, source))


# ================= MAIN =================

def main():
    urls = pd.read_csv(URLS_CSV)["url"].dropna().map(lambda x: x.rstrip("/")).tolist()

    done = set()
    if os.path.exists(CHECKPOINT):
        done = set(json.load(open(CHECKPOINT)))

    buffer = []
    first_write = not os.path.exists(OUT_CSV)

    def flush():
        nonlocal buffer, first_write
        if not buffer:
            return
        pd.DataFrame(buffer, columns=OUT_COLUMNS).to_csv(
            OUT_CSV,
            mode="a",
            header=first_write,
            index=False
        )
        first_write = False
        buffer.clear()

    for tool_url in tqdm(urls, desc="Processing tools", unit="tool"):
        if tool_url in done:
            continue

        scan_csv(CSV_2023, tool_url, "2023", SRC_2023, buffer)
        scan_csv(CSV_2024, tool_url, "2024", SRC_2024, buffer)
        scan_csv(CSV_2025, tool_url, "2025", SRC_2025, buffer)
        scan_csv(CSV_UNIFIED, tool_url, "unified", SRC_UNIFIED, buffer)

        flush()
        done.add(tool_url)
        json.dump(list(done), open(CHECKPOINT, "w"))

    print("DONE — all snapshots processed, nothing skipped.")

if __name__ == "__main__":
    main()
