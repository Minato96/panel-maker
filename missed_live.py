#!/usr/bin/env python3

import pandas as pd

INPUT_CSV = "new_directory.csv"
OUTPUT_CSV = "inactive_or_old_urls.csv"

df = pd.read_csv(INPUT_CSV)

# Normalize new_name (empty string → NaN)
df["new_name"] = df["new_name"].replace("", pd.NA)

filtered = df[
    (df["exited"] == 1) |
    (
        (df["name_changed"] == 1) &
        (df["tool_id"] != df["new_name"])
    )
]

filtered[["tool_id"]].to_csv(OUTPUT_CSV, index=False)

print("✅ inactive_or_old_urls.csv created")
print(f"Rows extracted: {len(filtered)}")
