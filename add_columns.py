#!/usr/bin/env python3

import pandas as pd

INPUT_CSV = "new_directory.csv"
OUTPUT_CSV = "new_directory.csv"   # overwrite safely

df = pd.read_csv(INPUT_CSV)

# Add new columns with fixed defaults
df["exited"] = 0
df["name_changed"] = 0
df["new_name"] = ""

df.to_csv(OUTPUT_CSV, index=False)

print("✅ Columns added successfully")
print("Added columns: exited, name_changed, new_name")
