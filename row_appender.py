import pandas as pd

# Load CSVs
reference_df = pd.read_csv("missed_live.csv")
data_df = pd.read_csv("new_directory.csv")
listing_df = pd.read_csv("taaft_tools_2015_2025.csv")

# Ensure tool_id is string (important)
reference_df["tool_id"] = reference_df["tool_id"].astype(str)
data_df["tool_id"] = data_df["tool_id"].astype(str)

new_rows = []

for tool_url in reference_df["tool_id"]:
    match = data_df[data_df["tool_id"] == tool_url]

    if match.empty:
        # Tool not found in data.csv → skip
        continue

    row = match.iloc[0]

    # Extract year from release_date
    try:
        year = float(row["release_date"][:4])
    except Exception:
        year = None

    new_rows.append({
        "year": year,
        "tool_name": row["name"],
        "tool_url": row["tool_id"]
    })

# Append new rows
if new_rows:
    new_df = pd.DataFrame(new_rows)
    listing_df = pd.concat([listing_df, new_df], ignore_index=True)

# Save back
listing_df.to_csv("listing.csv", index=False)

print(f"Added {len(new_rows)} new tools to listing.csv")
