#!/usr/bin/env python3

import pandas as pd

DIRECTORY_CSV = "new_directory.csv"
STATUS_CSV = "url_status_checked.csv"   # the new csv you showed

OUTPUT_CSV = "new_directory.csv"  # overwrite safely

# ---------------- LOAD ----------------

dir_df = pd.read_csv(DIRECTORY_CSV)
status_df = pd.read_csv(STATUS_CSV)

# Normalize URLs
def norm(u):
    if not isinstance(u, str):
        return None
    return u.rstrip("/")

dir_df["tool_id"] = dir_df["tool_id"].apply(norm)
status_df["url"] = status_df["url"].apply(norm)
status_df["redirected_to"] = status_df["redirected_to"].apply(norm)

# Index directory for fast updates
dir_df.set_index("tool_id", inplace=True)

# ---------------- APPLY LOGIC ----------------

for _, row in status_df.iterrows():
    url = row["url"]
    is_redirected = row.get("is_redirected")
    redirected_to = row.get("redirected_to")

    if not is_redirected or not isinstance(redirected_to, str):
        continue

    # ---- CASE 1: Redirected to another TOOL (rename) ----
    if redirected_to.startswith("https://theresanaiforthat.com/ai/"):

        # old tool
        if url in dir_df.index:
            dir_df.loc[url, "name_changed"] = 1
            dir_df.loc[url, "new_name"] = redirected_to

        # new tool
        if redirected_to in dir_df.index:
            dir_df.loc[redirected_to, "name_changed"] = 1
            dir_df.loc[redirected_to, "new_name"] = redirected_to

    # ---- CASE 2: Redirected to TASK or /s/ (exit) ----
    elif "/task/" in redirected_to or "/s/" in redirected_to:
        if url in dir_df.index:
            dir_df.loc[url, "exited"] = 1

# ---------------- WRITE ----------------

dir_df.reset_index().to_csv(OUTPUT_CSV, index=False)

print("✅ Redirect status applied successfully")
