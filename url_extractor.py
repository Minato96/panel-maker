import csv
from urllib.parse import urlparse

INPUT_CSV = "final_panel_data_final_4.csv"
OUTPUT_CSV = "clean_urls_2.csv"
COLUMN_NAME = "internal_link"

seen = set()

def extract_original_url(url):
    """
    Handles both:
    - Wayback URLs
    - Direct URLs
    """
    if not url:
        return None

    # Wayback case
    if "/web/" in url:
        try:
            part = url.split("/web/", 1)[1]
            return part.split("/", 1)[1]
        except IndexError:
            return None

    # Direct URL case
    return url



def is_valid_theresanaiforthat_url(url):
    parsed = urlparse(url)
    return parsed.netloc == "theresanaiforthat.com"


with open(INPUT_CSV, newline="", encoding="utf-8") as infile, \
     open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as outfile:

    reader = csv.DictReader(infile)
    writer = csv.writer(outfile)

    writer.writerow(["url"])

    for row in reader:
        raw_url = row.get(COLUMN_NAME)
        if not raw_url:
            continue

        original_url = extract_original_url(raw_url)
        if not original_url:
            continue

        if not is_valid_theresanaiforthat_url(original_url):
            continue

        if original_url in seen:
            continue

        seen.add(original_url)
        writer.writerow([original_url])
