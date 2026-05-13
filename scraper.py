import json
import time
import random
from datetime import datetime, timezone
from pathlib import Path

import requests
from bs4 import BeautifulSoup

import boto3
import os
from dotenv import load_dotenv

load_dotenv()

RAW_DIR = Path("data_lake/raw/")
RAW_DIR.mkdir(parents=True, exist_ok=True)

# Countries to scrape with their respective country codes for Steam
COUNTRIES = {
    "DE": "Germany",
    "NO": "Norway",
    "SE": "Sweden",
}

# Base URL for scraping top sellers with filters applied
# We filter for top sellers in the "Games" category (category1=998) and exclude free-to-play games (hidef2p=1)
# We also specify supported languages to ensure we get consistent data across countries (supportedlang=german,norwegian,swedish)
# The "ndl=1" parameter is used to disable dynamic loading of results, which makes scraping easier
# The "filter=topsellers" parameter ensures we only get top-selling games in the search results
BASE_SEARCH_URL = (
    "https://store.steampowered.com/search/"
    "?supportedlang=german%2Cnorwegian%2Cswedish"
    "&category1=998"
    "&hidef2p=1"
    "&filter=topsellers"
    "&ndl=1"
)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; BDE-ProjectBot/1.0; educational project)"
}

def upload_to_s3(local_file, bucket_name, s3_key):
    s3 = boto3.client("s3")

    s3.upload_file(
        local_file,
        bucket_name,
        s3_key
    )

    print(f"Uploaded to s3://{bucket_name}/{s3_key}")

def scrape_topseller_app_ids(country_code: str, max_pages: int = 2):
    app_ids = []

    for page in range(1, max_pages + 1):
        url = f"{BASE_SEARCH_URL}&cc={country_code}&page={page}"
        print(f"Scraping {country_code}: {url}")

        response = requests.get(url, headers=HEADERS, timeout=20)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")

        for row in soup.select("a.search_result_row"):
            app_id = row.get("data-ds-appid")

            if app_id and app_id.isdigit():
                app_ids.append(int(app_id))

        time.sleep(random.uniform(1, 3))

    return sorted(set(app_ids))


def fetch_app_details(app_id: int, country_code: str):
    url = "https://store.steampowered.com/api/appdetails"

    params = {
        "appids": app_id,
        "cc": country_code,
        "l": "english",
    }

    response = requests.get(url, params=params, headers=HEADERS, timeout=20)
    response.raise_for_status()

    payload = response.json()
    result = payload.get(str(app_id), {})

    if not result.get("success"):
        return None

    data = result.get("data", {})
    price = data.get("price_overview") or {}

    return {
        "country_code": country_code,
        "app_id": app_id,
        "name": data.get("name"),
        "type": data.get("type"),
        "is_free": data.get("is_free"),
        "release_date": (data.get("release_date") or {}).get("date"),
        "developers": data.get("developers", []),
        "publishers": data.get("publishers", []),
        "genres": [g.get("description") for g in data.get("genres", [])],
        "categories": [c.get("description") for c in data.get("categories", [])],
        "metacritic_score": (data.get("metacritic") or {}).get("score"),
        "currency": price.get("currency"),
        "initial_price_cents": price.get("initial"),
        "final_price_cents": price.get("final"),
        "discount_percent": price.get("discount_percent"),
        "final_price_formatted": price.get("final_formatted"),
        "scrape_timestamp": datetime.now(timezone.utc).isoformat(),
    }


def save_json(data, filename):
    path = RAW_DIR / filename

    with open(path, "w", encoding="utf-8") as file:
        json.dump(data, file, indent=2, ensure_ascii=False)

    print(f"Saved: {path}")

    return path


def main():
    today = datetime.now(timezone.utc).date().isoformat()

    topsellers = []
    details = []

    for country_code in COUNTRIES:
        app_ids = scrape_topseller_app_ids(country_code, max_pages=1)

        for rank, app_id in enumerate(app_ids, start=1):
            topsellers.append({
                "country_code": country_code,
                "rank": rank,
                "app_id": app_id,
                "scrape_date": today,
            })

        for app_id in app_ids:
            print(f"Fetching details {country_code} app_id={app_id}")
            detail = fetch_app_details(app_id, country_code)

            if detail:
                details.append(detail)

            time.sleep(random.uniform(1, 3))

    #save_json(topsellers, f"topsellers_{today}.json")
    #save_json(details, f"appdetails_{today}.json")

    topseller_file = save_json(topsellers, f"topsellers_{today}.json")
    appdetails_file = save_json(details, f"appdetails_{today}.json")

    bucket_name = os.getenv("AWS_S3_BUCKET")

    if bucket_name:
        upload_to_s3(
            local_file=str(topseller_file),
            bucket_name=bucket_name,
            s3_key=f"raw/{topseller_file.name}"
        )

        upload_to_s3(
            local_file=str(appdetails_file),
            bucket_name=bucket_name,
            s3_key=f"raw/{appdetails_file.name}"
        )
    else:
        print("AWS_S3_BUCKET not set. Skipping S3 upload.")


if __name__ == "__main__":
    main()

