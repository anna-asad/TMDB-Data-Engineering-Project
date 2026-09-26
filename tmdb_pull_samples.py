import json
import os
import time
from datetime import date

import requests

API_KEY = os.environ.get("TMDB_API_KEY", "407127911d8fd445afe7b69d21388f10")
BASE_URL = "https://api.themoviedb.org/3"

# Each page = 20 movies. 50 pages = 1,000 movies (~1-2 MB of JSON)
# TMDB caps most discover/top_rated endpoints at 500 pages
FULL_LOAD_PAGES = 50


def fetch_json(url, params):
    resp = requests.get(url, params=params, timeout=15)
    resp.raise_for_status()
    return resp.json()


def pull_full_load():
    """Historical baseline: top-rated movies, paginated."""
    all_results = []
    for page in range(1, FULL_LOAD_PAGES + 1):
        params = {"api_key": API_KEY, "language": "en-US", "page": page}
        data = fetch_json(f"{BASE_URL}/movie/top_rated", params)
        results = data.get("results", [])
        if not results:
            break
        all_results.extend(results)
        print(f"  full load: pulled page {page} ({len(results)} movies)")
        time.sleep(0.25)  # be polite to the API

    payload = {
        "pulled_at": str(date.today()),
        "source_endpoint": "/movie/top_rated",
        "pages_pulled": page,
        "record_count": len(all_results),
        "results": all_results,
    }
    with open("full_load.json", "w") as f:
        json.dump(payload, f, indent=2)
    print(f"Saved full_load.json ({len(all_results)} records)")
    return payload


def pull_incremental_load():
    """
    'New / changed today' snapshot. Combines two signals:
      - trending/movie/day  -> what's hot today
      - movie/changes       -> movie IDs TMDB flagged as updated in last 24h
    """
    trending = fetch_json(
        f"{BASE_URL}/trending/movie/day", {"api_key": API_KEY}
    )
    changes = fetch_json(
        f"{BASE_URL}/movie/changes", {"api_key": API_KEY, "page": 1}
    )

    payload = {
        "pulled_at": str(date.today()),
        "trending_today": {
            "source_endpoint": "/trending/movie/day",
            "record_count": len(trending.get("results", [])),
            "results": trending.get("results", []),
        },
        "changed_ids_today": {
            "source_endpoint": "/movie/changes",
            "record_count": len(changes.get("results", [])),
            "results": changes.get("results", []),
        },
    }
    with open("incremental_load.json", "w") as f:
        json.dump(payload, f, indent=2)
    print(
        f"Saved incremental_load.json "
        f"(trending={payload['trending_today']['record_count']}, "
        f"changed={payload['changed_ids_today']['record_count']})"
    )
    return payload


if __name__ == "__main__":
    if API_KEY == "YOUR_TMDB_API_KEY_HERE":
        raise SystemExit(
            "Set your TMDB API key first: either edit API_KEY in this "
            "script, or run `export TMDB_API_KEY=your_key_here` before "
            "running this script."
        )

    print("Pulling full load sample...")
    pull_full_load()

    print("\nPulling inc load sample...")
    pull_incremental_load()

    print("\nDone. Files written: full_load.json, incremental_load.json")