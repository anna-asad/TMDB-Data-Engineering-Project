#version 2 
"""TMDB full and incremental data pull script."""

import json
import os
import time
from datetime import date, datetime

import requests

API_KEY = os.environ.get("TMDB_API_KEY", "407127911d8fd445afe7b69d21388f10")
BASE_URL = "https://api.themoviedb.org/3"
# CONFIG
YEAR_RANGE = range(2005, 2015)      # inclusive start, exclusive end -> 2015-2026
MAX_PAGES_PER_YEAR = 500            # TMDB's hard cap per query
ENRICH_DETAILS = True               # fetch /movie/{id} for richer fields
ENRICH_LIMIT = 3000                # cap on how many movies get enriched
SLEEP_BETWEEN_CALLS = 0.05          # be polite to the API (~20 req/sec)
INCREMENTAL_CHANGES_PAGES = 5       # /movie/changes pages to pull (100 ids/page)

def api_get(path, params, retries=3):
    """GET with basic retry/backoff so one flaky request doesn't kill a long run."""
    params = {**params, "api_key": API_KEY}
    for attempt in range(retries):
        try:
            resp = requests.get(f"{BASE_URL}{path}", params=params, timeout=15)
            if resp.status_code == 429:
                wait = int(resp.headers.get("Retry-After", 2))
                print(f"  rate limited, waiting {wait}s...")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            print(f"  request failed ({e}), retry {attempt + 1}/{retries}")
            time.sleep(1 + attempt)
    return None
# FULL LOAD
def pull_full_load():
    all_movies = {}  # keyed by id to dedupe automatically
    start_time = time.time()

    for year in YEAR_RANGE:
        page = 1
        year_count = 0
        while page <= MAX_PAGES_PER_YEAR:
            data = api_get(
                "/discover/movie",
                {
                    "primary_release_year": year,
                    "sort_by": "popularity.desc",
                    "page": page,
                    "language": "en-US",
                    "include_adult": "false",
                },
            )
            if not data or not data.get("results"):
                break  # no more pages for this year

            for movie in data["results"]:
                all_movies[movie["id"]] = movie
            year_count += len(data["results"])

            total_pages = data.get("total_pages", 1)
            if page >= total_pages:
                break  # exhausted this year's results (usually well under 500)

            page += 1
            time.sleep(SLEEP_BETWEEN_CALLS)

        elapsed = time.time() - start_time
        print(
            f"  year {year}: +{year_count} movies "
            f"(running total: {len(all_movies)}, elapsed: {elapsed:.0f}s)"
        )

    movies_list = list(all_movies.values())
    # Optional enrichment: pull richer per-movie fields for the
    # highest-popularity subset, up to ENRICH_LIMIT.
    if ENRICH_DETAILS:
        movies_list.sort(key=lambda m: m.get("popularity", 0), reverse=True)
        to_enrich = movies_list[:ENRICH_LIMIT]
        print(f"\nEnriching {len(to_enrich)} movies with full details...")

        for i, movie in enumerate(to_enrich):
            details = api_get(
                f"/movie/{movie['id']}",
                {"append_to_response": "credits,keywords", "language": "en-US"},
            )
            if details:
                movie["runtime"] = details.get("runtime")
                movie["budget"] = details.get("budget")
                movie["revenue"] = details.get("revenue")
                movie["genres"] = details.get("genres")  # full {id, name} objects
                movie["production_companies"] = details.get("production_companies")
                movie["production_countries"] = details.get("production_countries")
                movie["spoken_languages"] = details.get("spoken_languages")
                movie["status"] = details.get("status")
                movie["tagline"] = details.get("tagline")
                cast = (details.get("credits") or {}).get("cast", [])[:5]
                movie["top_cast"] = [
                    {"name": c.get("name"), "character": c.get("character")}
                    for c in cast
                ]
                keywords = (details.get("keywords") or {}).get("keywords", [])
                movie["keywords"] = [k.get("name") for k in keywords]

            if (i + 1) % 500 == 0:
                elapsed = time.time() - start_time
                print(f"  enriched {i + 1}/{len(to_enrich)} (elapsed: {elapsed:.0f}s)")

            time.sleep(SLEEP_BETWEEN_CALLS)

    payload = {
        "pulled_at": str(date.today()),
        "source_endpoint": "/discover/movie (split by release_year)",
        "year_range": f"{YEAR_RANGE.start}-{YEAR_RANGE.stop - 1}",
        "enriched": ENRICH_DETAILS,
        "enrich_limit": ENRICH_LIMIT if ENRICH_DETAILS else 0,
        "record_count": len(movies_list),
        "results": movies_list,
    }

    with open("full_load_2005_2014.json", "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    size_mb = os.path.getsize("full_load.json") / (1024 * 1024)
    print(
        f"\nSaved full_load.json: {len(movies_list)} records, "
        f"{size_mb:.1f} MB"
    )
    return payload
# INCREMENTAL LOAD
def pull_incremental_load():
    # Get trending movies.
    trending = api_get("/trending/movie/day", {})
    trending_results = trending.get("results", []) if trending else []

    # Get changed movie IDs.
    changed_ids = []
    for page in range(1, INCREMENTAL_CHANGES_PAGES + 1):
        data = api_get("/movie/changes", {"page": page})
        if not data or not data.get("results"):
            break
        changed_ids.extend([item["id"] for item in data["results"]])
        time.sleep(SLEEP_BETWEEN_CALLS)

    print(f"Found {len(changed_ids)} changed movie IDs, resolving to full records...")

    # Resolve each changed ID to a full movie record.
    changed_movies = []
    for i, movie_id in enumerate(changed_ids):
        details = api_get(f"/movie/{movie_id}", {"language": "en-US"})
        if details and details.get("id"):
            changed_movies.append(details)

        if (i + 1) % 100 == 0:
            print(f"  resolved {i + 1}/{len(changed_ids)}")
        time.sleep(SLEEP_BETWEEN_CALLS)

    payload = {
        "pulled_at": str(date.today()),
        "trending_today": {
            "source_endpoint": "/trending/movie/day",
            "record_count": len(trending_results),
            "results": trending_results,
        },
        "changed_movies": {
            "source_endpoint": "/movie/changes -> resolved via /movie/{id}",
            "changed_id_count": len(changed_ids),
            "resolved_record_count": len(changed_movies),
            "results": changed_movies,
        },
    }

    with open("incremental_load.json", "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    size_kb = os.path.getsize("incremental_load.json") / 1024
    print(
        f"\nSaved incremental_load.json: "
        f"{len(trending_results)} trending + {len(changed_movies)} resolved "
        f"changed movies, {size_kb:.1f} KB"
    )
    return payload

if __name__ == "__main__":
    if API_KEY == "YOUR_TMDB_API_KEY_HERE":
        raise SystemExit(
            "Set your TMDB API key first: either edit API_KEY in this "
            "script, or run `export TMDB_API_KEY=your_key_here` "
            "(or `set TMDB_API_KEY=your_key_here` on Windows) before running."
        )

    print(f"Started at {datetime.now().strftime('%H:%M:%S')}")
    print("=" * 60)
    print("PULLING FULL LOAD (this can take a while depending on CONFIG)")
    print("=" * 60)
    pull_full_load()

    print("\n" + "=" * 60)
    print("PULLING INCREMENTAL LOAD")
    print("=" * 60)
    pull_incremental_load()

    print(f"\nFinished at {datetime.now().strftime('%H:%M:%S')}")
    print("Files written: full_load.json, incremental_load.json")
    #print("Re-run tomorrow to get a genuinely different incremental_load.json.")