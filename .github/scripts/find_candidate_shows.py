#!/usr/bin/env python3
"""find_candidate_shows.py — pulls Apple Podcasts top charts and identifies
shows not currently in the master list. Outputs candidates as JSON for human review.
"""

import argparse
import json
import sys
import time
import urllib.request
from typing import Any, Dict, List, Optional

GENRES = {
    "Business": 1321,
    "Technology": 1318,
    "Investing": 1493,
    "Entrepreneurship": 1494,
    "Tech News": 1480,
}

CHART_URL_TEMPLATE = "https://itunes.apple.com/us/rss/toppodcasts/limit={limit}/genre={genre_id}/json"
LOOKUP_URL_TEMPLATE = "https://itunes.apple.com/lookup?id={apple_id}"
USER_AGENT = "Lightwing-Podcast-Intel-QuarterlyRefresh/1.0"

CHART_LIMIT = 50
DELAY_BETWEEN_CALLS = 1.0


def fetch_json(url: str, timeout: int = 15) -> Optional[Any]:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        sys.stderr.write(f"[fetch] FAIL {url}: {e}\n")
        return None


def fetch_chart(genre_name: str, genre_id: int) -> List[Dict[str, Any]]:
    url = CHART_URL_TEMPLATE.format(limit=CHART_LIMIT, genre_id=genre_id)
    sys.stderr.write(f"[chart] {genre_name} (id={genre_id})\n")
    data = fetch_json(url)
    if not data:
        return []
    feed = data.get("feed", {})
    entries = feed.get("entry", [])
    out = []
    for entry in entries:
        apple_id = entry.get("id", {}).get("attributes", {}).get("im:id")
        name = entry.get("im:name", {}).get("label")
        if apple_id and name:
            out.append({
                "apple_id": int(apple_id),
                "name": name,
                "genre_source": genre_name,
            })
    sys.stderr.write(f"  -> {len(out)} entries\n")
    return out


def fetch_show_details(apple_id: int) -> Optional[Dict[str, Any]]:
    url = LOOKUP_URL_TEMPLATE.format(apple_id=apple_id)
    data = fetch_json(url)
    if not data or not data.get("results"):
        return None
    return data["results"][0]


def load_master_list_apple_ids(path: str) -> set:
    try:
        with open(path) as f:
            data = json.load(f)
    except Exception as e:
        sys.stderr.write(f"[error] Could not load master list at {path}: {e}\n")
        sys.exit(2)

    shows = data["shows"] if "shows" in data else data
    return {s["apple_id"] for s in shows if s.get("apple_id")}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--master-list", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    existing_ids = load_master_list_apple_ids(args.master_list)
    sys.stderr.write(f"[info] Master list has {len(existing_ids)} shows with apple_ids\n")

    chart_entries: Dict[int, Dict[str, Any]] = {}
    for genre_name, genre_id in GENRES.items():
        entries = fetch_chart(genre_name, genre_id)
        for e in entries:
            aid = e["apple_id"]
            if aid in chart_entries:
                chart_entries[aid]["genres_charting"].append(genre_name)
            else:
                chart_entries[aid] = {
                    "apple_id": aid,
                    "chart_name": e["name"],
                    "genres_charting": [genre_name],
                }
        time.sleep(DELAY_BETWEEN_CALLS)

    sys.stderr.write(f"[info] Found {len(chart_entries)} unique shows across charts\n")

    new_candidates = [e for aid, e in chart_entries.items() if aid not in existing_ids]
    sys.stderr.write(f"[info] {len(new_candidates)} candidates not in master list\n")

    enriched = []
    for i, candidate in enumerate(new_candidates):
        sys.stderr.write(f"[enrich {i+1}/{len(new_candidates)}] {candidate['chart_name']}\n")
        details = fetch_show_details(candidate["apple_id"])
        if not details:
            sys.stderr.write(f"  -> lookup failed, skipping\n")
            continue
        enriched.append({
            "name": details.get("collectionName"),
            "apple_id": candidate["apple_id"],
            "feed_url": details.get("feedUrl"),
            "artist_name": details.get("artistName"),
            "categories": details.get("genres", []),
            "track_count": details.get("trackCount"),
            "country": details.get("country"),
            "genres_charting_in": candidate["genres_charting"],
            "tier": "REVIEW",
            "notes": "Surfaced by quarterly refresh. Verify show fits curation criteria before adding.",
        })
        time.sleep(DELAY_BETWEEN_CALLS)

    enriched.sort(key=lambda x: -len(x["genres_charting_in"]))

    output = {
        "generated_at": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()),
        "source": "Apple Podcasts top charts (US)",
        "genres_scanned": list(GENRES.keys()),
        "shows_in_existing_list": len(existing_ids),
        "shows_in_charts": len(chart_entries),
        "candidates_count": len(enriched),
        "candidates": enriched,
    }

    with open(args.output, "w") as f:
        json.dump(output, f, indent=2)

    sys.stderr.write(f"[info] Wrote {len(enriched)} candidates to {args.output}\n")


if __name__ == "__main__":
    main()
