#!/usr/bin/env python3
"""find_candidate_shows.py v2 — pulls Apple Podcasts top charts and identifies
shows not currently in the master list. Outputs candidates with smart filtering.
"""

import argparse
import json
import re
import sys
import time
import urllib.request
from typing import Any, Dict, List, Optional, Tuple

GENRES = {
    "Business": 1321,
    "Technology": 1318,
    "Investing": 1493,
    "Entrepreneurship": 1494,
    "Tech News": 1480,
}

CHART_URL_TEMPLATE = "https://itunes.apple.com/us/rss/toppodcasts/limit={limit}/genre={genre_id}/json"
LOOKUP_URL_TEMPLATE = "https://itunes.apple.com/lookup?id={apple_id}"
USER_AGENT = "Lightwing-Podcast-Intel-QuarterlyRefresh/2.0"

CHART_LIMIT = 50
DELAY_BETWEEN_CALLS = 1.0
MIN_TRACK_COUNT = 25
ALLOWED_COUNTRIES = {"USA", "GBR", "AUS", "CAN"}

EXCLUSION_KEYWORDS = [
    "passive income", "real estate", "manifest", "abundance", "millionaire",
    "dropship", "side hustle", "financial freedom", "wealth building",
    "rich dad", "money mindset", "get rich", "build wealth",
    "money habits", "money mistakes",
    "crypto pump", "altcoin", "memecoin", "to the moon", "moonshot crypto",
    "shitcoin", "alpha leak",
    "mindset shift", "limitless mind", "unstoppable", "success habits",
    "manifest your", "high performance habits", "morning routine",
    "self improvement", "self-improvement", "personal growth",
    "network marketing", "high ticket", "6 figure", "7 figure", "8 figure",
    "6-figure", "7-figure", "8-figure", "six figure", "seven figure",
    "eight figure", "scale to", "scale your",
    "kingdom business", "christian entrepreneur", "biblical wealth",
    "faith and business", "faith driven",
    "rise and grind", "hustle culture", "no days off", "grind never stops",
    "andy frisella", "ed mylett", "lewis howes", "tom bilyeu",
    "grant cardone", "gary vaynerchuk", "garyvee",
]

QUALITY_PUBLISHERS = [
    "bloomberg", "new york times", "nyt", "wall street journal", "wsj",
    "financial times", "the information", "vox", "vox media", "npr",
    "pushkin", "wondery", "iheart", "iheartmedia", "iheartpodcasts",
    "axios", "the verge", "techcrunch", "fortune", "forbes", "cnbc",
    "marketplace", "american public media",
    "a16z", "andreessen horowitz", "sequoia", "lightspeed", "greylock",
    "founders fund", "benchmark", "kleiner perkins", "y combinator",
    "harvard business review", "hbr", "mckinsey", "stanford", "wharton",
    "goldman sachs", "jpmorgan", "morgan stanley",
]


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
    sys.stderr.write(f"[chart] {genre_name}\n")
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
            out.append({"apple_id": int(apple_id), "name": name, "genre_source": genre_name})
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


def is_excluded(name: str, artist: str) -> Tuple[bool, Optional[str]]:
    haystack = f"{name or ''} | {artist or ''}".lower()
    for keyword in EXCLUSION_KEYWORDS:
        pattern = r"\b" + re.escape(keyword) + r"\b"
        if re.search(pattern, haystack):
            return True, keyword
    return False, None


def quality_score(show: Dict[str, Any], chart_count: int) -> int:
    score = chart_count * 10
    artist = (show.get("artist_name") or "").lower()
    for publisher in QUALITY_PUBLISHERS:
        if publisher in artist:
            score += 50
            break
    relevant_cats = {"Technology", "Business", "News", "Tech News", "Investing", "Management"}
    for cat in show.get("categories", []):
        if cat in relevant_cats:
            score += 5
    if (show.get("track_count") or 0) > 100:
        score += 5
    return score


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

    new_candidates = [e for aid, e in chart_entries.items() if aid not in existing_ids]
    sys.stderr.write(f"[info] {len(new_candidates)} candidates not in master list\n")

    enriched = []
    excluded = {"keyword": 0, "track_count": 0, "country": 0, "lookup_failed": 0}
    excluded_examples: Dict[str, List[str]] = {"keyword": [], "track_count": [], "country": []}

    for i, candidate in enumerate(new_candidates):
        sys.stderr.write(f"[{i+1}/{len(new_candidates)}] {candidate['chart_name']}")
        details = fetch_show_details(candidate["apple_id"])
        if not details:
            sys.stderr.write(f"  -> lookup failed\n")
            excluded["lookup_failed"] += 1
            time.sleep(DELAY_BETWEEN_CALLS)
            continue

        name = details.get("collectionName") or ""
        artist = details.get("artistName") or ""
        track_count = details.get("trackCount") or 0
        country = details.get("country") or ""

        excluded_flag, reason = is_excluded(name, artist)
        if excluded_flag:
            sys.stderr.write(f"  -> EXCLUDED (keyword: '{reason}')\n")
            excluded["keyword"] += 1
            if len(excluded_examples["keyword"]) < 5:
                excluded_examples["keyword"].append(f"{name} (matched: {reason})")
            time.sleep(DELAY_BETWEEN_CALLS)
            continue

        if track_count < MIN_TRACK_COUNT:
            sys.stderr.write(f"  -> EXCLUDED (track_count={track_count})\n")
            excluded["track_count"] += 1
            if len(excluded_examples["track_count"]) < 5:
                excluded_examples["track_count"].append(f"{name} ({track_count} eps)")
            time.sleep(DELAY_BETWEEN_CALLS)
            continue

        if country and country not in ALLOWED_COUNTRIES:
            sys.stderr.write(f"  -> EXCLUDED (country={country})\n")
            excluded["country"] += 1
            if len(excluded_examples["country"]) < 5:
                excluded_examples["country"].append(f"{name} ({country})")
            time.sleep(DELAY_BETWEEN_CALLS)
            continue

        score = quality_score(
            {"artist_name": artist, "categories": details.get("genres", []), "track_count": track_count},
            chart_count=len(candidate["genres_charting"]),
        )
        sys.stderr.write(f"  -> KEPT (score={score})\n")

        enriched.append({
            "name": name,
            "apple_id": candidate["apple_id"],
            "feed_url": details.get("feedUrl"),
            "artist_name": artist,
            "categories": details.get("genres", []),
            "track_count": track_count,
            "country": country,
            "genres_charting_in": candidate["genres_charting"],
            "quality_score": score,
            "tier": "REVIEW",
            "notes": "Surfaced by quarterly refresh. Verify show fits curation criteria before adding.",
        })
        time.sleep(DELAY_BETWEEN_CALLS)

    enriched.sort(key=lambda x: -x["quality_score"])

    output = {
        "generated_at": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()),
        "source": "Apple Podcasts top charts (US)",
        "filter_version": "v2",
        "genres_scanned": list(GENRES.keys()),
        "shows_in_existing_list": len(existing_ids),
        "shows_in_charts": len(chart_entries),
        "raw_candidates_before_filters": len(new_candidates),
        "filter_results": {
            "kept": len(enriched),
            "excluded_by_keyword": excluded["keyword"],
            "excluded_by_track_count": excluded["track_count"],
            "excluded_by_country": excluded["country"],
            "lookup_failed": excluded["lookup_failed"],
        },
        "filter_examples": excluded_examples,
        "candidates_count": len(enriched),
        "candidates": enriched,
    }

    with open(args.output, "w") as f:
        json.dump(output, f, indent=2)

    sys.stderr.write(f"\n[summary] Kept {len(enriched)} of {len(new_candidates)}\n")


if __name__ == "__main__":
    main()
