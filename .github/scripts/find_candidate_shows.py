#!/usr/bin/env python3
"""find_candidate_shows.py v3 — allowlist mode.

Keeps only shows that pass at least one of:
  1. Artist name matches a known quality publisher
  2. Apple categories include high-signal combinations
  3. Show charts in 3+ of the 5 scanned genres

Plus the v2 pre-filters: keyword exclusion, track count, country.
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
USER_AGENT = "Lightwing-Podcast-Intel-QuarterlyRefresh/3.0"

CHART_LIMIT = 50
DELAY_BETWEEN_CALLS = 1.0
MIN_TRACK_COUNT = 25
ALLOWED_COUNTRIES = {"USA", "GBR", "AUS", "CAN"}
MIN_CROSS_CATEGORY_CHARTS = 3

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

# ALLOWLIST: artist name must contain at least one of these for the
# "known publisher" path. Lowercased substring match.
QUALITY_PUBLISHERS = [
    "bloomberg", "new york times", "nyt", "wall street journal", "wsj",
    "financial times", "the information", "vox", "vox media", "npr",
    "pushkin", "wondery", "iheart", "iheartmedia", "iheartpodcasts",
    "axios", "the verge", "techcrunch", "fortune", "forbes", "cnbc",
    "marketplace", "american public media", "apm",
    "a16z", "andreessen horowitz", "sequoia", "lightspeed", "greylock",
    "founders fund", "benchmark", "kleiner perkins", "y combinator",
    "harvard business review", "hbr", "mckinsey", "stanford", "wharton",
    "goldman sachs", "jpmorgan", "morgan stanley",
    "the atlantic", "new yorker", "wired", "fast company", "inc.",
    "puck", "semafor", "stratechery", "ben thompson",
    "kara swisher", "scott galloway", "tim ferriss", "ezra klein",
    "nilay patel", "matt belloni", "dwarkesh", "lex fridman",
    "patrick o'shaughnessy", "harry stebbings", "logan bartlett",
    "all-in", "chamath", "jason calacanis",
    "ted", "ted talks", "ted radio",
    "colossus",
]

# Categories that, when present, signal serious tech/business content
HIGH_SIGNAL_CATEGORIES = {
    "Tech News",
}
# Pairs of categories that together signal serious content
HIGH_SIGNAL_PAIRS = [
    {"Technology", "Business"},
    {"Technology", "News"},
    {"Business", "News"},
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


def passes_allowlist(artist: str, categories: List[str], chart_count: int) -> Tuple[bool, str]:
    """v3 allowlist check. Returns (passes, reason)."""
    artist_lc = (artist or "").lower()

    # Path 1: known publisher
    for pub in QUALITY_PUBLISHERS:
        if pub in artist_lc:
            return True, f"publisher:{pub}"

    # Path 2: high-signal category present
    cats_set = set(categories or [])
    for cat in HIGH_SIGNAL_CATEGORIES:
        if cat in cats_set:
            return True, f"category:{cat}"

    # Path 3: high-signal category PAIRS
    for pair in HIGH_SIGNAL_PAIRS:
        if pair.issubset(cats_set):
            return True, f"category_pair:{'+'.join(sorted(pair))}"

    # Path 4: cross-category chart appearance
    if chart_count >= MIN_CROSS_CATEGORY_CHARTS:
        return True, f"cross_category_charts:{chart_count}"

    return False, "no_allowlist_match"


def quality_score(artist: str, categories: List[str], chart_count: int, track_count: int) -> int:
    """Higher = better. Used to sort candidates within the kept list."""
    score = chart_count * 10
    artist_lc = (artist or "").lower()
    for pub in QUALITY_PUBLISHERS:
        if pub in artist_lc:
            score += 50
            break
    relevant = {"Technology", "Business", "News", "Tech News", "Investing", "Management"}
    for cat in categories or []:
        if cat in relevant:
            score += 5
    if track_count > 100:
        score += 5
    return score


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--master-list", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    existing_ids = load_master_list_apple_ids(args.master_list)
    sys.stderr.write(f"[info] Master list has {len(existing_ids)} shows\n")

    chart_entries: Dict[int, Dict[str, Any]] = {}
    for genre_name, genre_id in GENRES.items():
        for e in fetch_chart(genre_name, genre_id):
            aid = e["apple_id"]
            if aid in chart_entries:
                chart_entries[aid]["genres_charting"].append(genre_name)
            else:
                chart_entries[aid] = {"apple_id": aid, "chart_name": e["name"], "genres_charting": [genre_name]}
        time.sleep(DELAY_BETWEEN_CALLS)

    new_candidates = [e for aid, e in chart_entries.items() if aid not in existing_ids]
    sys.stderr.write(f"[info] {len(new_candidates)} candidates not in master list\n")

    enriched = []
    excluded = {"keyword": 0, "track_count": 0, "country": 0, "allowlist": 0, "lookup_failed": 0}
    excluded_examples: Dict[str, List[str]] = {"keyword": [], "track_count": [], "country": [], "allowlist": []}

    for i, candidate in enumerate(new_candidates):
        sys.stderr.write(f"[{i+1}/{len(new_candidates)}] {candidate['chart_name']}")
        details = fetch_show_details(candidate["apple_id"])
        if not details:
            excluded["lookup_failed"] += 1
            sys.stderr.write(f"  -> lookup failed\n")
            time.sleep(DELAY_BETWEEN_CALLS)
            continue

        name = details.get("collectionName") or ""
        artist = details.get("artistName") or ""
        track_count = details.get("trackCount") or 0
        country = details.get("country") or ""
        categories = details.get("genres", [])
        chart_count = len(candidate["genres_charting"])

        # Pre-filter 1: keyword
        excl, reason = is_excluded(name, artist)
        if excl:
            excluded["keyword"] += 1
            if len(excluded_examples["keyword"]) < 5:
                excluded_examples["keyword"].append(f"{name} ({reason})")
            sys.stderr.write(f"  -> EXCLUDED keyword:{reason}\n")
            time.sleep(DELAY_BETWEEN_CALLS)
            continue

        # Pre-filter 2: track count
        if track_count < MIN_TRACK_COUNT:
            excluded["track_count"] += 1
            if len(excluded_examples["track_count"]) < 5:
                excluded_examples["track_count"].append(f"{name} ({track_count} eps)")
            sys.stderr.write(f"  -> EXCLUDED track:{track_count}\n")
            time.sleep(DELAY_BETWEEN_CALLS)
            continue

        # Pre-filter 3: country
        if country and country not in ALLOWED_COUNTRIES:
            excluded["country"] += 1
            if len(excluded_examples["country"]) < 5:
                excluded_examples["country"].append(f"{name} ({country})")
            sys.stderr.write(f"  -> EXCLUDED country:{country}\n")
            time.sleep(DELAY_BETWEEN_CALLS)
            continue

        # ALLOWLIST CHECK
        passes, reason = passes_allowlist(artist, categories, chart_count)
        if not passes:
            excluded["allowlist"] += 1
            if len(excluded_examples["allowlist"]) < 10:
                excluded_examples["allowlist"].append(f"{name} (artist: {artist})")
            sys.stderr.write(f"  -> EXCLUDED allowlist\n")
            time.sleep(DELAY_BETWEEN_CALLS)
            continue

        score = quality_score(artist, categories, chart_count, track_count)
        sys.stderr.write(f"  -> KEPT score={score} reason={reason}\n")

        enriched.append({
            "name": name,
            "apple_id": candidate["apple_id"],
            "feed_url": details.get("feedUrl"),
            "artist_name": artist,
            "categories": categories,
            "track_count": track_count,
            "country": country,
            "genres_charting_in": candidate["genres_charting"],
            "quality_score": score,
            "allowlist_reason": reason,
            "tier": "REVIEW",
            "notes": "Surfaced by quarterly refresh. Verify show fits curation criteria before adding.",
        })
        time.sleep(DELAY_BETWEEN_CALLS)

    enriched.sort(key=lambda x: -x["quality_score"])

    output = {
        "generated_at": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()),
        "source": "Apple Podcasts top charts (US)",
        "filter_version": "v3-allowlist",
        "genres_scanned": list(GENRES.keys()),
        "shows_in_existing_list": len(existing_ids),
        "shows_in_charts": len(chart_entries),
        "raw_candidates_before_filters": len(new_candidates),
        "filter_results": {
            "kept": len(enriched),
            "excluded_by_keyword": excluded["keyword"],
            "excluded_by_track_count": excluded["track_count"],
            "excluded_by_country": excluded["country"],
            "excluded_by_allowlist": excluded["allowlist"],
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
