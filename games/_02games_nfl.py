#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Scrape 2025 NFL schedule from NFL Operations and write games/nflgames.xml.

- Source: https://operations.nfl.com/gameday/nfl-schedule/2025-nfl-schedule/
- Produces a compact XML compatible with your college-football-ish fields
- Marks neutral-site games when matchup uses "vs"
- Stores time as ET string exactly as listed (e.g., "1:00p (ET)")
- Creates games/nflgames.xml.bak before overwrite

Requires: requests, beautifulsoup4
    pip install requests beautifulsoup4
"""

import os
import re
import shutil
from datetime import datetime
from pathlib import Path

import requests
from bs4 import BeautifulSoup, NavigableString

URL = "https://operations.nfl.com/gameday/nfl-schedule/2025-nfl-schedule/"
OUT_PATH = Path("games/nflgames.xml")
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

# ---- helpers ---------------------------------------------------------------

def fetch_html(url: str, timeout: int = 30) -> str:
    r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=timeout)
    r.raise_for_status()
    return r.text

def normalize_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", s.strip())

def extract_text_blocks(soup: BeautifulSoup) -> list[str]:
    """
    The page mixes headings and bare text nodes inside the article.
    We walk the main article content and collect clean text lines,
    preserving order. Empty lines are discarded.
    """
    article = soup.find(id="main") or soup  # fallback
    # The schedule sits under the h1/h2 content; safest is to walk all strings:
    lines = []
    for node in article.descendants:
        if isinstance(node, NavigableString):
            txt = normalize_spaces(str(node))
            if txt:
                lines.append(txt)
    return lines

def is_week_header(line: str) -> bool:
    return re.fullmatch(r"WEEK\s+\d+", line.upper()) is not None

def is_date_header(line: str) -> bool:
    # e.g., "Thursday, Sept. 4, 2025"
    return bool(re.match(r"^(Sunday|Monday|Tuesday|Wednesday|Thursday|Friday|Saturday),\s", line))

def is_matchup(line: str) -> bool:
    # "Team A at Team B" or "Team A vs Team B (Sao Paulo)" etc.
    return (" at " in line) or re.search(r"\s+vs\s+", line)

def is_time_et(line: str) -> bool:
    # Want the ET-bearing time like "1:00p (ET)" or "8:20p (ET)"
    return bool(re.search(r"\b\d{1,2}:\d{2}p\s*\(ET\)", line))

def is_network(line: str) -> bool:
    # Networks are short tokens like FOX, CBS, NBC, ABC/ESPN, Prime Video, NFLN, YouTube, ESPN/ABC, etc.
    return bool(re.fullmatch(r"[A-Z0-9+/& ]{2,20}", line)) and "ET" not in line

def split_matchup(line: str):
    """
    Returns (away, home, neutral_site: bool, note)
    - For 'at' -> away at home
    - For 'vs' -> neutral site; first is nominal home? We'll store first as away and second as home,
      but mark neutralsite=True. (You can swap if you prefer.)
    The page sometimes includes a location in parentheses after the second team: capture as 'note'.
    """
    # capture trailing "(...)" note
    note = ""
    m_note = re.search(r"\(([^)]*)\)$", line)
    if m_note:
        note = m_note.group(1).strip()
        core = line[:m_note.start()].strip()
    else:
        core = line

    if " at " in core:
        away, home = core.split(" at ", 1)
        neutral = False
    else:
        # "vs" case
        parts = re.split(r"\s+vs\s+", core)
        if len(parts) == 2:
            away, home = parts
        else:
            # Fallback: if something odd, return as-is
            away, home = core, ""
        neutral = True

    return normalize_spaces(away), normalize_spaces(home), neutral, note

MONTH_MAP = {
    "Sept.": "Sep", "Sep.": "Sep", "Sept": "Sep",
    "Oct.": "Oct", "Nov.": "Nov", "Dec.": "Dec", "Jan.": "Jan",
    "February": "Feb", "January": "Jan", "October": "Oct",
}

def parse_date_header(line: str) -> str:
    """
    Convert 'Thursday, Sept. 4, 2025' -> '2025-09-04'
    """
    # Remove weekday:
    line = re.sub(r"^[A-Za-z]+,\s+", "", line).strip()
    # Normalize month token:
    tokens = line.replace(",", "").split()
    # Expect: [Mon, DD, YYYY]
    if len(tokens) >= 3:
        mon, day, year = tokens[0], tokens[1], tokens[2]
        mon = MONTH_MAP.get(mon, mon[:3])  # crude normalize
        dt = datetime.strptime(f"{day} {mon} {year}", "%d %b %Y")
        return dt.strftime("%Y-%m-%d")
    return ""

def ensure_dir(p: Path):
    p.parent.mkdir(parents=True, exist_ok=True)

def backup_file(p: Path):
    if p.exists():
        shutil.copy2(p, p.with_suffix(p.suffix + f".bak"))

# ---- main scrape/parse -----------------------------------------------------

def scrape_schedule():
    html = fetch_html(URL)
    soup = BeautifulSoup(html, "html.parser")
    lines = extract_text_blocks(soup)

    season_year = 2025
    week = None
    cur_date = None

    games = []  # list of dicts with fields below

    i = 0
    while i < len(lines):
        line = lines[i]

        if is_week_header(line):
            week = int(line.split()[-1])
            i += 1
            continue

        if is_date_header(line):
            cur_date = parse_date_header(line)
            i += 1
            continue

        if is_matchup(line):
            away, home, neutral, note = split_matchup(line)

            # Expect the ET time on the next few lines; the page often lists ET time and then repeats a bare time.
            t_et = ""
            tv = ""
            j = i + 1
            # Scan a small window ahead for "ET" time and then a network token
            scan_limit = min(len(lines), i + 8)
            while j < scan_limit:
                nxt = lines[j]
                if not t_et and is_time_et(nxt):
                    t_et = nxt
                elif not tv and is_network(nxt):
                    tv = nxt.strip()
                    break
                j += 1

            games.append({
                "season": season_year,
                "week": week,
                "date": cur_date,           # YYYY-MM-DD
                "time_et": t_et,            # as listed, e.g. "1:00p (ET)"
                "away": away,
                "home": home,
                "tv": tv,
                "neutralsite": "True" if neutral else "False",
                "note": note,               # e.g., "Sao Paulo", "Dublin", "Tottenham"
            })

            i = j + 1
            continue

        i += 1

    return games

# ---- XML output ------------------------------------------------------------

def to_xml(games: list[dict]) -> str:
    from xml.etree.ElementTree import Element, SubElement, tostring
    from xml.dom import minidom

    root = Element("games", {
        "generated": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "source": "NFL Football Operations",
        "year": "2025",
        "last_updated": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "total_teams": "32",
    })

    # group by team like your college file, but here we’ll group by home team for compactness
    # also add a <team code=""> wrapper like your structure
    # collect unique team codes from home/away names (use display names as codes for now)
    from collections import defaultdict
    buckets = defaultdict(list)
    for g in games:
        buckets[g["home"]].append(g)

    gid = 1
    for team_name in sorted(buckets.keys()):
        team_el = SubElement(root, "team", {
            "code": team_name,
            "name": team_name,
            "total_games": str(len(buckets[team_name])),
            "updated": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        })
        for g in buckets[team_name]:
            # id scheme: YYYYWW_home_vs_away_serial
            game_id = f"{g['season']}{str(g['week']).zfill(2)}_{re.sub(r'\\W+','', g['home'])}_vs_{re.sub(r'\\W+','', g['away'])}_{gid}"
            SubElement(team_el, "game", {
                "id": str(gid),
                "extid": game_id,
                "season": str(g["season"]),
                "week": str(g["week"]),
                "seasontype": "regular",
                "startdate": f"{g['date']}",
                "starttimetbd": "False",
                "completed": "False",
                "neutralsite": g["neutralsite"],
                "conferencegame": "False",
                "venueid": "",
                "venue": g["note"] if g["neutralsite"] == "True" else "",
                "homeid": "",
                "hometeam": g["home"],
                "homeclassification": "pro",
                "homeconference": "NFL",
                "homepregameelo": "",
                "awayid": "",
                "awayteam": g["away"],
                "awayclassification": "pro",
                "awayconference": "NFL",
                "awaypregameelo": "",
                "notes": g["tv"],
                "tv": g["tv"],
                "time_et": g["time_et"],
            })
            gid += 1

    ugly = tostring(root, encoding="utf-8")
    pretty = minidom.parseString(ugly).toprettyxml(indent="  ", encoding="utf-8")
    return pretty.decode("utf-8")

def main():
    games = scrape_schedule()
    if not games:
        raise SystemExit("No games parsed — the page structure may have changed.")

    xml_text = to_xml(games)
    ensure_dir(OUT_PATH)
    backup_file(OUT_PATH)
    OUT_PATH.write_text(xml_text, encoding="utf-8")
    print(f"Wrote {OUT_PATH} with {len(games)} games.")

if __name__ == "__main__":
    main()
