#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
teams_nfl_from_nfl_com.py
---------------------------------
Basic, robust scraper for live NFL team metadata from https://www.nfl.com/teams/.

Outputs XML (../teams/teams_nfl.xml) with:
  <representing_name>  City/region (e.g., "San Francisco")
  <nickname>           Team nickname (e.g., "49ers")
  <league_abbr>        League abbreviation (NFL)
  <conference>         AFC or NFC (derived from nearest heading or fallback list)
  <conf_abbr>          Abbreviation of conference (AFC or NFC, same as conference)
  <division>           (blank in this "basic" version; index page usually doesn't show it)
  <official_site>      The team's NFL.com profile URL (on nfl.com)
  <corp_site>          The club's own site (e.g., 49ers.com), found in the same card/section

Why scrape the teams index page?
- It lists every club and links to both the NFL profile and the club site.
- We avoid brittle Wikipedia tables and stick to the official league site.
- We keep network usage small: 1 HTTP GET to the index page (no per-team fetch).

Key techniques you’ll learn here:
- Anchoring output paths to the script’s location (so it works no matter where you run it)
- Making HTTP GETs with timeout and a helpful User-Agent
- HTML parsing with BeautifulSoup (multiple name extraction strategies)
- Defensive heuristics (skip "View Profile" text; fall back to URL slug → proper name)
- Fallback conference assignment to ensure no team lacks a conference
- XML building with ElementTree + pretty indentation
"""

# --------------------- Standard Library ---------------------
import os
import re
import sys
import urllib.parse
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timezone

# --------------------- Third-Party --------------------------
# pip install requests beautifulsoup4
import requests
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET


# =============================================================================
#                                CONFIGURATION
# =============================================================================

INDEX_URL = "https://www.nfl.com/teams/"

# Hard-coded league information
LEAGUE_NAME = "National Football League"
LEAGUE_ABBR = "NFL"
CONFERENCES = ["AFC", "NFC"]

# Fallback conference assignments for robustness
FALLBACK_CONFERENCES = {
    # AFC Teams (by slug or nickname)
    "buffalo-bills": "AFC", "miami-dolphins": "AFC", "new-england-patriots": "AFC",
    "new-york-jets": "AFC", "baltimore-ravens": "AFC", "cincinnati-bengals": "AFC",
    "cleveland-browns": "AFC", "pittsburgh-steelers": "AFC", "houston-texans": "AFC",
    "indianapolis-colts": "AFC", "jacksonville-jaguars": "AFC", "tennessee-titans": "AFC",
    "denver-broncos": "AFC", "kansas-city-chiefs": "AFC", "las-vegas-raiders": "AFC",
    "los-angeles-chargers": "AFC",
    # NFC Teams (by slug or nickname)
    "dallas-cowboys": "NFC", "new-york-giants": "NFC", "philadelphia-eagles": "NFC",
    "washington-commanders": "NFC", "chicago-bears": "NFC", "detroit-lions": "NFC",
    "green-bay-packers": "NFC", "minnesota-vikings": "NFC", "atlanta-falcons": "NFC",
    "carolina-panthers": "NFC", "new-orleans-saints": "NFC", "tampa-bay-buccaneers": "NFC",
    "arizona-cardinals": "NFC", "los-angeles-rams": "NFC", "san-francisco-49ers": "NFC",
    "seattle-seahawks": "NFC"
}

# A polite, descriptive User-Agent helps site operators identify your traffic
# and may reduce the risk of being throttled or blocked.
HEADERS = {
    "User-Agent": "HorsesMouthSportsData/1.2 (NFL teams scraper; contact: you@example.com)"
}

# Anchor the output to the SCRIPT FILE'S directory (not the process CWD).
# This means it will always write ../teams/teams_nfl.xml *relative to this .py*,
# even if you run it from somewhere else.
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_PATH = os.path.normpath(os.path.join(SCRIPT_DIR, "..", "teams", "teams_nfl.xml"))

# Simple “vocabulary” for headings we’ll parse (e.g., “AFC Teams”)
DIV_WORDS = ("East", "North", "South", "West")  # not used in basic mode, but kept for clarity
CONF_WORDS = ("AFC", "NFC")

# Basic debug switch for teaching. Set to True to see extra logs on stderr.
DEBUG = True

# Social domains we’ll ignore when looking for the club’s corporate site link
SOCIAL_HINTS = ("facebook.", "instagram.", "x.com", "twitter.", "snapchat.", "tiktok.")


# =============================================================================
#                                  LOGGING
# =============================================================================

def debug(msg: str) -> None:
    """Print teaching/debug info to stderr when DEBUG is True."""
    if DEBUG:
        print(f"[DEBUG] {msg}", file=sys.stderr)


# =============================================================================
#                               HTTP UTILITIES
# =============================================================================

def get_html(url: str) -> str:
    """
    Minimal HTTP GET wrapper:
    - Adds timeout (never scrape without a timeout).
    - Raises for non-2xx status codes.
    """
    debug(f"GET {url}")
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.text


# =============================================================================
#                             TEXT & URL UTILITIES
# =============================================================================

def clean(s: str) -> str:
    """Normalize whitespace: squash runs of spaces/tabs/newlines into single spaces."""
    return " ".join((s or "").split()).strip()


def split_rep_and_nick(full_name: str) -> Tuple[str, str]:
    """
    Split 'City Nickname' at the LAST space.
    This preserves multi-word cities:
      - 'New York Giants'     -> ('New York', 'Giants')
      - 'San Francisco 49ers' -> ('San Francisco', '49ers')
      - 'Green Bay Packers'   -> ('Green Bay', 'Packers')
    """
    parts = full_name.rsplit(" ", 1)
    if len(parts) == 2:
        return parts[0], parts[1]
    return full_name, ""


def is_team_profile_href(href: str) -> bool:
    """
    Match links like:
      /teams/<slug>/         (relative)
      https://www.nfl.com/teams/<slug>[/]  (absolute)
    """
    if not href:
        return False
    test = href
    if test.startswith("//"):
        test = "https:" + test
    if test.startswith("/"):
        test = "https://www.nfl.com" + test
    return re.search(r"^https?://www\.nfl\.com/teams/[^/]+/?$", test) is not None


def absolutize_href(href: str, base: str = "https://www.nfl.com") -> str:
    """
    Convert relative links to absolute URLs (keeps absolute links unchanged).
    """
    if not href:
        return ""
    if href.startswith("//"):
        return "https:" + href
    if href.startswith("/"):
        return urllib.parse.urljoin(base, href)
    return href


def url_domain(url: str) -> str:
    """Return just the domain (host) of a URL, lowercased."""
    try:
        return urllib.parse.urlparse(url).netloc.lower()
    except Exception:
        return ""


def slug_from_official_site(url: str) -> Optional[str]:
    """
    Extract the <slug> from something like:
      https://www.nfl.com/teams/las-vegas-raiders/
    -> 'las-vegas-raiders'
    """
    m = re.search(r"/teams/([^/]+)/?$", url or "")
    return m.group(1) if m else None


def name_from_slug(slug: str) -> str:
    """
    Fallback: Build a decent team name from the URL slug.
    We avoid str.title() (which would turn "49ers" into "49Ers").
    Rule:
      - If a token starts with a digit, keep it as-is (e.g., "49ers").
      - Otherwise capitalize first letter, rest lower (e.g., 'kansas' -> 'Kansas').
    """
    parts = (slug or "").split("-")
    fixed = []
    for p in parts:
        if not p:
            continue
        if p[0].isdigit():
            fixed.append(p)  # keep "49ers" as-is
        else:
            fixed.append(p[0].upper() + p[1:].lower())
    return " ".join(fixed).strip()


# =============================================================================
#                         HEADINGS / CONFERENCE DETECTION
# =============================================================================

def parse_heading_text(txt: str) -> Tuple[Optional[str], Optional[str]]:
    """
    From a heading like 'AFC Teams' / 'NFC West Teams', detect (conference, division).
    For the basic scraper we only keep 'conference'; 'division' stays blank.
    """
    t = clean(txt)
    conf = next((c for c in CONF_WORDS if c in t), None)
    div  = next((d for d in DIV_WORDS if d in t), None)
    return conf, div


def nearest_conference_heading(el) -> Optional[str]:
    """
    Walk backward through previous siblings and then up the DOM looking for the
    nearest heading (h1..h6) that contains 'AFC' or 'NFC'.

    Why this approach?
    - nfl.com/teams groups teams visually under 'NFC Teams' / 'AFC Teams'.
    - Card markup can vary over time; anchoring to nearby headings is resilient.
    """
    cur = el
    for _ in range(200):  # guard against infinite loops on odd HTML
        if cur is None:
            break
        if getattr(cur, "name", None) and re.fullmatch(r"h[1-6]", cur.name):
            conf, _ = parse_heading_text(cur.get_text(" "))
            if conf:
                return conf

        # Prefer walking previous siblings before climbing the tree,
        # so we stay within the same visual section if possible.
        ps = getattr(cur, "previous_sibling", None)
        while ps is not None and getattr(ps, "name", None) is None:
            ps = getattr(ps, "previous_sibling", None)
        if ps is not None:
            cur = ps
            continue

        # No more previous siblings; climb to the parent.
        cur = getattr(cur, "parent", None)

    return None


def get_fallback_conference(slug: str, name_text: str) -> Optional[str]:
    """
    Fallback to assign a conference based on team slug or name if no heading is found.
    Returns 'AFC', 'NFC', or None if no match is found.
    """
    if slug in FALLBACK_CONFERENCES:
        return FALLBACK_CONFERENCES[slug]
    
    # Try matching by nickname (case-insensitive)
    nickname = clean(name_text).rsplit(" ", 1)[-1].lower()
    for team_slug, conf in FALLBACK_CONFERENCES.items():
        team_nickname = team_slug.split("-")[-1].lower()
        if nickname == team_nickname:
            return conf
    
    return None


# =============================================================================
#                        TEAM NAME / CORP-SITE DETECTION
# =============================================================================

def get_team_name_for_anchor(a, official_site: str) -> Optional[str]:
    """
    Given an <a> that points to an NFL.com team profile, derive the human team name.

    Strategy (ordered):
    1) If the anchor's own text is a real name (not 'View Profile' / 'View Full Site'),
       use it.
    2) Look for the nearest preceding <h1..h6> (cards frequently render the team
       name as a small heading near the buttons).
    3) Look for a nearby <a> whose text starts with 'Image: ' and strip that prefix —
       those often carry 'Image: <Team Name>'.
    4) Fallback: build a proper-looking name from the URL slug (e.g., 'las-vegas-raiders'
       -> 'Las Vegas Raiders').
    """
    txt = clean(a.get_text(" "))
    if txt and txt not in ("View Profile", "View Full Site") and not txt.lower().startswith("image:"):
        return txt

    # 2) nearest preceding heading
    cur = a
    for _ in range(100):  # limit the walk
        if cur is None:
            break
        ps = getattr(cur, "previous_sibling", None)
        while ps is not None and getattr(ps, "name", None) is None:
            ps = getattr(ps, "previous_sibling", None)
        if ps is not None:
            if getattr(ps, "name", None) and re.fullmatch(r"h[1-6]", ps.name):
                name_text = clean(ps.get_text(" "))
                if name_text and name_text not in ("View Profile", "View Full Site"):
                    return name_text
            cur = ps
            continue
        cur = getattr(cur, "parent", None)

    # 3) Nearby 'Image: Team' anchor
    img_a = a.find_previous("a", href=True)
    if img_a:
        label = clean(img_a.get_text(" "))
        if label.lower().startswith("image:"):
            maybe_name = clean(label.split(":", 1)[-1])
            if maybe_name:
                return maybe_name

    # 4) Fallback: slug → name
    slug = slug_from_official_site(official_site)
    if slug:
        return name_from_slug(slug)

    return None


def find_corp_site_near(a) -> Optional[str]:
    """
    Find the club's corporate site URL near the team profile link.

    Heuristics:
    - Prefer an anchor whose visible text contains 'View Full Site'.
    - Otherwise, pick the first external (non-nfl.com) link in the same
      logical container (walking up a few ancestors).
    - Skip obvious social domains; we want the main club site.
    """
    container = a
    for _ in range(6):  # walk up a few ancestors
        if container is None:
            break

        # 1) Explicit 'View Full Site'
        link = container.find("a", string=lambda s: isinstance(s, str) and "View Full Site" in s)
        if link and link.get("href"):
            href = absolutize_href(link.get("href"))
            dom = url_domain(href)
            if href and dom and not dom.endswith("nfl.com") and not any(s in dom for s in SOCIAL_HINTS):
                return href

        # 2) Any non-nfl.com link in this container
        for cand in container.select("a[href]"):
            href = absolutize_href(cand.get("href"))
            dom = url_domain(href)
            if href and dom and not dom.endswith("nfl.com"):
                if any(s in dom for s in SOCIAL_HINTS):
                    continue
                return href

        container = getattr(container, "parent", None)

    return None


# =============================================================================
#                              CORE PAGE PARSE
# =============================================================================

def parse_index_page(soup: BeautifulSoup) -> List[Dict]:
    """
    Walk the whole page once, pick out every anchor that looks like a team profile.
    For each such anchor:
      - Derive the team name (robustly).
      - Derive conference from the nearest heading or fallback list.
      - Find the club's corporate site link in the same card/section.
    Deduplicate by slug so each team appears only once.
    """
    teams: List[Dict] = []
    anchors = soup.select("a[href]")
    seen_slugs = set()

    for a in anchors:
        href_raw = a.get("href") or ""
        if not is_team_profile_href(href_raw):
            continue

        official_site = absolutize_href(href_raw, base=INDEX_URL)
        slug = slug_from_official_site(official_site)
        if not slug:
            continue
        if slug in seen_slugs:
            continue
        seen_slugs.add(slug)

        # Derive a proper human-readable team name
        name_text = get_team_name_for_anchor(a, official_site)
        if not name_text or name_text in ("View Profile", "View Full Site"):
            debug(f"Skipped ambiguous anchor for slug={slug} (text={name_text!r})")
            continue

        representing_name, nickname = split_rep_and_nick(name_text)

        # Find conference (AFC/NFC) from nearby headings or fallback
        conference = nearest_conference_heading(a)
        if not conference:
            conference = get_fallback_conference(slug, name_text)
            if conference:
                debug(f"Assigned fallback conference {conference} for team {name_text} (slug={slug})")
            else:
                debug(f"No conference detected for team {name_text} (slug={slug})")

        # Grab the club's official corporate site (non-nfl.com) near this link
        corp_site = find_corp_site_near(a) or ""

        teams.append({
            "representing_name": representing_name,
            "nickname": nickname,
            "league_abbr": LEAGUE_ABBR,
            "conference": conference or "",
            "conf_abbr": conference or "",  # Same as conference for AFC/NFC
            "division": "",  # not present on index; leave blank for basic mode
            "official_site": official_site,
            "corp_site": corp_site,
        })

    return teams


# =============================================================================
#                             XML BUILD / WRITE
# =============================================================================

def build_xml(rows: List[Dict]) -> ET.ElementTree:
    """
    Build a simple, stable XML document:

    <teams generated="...UTC..." source="..." league_name="National Football League" league_abbr="NFL" conferences="AFC,NFC">
      <team>
        <representing_name>San Francisco</representing_name>
        <nickname>49ers</nickname>
        <league_abbr>NFL</league_abbr>
        <conference>NFC</conference>
        <conf_abbr>NFC</conf_abbr>
        <division></division>
        <official_site>https://www.nfl.com/teams/san-francisco-49ers/</official_site>
        <corp_site>https://www.49ers.com/</corp_site>
      </team>
      ...
    </teams>

    Notes:
    - 'generated' is timezone-aware UTC (audit-friendly).
    - Sorted output keeps diffs clean in version control.
    """
    root = ET.Element("teams", {
        "generated": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "source": "Scraped from https://www.nfl.com/teams/",
        "league_name": LEAGUE_NAME,
        "league_abbr": LEAGUE_ABBR,
        "conferences": ",".join(CONFERENCES),
    })

    def s_key(t: Dict) -> Tuple:
        return (t.get("conference",""), t.get("division",""),
                t.get("representing_name",""), t.get("nickname",""))

    for r in sorted(rows, key=s_key):
        te = ET.SubElement(root, "team")
        ET.SubElement(te, "representing_name").text = r["representing_name"]
        ET.SubElement(te, "nickname").text = r["nickname"]
        ET.SubElement(te, "league_abbr").text = r["league_abbr"]
        if r.get("conference"): ET.SubElement(te, "conference").text = r["conference"]
        if r.get("conf_abbr"): ET.SubElement(te, "conf_abbr").text = r["conf_abbr"]
        if r.get("division"):   ET.SubElement(te, "division").text   = r["division"]
        ET.SubElement(te, "official_site").text = r["official_site"]
        if r.get("corp_site"):  ET.SubElement(te, "corp_site").text  = r["corp_site"]

    return ET.ElementTree(root)


def pretty_indent(elem: ET.Element, level: int = 0) -> None:
    """
    Human-friendly “pretty print” for ElementTree.
    - Adds newlines + two-space indentation between nodes.
    - Beware: pretty whitespace is technically part of text nodes; fine for this use case.
    """
    i = "\n" + level * "  "
    if len(elem):
        if not elem.text or not elem.text.strip():
            elem.text = i + "  "
        for child in elem:
            pretty_indent(child, level + 1)
        if not elem.tail or not elem.tail.strip():
            elem.tail = i
    else:
        if level and (not elem.tail or not elem.tail.strip()):
            elem.tail = i


def ensure_parent_dir(path: str) -> None:
    """Create the parent directory if needed so the script 'just works' on fresh clones."""
    d = os.path.dirname(path)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)


# =============================================================================
#                                    MAIN
# =============================================================================

def main() -> None:
    """
    Orchestrates the pipeline:
      1) GET the teams index page HTML.
      2) Parse anchors -> team rows.
      3) Build and write pretty XML at ../teams/teams_nfl.xml
    """
    html = get_html(INDEX_URL)
    soup = BeautifulSoup(html, "html.parser")

    rows = parse_index_page(soup)

    # Sanity check / teaching hint:
    if len(rows) < 24:  # 32 is expected; warn below 24 to catch layout changes
        print(f"[WARN] Parsed only {len(rows)} teams from NFL.com; the page layout may have changed.",
              file=sys.stderr)

    # Check for teams without a conference
    missing_confs = [r["representing_name"] + " " + r["nickname"] for r in rows if not r["conference"]]
    if missing_confs:
        print(f"[WARN] Teams without a conference: {', '.join(missing_confs)}", file=sys.stderr)

    ensure_parent_dir(OUTPUT_PATH)
    tree = build_xml(rows)
    pretty_indent(tree.getroot())
    tree.write(OUTPUT_PATH, encoding="utf-8", xml_declaration=True)

    print(f"Wrote {OUTPUT_PATH} with {len(rows)} teams.")


if __name__ == "__main__":
    main()