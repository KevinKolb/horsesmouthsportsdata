#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fetch_sec.py
Append NCAA FBS SEC football members to teams.xml (same schema as your sample).

- Source: Wikipedia "Southeastern Conference"
- SEC has no divisions now (single bucket)
- Idempotent: skips teams already present (by @id)
- Makes teams.xml.bak before writing
- ENHANCEMENTS: fills official athletics URL and logo from Wikipedia program page
"""
import os, re, time, shutil, sys
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET
from typing import List, Dict, Optional

WIKI_URL = "https://en.wikipedia.org/wiki/Southeastern_Conference"
HDRS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/124.0.0.0 Safari/537.36")
}
TIMEOUT = 20
RETRIES = 3
SLEEP_BETWEEN = 1.0

# XML placement
LEAGUE_ATTR = dict(name="NCAA", abbreviation="NCAA", country="USA", sport="College Football")
CONF_NAME = "Southeastern Conference"
CONF_ABBR = "SEC"
DIV_NAME = "—"  # single bucket

# Known-official athletics URLs (authoritative)
ATHLETICS_URL = {
    "Alabama": "https://rolltide.com",
    "Arkansas": "https://arkansasrazorbacks.com",
    "Auburn": "https://auburntigers.com",
    "Florida": "https://floridagators.com",
    "Georgia": "https://georgiadogs.com",
    "Kentucky": "https://ukathletics.com",
    "LSU": "https://lsusports.net",
    "Ole Miss": "https://olemisssports.com",
    "Mississippi State": "https://hailstate.com",
    "Missouri": "https://mutigers.com",
    "South Carolina": "https://gamecocksonline.com",
    "Tennessee": "https://utsports.com",
    "Texas A&M": "https://12thman.com",
    "Vanderbilt": "https://vucommodores.com",
    "Oklahoma": "https://soonersports.com",
    "Texas": "https://texassports.com",
}

# Wikipedia athletics program page titles for infobox logos
WIKI_PROGRAM_TITLE = {
    "Alabama": "Alabama Crimson Tide",
    "Arkansas": "Arkansas Razorbacks",
    "Auburn": "Auburn Tigers",
    "Florida": "Florida Gators",
    "Georgia": "Georgia Bulldogs",
    "Kentucky": "Kentucky Wildcats",
    "LSU": "LSU Tigers and Lady Tigers",
    "Ole Miss": "Ole Miss Rebels",
    "Mississippi State": "Mississippi State Bulldogs",
    "Missouri": "Missouri Tigers",
    "South Carolina": "South Carolina Gamecocks",
    "Tennessee": "Tennessee Volunteers",
    "Texas A&M": "Texas A&M Aggies",
    "Vanderbilt": "Vanderbilt Commodores",
    "Oklahoma": "Oklahoma Sooners",
    "Texas": "Texas Longhorns",
}

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

def get(url):
    last = None
    for i in range(RETRIES):
        try:
            r = requests.get(url, headers=HDRS, timeout=TIMEOUT)
            r.raise_for_status()
            return r
        except Exception as e:
            last = e
            log(f"GET failed ({i+1}/{RETRIES}): {e}")
            time.sleep(SLEEP_BETWEEN)
    raise last

def slugify(s: str) -> str:
    s = s.strip().lower()
    s = re.sub(r"[’'`]", "", s)
    s = re.sub(r"[^a-z0-9]+", "-", s).strip("-")
    return s

def ensure_tree(path: str) -> ET.ElementTree:
    if not os.path.exists(path):
        root = ET.Element("sports_teams", {
            "last_updated": datetime.utcnow().isoformat(),
            "total_teams": "0",
            "total_leagues": "0",
        })
        ET.SubElement(root, "league", LEAGUE_ATTR)
        tree = ET.ElementTree(root)
        tree.write(path, encoding="utf-8", xml_declaration=True)
        log(f"Created new {path}")
    return ET.parse(path)

def find_or_create(parent, tag, match_attr=None, create_attr=None):
    match_attr = match_attr or {}
    for child in parent.findall(tag):
        if all(child.get(k) == v for k, v in match_attr.items()):
            return child
    return ET.SubElement(parent, tag, create_attr or match_attr)

def ensure_league_conference_division(root) -> ET.Element:
    league = None
    for lg in root.findall("league"):
        if all(lg.get(k) == v for k, v in LEAGUE_ATTR.items()):
            league = lg
            break
    if league is None:
        league = ET.SubElement(root, "league", LEAGUE_ATTR)

    conferences = league.find("conferences") or ET.SubElement(league, "conferences")
    conference = find_or_create(
        conferences, "conference",
        match_attr={"name": CONF_NAME, "abbreviation": CONF_ABBR},
        create_attr={"name": CONF_NAME, "abbreviation": CONF_ABBR}
    )
    divisions = conference.find("divisions") or ET.SubElement(conference, "divisions")
    division = find_or_create(divisions, "division", match_attr={"name": DIV_NAME}, create_attr={"name": DIV_NAME})
    return division

def team_exists(root: ET.Element, team_id: str) -> bool:
    return root.find(f".//team[@id='{team_id}']") is not None

def build_team_element(name: str, official_url: str = "", logo_url: str = "") -> ET.Element:
    slug = slugify(name)
    tid = f"sec_{slug}"
    team = ET.Element("team", {"id": tid})

    basic = ET.SubElement(team, "basic_info")
    ET.SubElement(basic, "name").text = name
    ET.SubElement(basic, "slug").text = slug
    ET.SubElement(basic, "established").text = ""
    ET.SubElement(basic, "league").text = "NCAA Division I FBS"
    ET.SubElement(basic, "league_abbr").text = "FBS"

    loc = ET.SubElement(team, "location")
    ET.SubElement(loc, "hometown").text = ""
    ET.SubElement(loc, "population").text = ""

    vis = ET.SubElement(team, "visual_identity")
    ET.SubElement(vis, "primary_color").text = ""
    ET.SubElement(vis, "secondary_color").text = ""
    ET.SubElement(vis, "logo_url").text = logo_url or ""
    ET.SubElement(vis, "header_background_url").text = ""

    org = ET.SubElement(team, "organization")
    ET.SubElement(org, "head_coach").text = ""
    ET.SubElement(org, "owners").text = ""

    ven = ET.SubElement(team, "venue")
    ET.SubElement(ven, "stadium").text = ""

    urls = ET.SubElement(team, "urls")
    ET.SubElement(urls, "official_url").text = official_url or ""
    ET.SubElement(urls, "operations_url").text = ""

    return team

def update_counts(root: ET.Element):
    root.set("total_leagues", str(len(root.findall(".//league"))))
    root.set("total_teams", str(len(root.findall(".//team"))))
    root.set("last_updated", datetime.utcnow().isoformat())

def backup_file(path: str):
    if os.path.exists(path):
        shutil.copy2(path, path + ".bak")
        log(f"Backed up to {path}.bak")

def _normalize_img_src(src: str) -> str:
    # Wikipedia uses protocol-relative URLs in thumbnails; normalize to https
    if src.startswith("//"):
        return "https:" + src
    return src

def fetch_program_logo_from_wiki(title: str) -> Optional[str]:
    """
    Given a Wikipedia page title for the athletics program, return the first
    infobox image thumbnail URL, if present.
    """
    try:
        url = "https://en.wikipedia.org/wiki/" + title.replace(" ", "_")
        soup = BeautifulSoup(get(url).text, "html.parser")
        infobox = soup.select_one("table.infobox")
        if not infobox:
            return None
        # common pattern: first .image img inside infobox
        img = infobox.select_one(".image img")
        if not img:
            return None
        src = img.get("src") or ""
        return _normalize_img_src(src)
    except Exception as e:
        log(f"Logo fetch failed for {title}: {e}")
        return None

def scrape_sec_members() -> List[Dict[str, str]]:
    """
    Scrape current SEC *member institutions* from the 'Member institutions'
    table on Wikipedia, map to athletics brands, and enrich with athletics URL
    and program logo URL (from the program page’s infobox).
    """
    log(f"Scraping {WIKI_URL}")
    soup = BeautifulSoup(get(WIKI_URL).text, "html.parser")

    # Anchor on the correct section to avoid unrelated tables
    anchor = soup.select_one("span#Member_institutions") or soup.select_one("span#Members")
    if not anchor:
        raise RuntimeError("Could not find the 'Member institutions' section on the SEC page.")
    h2 = anchor.find_parent(["h2", "h3"])
    if not h2:
        raise RuntimeError("Could not locate the heading parent for the member institutions section.")
    table = h2.find_next("table", class_="wikitable")
    if not table:
        raise RuntimeError("Could not find the member institutions table after the heading.")

    headers = [th.get_text(strip=True).lower() for th in table.select("tr th")]
    if not headers or not any(h in ("school", "institution", "university", "member", "college") for h in headers):
        raise RuntimeError(f"Unexpected headers for members table: {headers}")

    raw_names = []
    for row in table.select("tr"):
        cells = row.find_all("td")
        if not cells:
            continue
        first_td = cells[0]
        link = first_td.find("a")
        name = (link.get_text(" ", strip=True) if link else first_td.get_text(" ", strip=True)).strip()
        name = re.sub(r"\s*\[.*?\]\s*", "", name)
        if name:
            raw_names.append(name)

    brand_map = {
        "University of Alabama": "Alabama",
        "Auburn University": "Auburn",
        "University of Florida": "Florida",
        "University of Georgia": "Georgia",
        "University of Kentucky": "Kentucky",
        "Louisiana State University": "LSU",
        "University of Mississippi": "Ole Miss",
        "Mississippi State University": "Mississippi State",
        "University of Missouri": "Missouri",
        "University of South Carolina": "South Carolina",
        "University of Tennessee": "Tennessee",
        "Texas A&M University": "Texas A&M",
        "Vanderbilt University": "Vanderbilt",
        "University of Arkansas": "Arkansas",
        "University of Oklahoma": "Oklahoma",
        "The University of Texas at Austin": "Texas",
        # occasional short forms:
        "Texas A&M": "Texas A&M",
        "Ole Miss": "Ole Miss",
    }

    def looks_like_institution(s: str) -> bool:
        inst_tokens = ("university", "college", "state", "a&m", "mississippi", "tennessee", "carolina")
        s_low = s.lower()
        return any(t in s_low for t in inst_tokens) or s in brand_map.values()

    cleaned = set()
    for n in raw_names:
        n2 = re.sub(r"\s*\(.*?\)$", "", n).strip()
        if not looks_like_institution(n2):
            continue
        cleaned.add(brand_map.get(n2, n2))

    must_have = {
        "Alabama","Arkansas","Auburn","Florida","Georgia","Kentucky","LSU",
        "Ole Miss","Mississippi State","Missouri","South Carolina","Tennessee",
        "Texas A&M","Vanderbilt","Oklahoma","Texas"
    }
    cleaned.update(must_have)

    brands = sorted(cleaned, key=lambda x: x.lower())
    log(f"SEC members detected: {', '.join(brands)}")

    # Enrich with official athletics URL + Wikipedia infobox logo
    out: List[Dict[str, str]] = []
    for b in brands:
        official = ATHLETICS_URL.get(b, "")
        wiki_title = WIKI_PROGRAM_TITLE.get(b)
        logo = fetch_program_logo_from_wiki(wiki_title) if wiki_title else ""
        out.append({"name": b, "official_url": official, "logo_url": logo or ""})
        if official or logo:
            log(f"Enriched {b}: official_url={bool(official)} logo_url={bool(logo)}")
        else:
            log(f"No enrichment for {b}")
    return out

def main():
    here = os.path.abspath(os.path.dirname(__file__) or ".")
    xml_path = os.path.join(here, "teams.xml")
    tree = ensure_tree(xml_path)
    root = tree.getroot()

    backup_file(xml_path)
    division_node = ensure_league_conference_division(root)

    members = scrape_sec_members()  # list of dicts

    added = skipped = 0
    for m in members:
        name = m["name"]
        official_url = m.get("official_url", "")
        logo_url = m.get("logo_url", "")

        el = build_team_element(name, official_url=official_url, logo_url=logo_url)
        tid = el.get("id")
        if team_exists(root, tid):
            skipped += 1
            log(f"Skip (exists): {name} -> {tid}")
            continue
        division_node.append(el)
        added += 1
        log(f"Added: {name} -> {tid}")

    update_counts(root)
    ET.indent(tree, space="  ", level=0)
    tree.write(xml_path, encoding="utf-8", xml_declaration=True)
    log(f"Done. Added {added}, skipped {skipped}. Updated {xml_path}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log(f"ERROR: {e}")
        sys.exit(1)
