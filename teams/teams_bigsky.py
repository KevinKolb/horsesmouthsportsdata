#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fetch_ac.py
Append hardcoded 2025 American Athletic Conference (FBS) football members to teams.xml.

- Guaranteed to only contain the correct 14 football members for the 2025 season
- Skips any scraping/parsing to avoid Wikipedia noise
"""
import sys, shutil, os
from datetime import datetime
import xml.etree.ElementTree as ET

LEAGUE_ATTR = dict(name="NCAA", abbreviation="NCAA", country="USA", sport="College Football")
CONF_NAME = "American Athletic Conference"
CONF_ABBR = "AC"

# Hardcoded AAC football lineup for 2025
AAC_FOOTBALL_TEAMS_2025 = [
    "Army",
    "Charlotte",
    "East Carolina",
    "Florida Atlantic",
    "Memphis",
    "Navy",
    "North Texas",
    "Rice",
    "South Florida",
    "Temple",
    "Tulane",
    "Tulsa",
    "UAB",
    "UTSA"
]

def log(msg):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}")

def slugify(name: str) -> str:
    import re
    s = name.strip().lower()
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

def team_exists(root: ET.Element, team_id: str) -> bool:
    return root.find(f".//team[@id='{team_id}']") is not None

def build_team_element(name: str) -> ET.Element:
    slug = slugify(name)
    team_id = f"aac_{slug}"
    team = ET.Element("team", {"id": team_id})

    basic_info = ET.SubElement(team, "basic_info")
    ET.SubElement(basic_info, "name").text = name
    ET.SubElement(basic_info, "slug").text = slug
    ET.SubElement(basic_info, "established").text = ""
    ET.SubElement(basic_info, "league").text = "NCAA Division I FBS"
    ET.SubElement(basic_info, "league_abbr").text = "FBS"

    location = ET.SubElement(team, "location")
    ET.SubElement(location, "hometown").text = ""
    ET.SubElement(location, "population").text = ""

    visual = ET.SubElement(team, "visual_identity")
    ET.SubElement(visual, "primary_color").text = ""
    ET.SubElement(visual, "secondary_color").text = ""
    ET.SubElement(visual, "logo_url").text = ""
    ET.SubElement(visual, "header_background_url").text = ""

    org = ET.SubElement(team, "organization")
    ET.SubElement(org, "head_coach").text = ""
    ET.SubElement(org, "owners").text = ""

    venue = ET.SubElement(team, "venue")
    ET.SubElement(venue, "stadium").text = ""

    urls = ET.SubElement(team, "urls")
    ET.SubElement(urls, "official_url").text = ""
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
    # Single bucket division
    division = find_or_create(divisions, "division", match_attr={"name": "—"}, create_attr={"name": "—"})
    return division

def main():
    here = os.path.abspath(os.path.dirname(__file__) or ".")
    xml_path = os.path.join(here, "teams.xml")
    tree = ensure_tree(xml_path)
    root = tree.getroot()

    backup_file(xml_path)

    division_node = ensure_league_conference_division(root)
    members = AAC_FOOTBALL_TEAMS_2025

    added = skipped = 0
    for name in members:
        t = build_team_element(name)
        tid = t.get("id")
        if team_exists(root, tid):
            skipped += 1
            log(f"Skip (exists): {name} -> {tid}")
            continue
        division_node.append(t)
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
