#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fetch_ac.py — Append hardcoded 2025 AAC football teams to teams.xml,
enriching from CollegeFootballData (logos/colors/venue/coach) with safe writes.

Key points:
- Atomic write: builds teams.xml.tmp then replaces teams.xml on success
- Exact CFBD school names (no mascots), no fragile conference filter
- Venues filtered to those that list our school in teams[].school
- Hometown prefers venue "city, state"; otherwise branding.location
- Robust logging + backoff for 429s; no random/stale fallbacks
"""

import os, sys, shutil, re, time
from datetime import datetime
import requests
import xml.etree.ElementTree as ET
from functools import lru_cache

# ---------- Config ----------
LEAGUE_ATTR = dict(name="NCAA", abbreviation="NCAA", country="USA", sport="College Football")
CONF_NAME = "American Athletic Conference"
CONF_ABBR = "AC"
YEAR = 2025

CFBD_BASE = "https://api.collegefootballdata.com"
CFBD_KEY = "uec6arYek/ahsRs391Jina31sNXWeXjf8U3t/y59S7lKe11gw3aVUL1BQJPr31xf"  # baked key

# Display names you want + *exact* CFBD "school" strings
AAC_TEAMS = [
    ("Army", "Army"),
    ("Charlotte", "Charlotte"),
    ("East Carolina", "East Carolina"),
    ("Florida Atlantic", "Florida Atlantic"),
    ("Memphis", "Memphis"),
    ("Navy", "Navy"),
    ("North Texas", "North Texas"),
    ("Rice", "Rice"),
    ("South Florida", "South Florida"),
    ("Temple", "Temple"),
    ("Tulane", "Tulane"),
    ("Tulsa", "Tulsa"),
    ("UAB", "UAB"),
    ("UTSA", "UTSA"),
]

# ---------- Utils ----------
def log(msg: str):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}")

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
        ET.ElementTree(root).write(path, encoding="utf-8", xml_declaration=True)
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

def update_counts(root: ET.Element):
    root.set("total_leagues", str(len(root.findall(".//league"))))
    root.set("total_teams", str(len(root.findall(".//team"))))
    root.set("last_updated", datetime.utcnow().isoformat())

def backup_file(path: str):
    if os.path.exists(path):
        shutil.copy2(path, path + ".bak")
        log(f"Backed up to {path}.bak")

def atomic_write(tree: ET.ElementTree, final_path: str):
    tmp_path = final_path + ".tmp"
    ET.indent(tree, space="  ", level=0)
    tree.write(tmp_path, encoding="utf-8", xml_declaration=True)
    os.replace(tmp_path, final_path)

# ---------- CFBD client ----------
class CFBDApi:
    def __init__(self, base=CFBD_BASE, key=CFBD_KEY, timeout=25):
        if not key.strip():
            raise RuntimeError("CFBD API key missing.")
        self.base = base
        self.session = requests.Session()
        self.session.headers.update({"Authorization": f"Bearer {key}"})
        self.timeout = timeout

    def _get(self, path, params=None, max_attempts=4):
        url = f"{self.base}{path}"
        params = params or {}
        backoff = 1.0
        last_err = None
        for attempt in range(1, max_attempts + 1):
            try:
                r = self.session.get(url, params=params, timeout=self.timeout)
                if r.status_code == 429:
                    ra = r.headers.get("Retry-After")
                    wait = float(ra) if ra and ra.isdigit() else backoff
                    log(f"[CFBD 429] rate limited; sleeping {wait:.1f}s (attempt {attempt}/{max_attempts})")
                    time.sleep(wait)
                    backoff = min(backoff * 2, 8)
                    continue
                r.raise_for_status()
                return r.json()
            except requests.RequestException as e:
                last_err = e
                log(f"[CFBD] {path} failed (attempt {attempt}/{max_attempts}): {e}")
                time.sleep(backoff)
                backoff = min(backoff * 2, 8)
        raise last_err

    # Avoid conference filter weirdness: get all FBS teams, then pick ours.
    @lru_cache(maxsize=3)
    def teams_all(self, year):
        params = {}
        if year is not None:
            params["year"] = year
        return self._get("/teams/fbs", params)

    def branding_index(self):
        for y in (YEAR, 2024, None):
            try:
                data = self.teams_all(y)
                if data:
                    src = f"{y}" if y is not None else "no-year"
                    log(f"[branding] loaded {len(data)} FBS records from CFBD ({src})")
                    return { (d.get("school") or ""): d for d in data }
            except Exception as e:
                log(f"[branding] error ({y}): {e}")
        log("[branding] no FBS branding payload found")
        return {}

    @lru_cache(maxsize=None)
    def venues(self, team_label):
        return self._get("/venues", {"team": team_label}) or []

    @lru_cache(maxsize=None)
    def coaches(self, team_label, year):
        return self._get("/coaches", {"team": team_label, "year": year}) or []

# ---------- CFBD helpers ----------
def make_lookup_variants(branding_item: dict):
    vals = set()
    if not branding_item:
        return []
    school = (branding_item.get("school") or "").strip()
    abbr = (branding_item.get("abbreviation") or "").strip()
    a1 = (branding_item.get("alt_name1") or "").strip()
    a2 = (branding_item.get("alt_name2") or "").strip()
    a3 = (branding_item.get("alt_name3") or "").strip()
    for v in (school, abbr, a1, a2, a3):
        if v:
            vals.add(v)
    return [v for v in vals if v]

def filter_venues_to_school(venues_payload, school_label: str):
    out = []
    for v in venues_payload or []:
        teams = v.get("teams") or []
        schools = { (t.get("school") or "").strip() for t in teams if isinstance(t, dict) }
        if school_label in schools:
            out.append(v)
    return out

def pick_head_coach(coach_payload, year: int):
    if not coach_payload:
        return None
    best = None
    best_score = (-1, -1)
    for c in coach_payload:
        pos = (c.get("position") or "")
        is_hc = 1 if ("head" in pos.lower() and "coach" in pos.lower()) else 0
        latest = max((s.get("year", -1) for s in c.get("seasons", []) if isinstance(s, dict)), default=-1)
        has_target = any((s.get("year") == year) for s in c.get("seasons", []) if isinstance(s, dict))
        score = (is_hc + (1 if has_target else 0), latest)
        if score > best_score:
            best = c; best_score = score
    return best

# ---------- XML building ----------
def build_team_element(display_name: str, branding: dict | None, venues: list, coach: dict | None) -> ET.Element:
    slug = slugify(display_name)
    team_id = f"aac_{slug}"
    team = ET.Element("team", {"id": team_id})

    basic = ET.SubElement(team, "basic_info")
    ET.SubElement(basic, "name").text = display_name
    ET.SubElement(basic, "slug").text = slug
    ET.SubElement(basic, "established").text = ""
    ET.SubElement(basic, "league").text = "NCAA Division I FBS"
    ET.SubElement(basic, "league_abbr").text = "FBS"

    # Hometown: prefer venue city/state; else branding.location
    city_state = ""
    if venues:
        v0 = venues[0]
        city = (v0.get("city") or "").strip()
        state = (v0.get("state") or "").strip()
        city_state = ", ".join([p for p in (city, state) if p])
    loc = ET.SubElement(team, "location")
    ET.SubElement(loc, "hometown").text = city_state or (branding.get("location", "") if branding else "")
    ET.SubElement(loc, "population").text = ""

    vis = ET.SubElement(team, "visual_identity")
    ET.SubElement(vis, "primary_color").text = (branding.get("color") or "").strip() if branding else ""
    ET.SubElement(vis, "secondary_color").text = (branding.get("alt_color") or "").strip() if branding else ""
    logo = ""
    if branding and isinstance(branding.get("logos"), list) and branding["logos"]:
        logo = branding["logos"][0]
    ET.SubElement(vis, "logo_url").text = logo
    ET.SubElement(vis, "header_background_url").text = ""

    org = ET.SubElement(team, "organization")
    hc = ""
    if coach:
        first = coach.get("first_name") or ""
        last = coach.get("last_name") or ""
        hc = (first + " " + last).strip()
    ET.SubElement(org, "head_coach").text = hc
    ET.SubElement(org, "owners").text = ""

    ven = ET.SubElement(team, "venue")
    stadium = venues[0].get("name") if (venues and isinstance(venues[0], dict)) else ""
    ET.SubElement(ven, "stadium").text = stadium

    urls = ET.SubElement(team, "urls")
    ET.SubElement(urls, "official_url").text = ""
    ET.SubElement(urls, "operations_url").text = ""
    return team

def ensure_league_conference_division(root: ET.Element) -> ET.Element:
    league = None
    for lg in root.findall("league"):
        if all(lg.get(k) == v for k, v in LEAGUE_ATTR.items()):
            league = lg; break
    if league is None:
        league = ET.SubElement(root, "league", LEAGUE_ATTR)
    conferences = league.find("conferences") or ET.SubElement(league, "conferences")
    conference = find_or_create(
        conferences, "conference",
        match_attr={"name": CONF_NAME, "abbreviation": CONF_ABBR},
        create_attr={"name": CONF_NAME, "abbreviation": CONF_ABBR}
    )
    divisions = conference.find("divisions") or ET.SubElement(conference, "divisions")
    return find_or_create(divisions, "division", match_attr={"name": "—"}, create_attr={"name": "—"})

# ---------- Main ----------
def main():
    here = os.path.abspath(os.path.dirname(__file__) or ".")
    xml_path = os.path.join(here, "teams.xml")
    tree = ensure_tree(xml_path)
    root = tree.getroot()
    backup_file(xml_path)

    api = CFBDApi()
    branding_idx = api.branding_index()  # by school name

    division_node = ensure_league_conference_division(root)
    added = skipped = 0

    for display, school in AAC_TEAMS:
        branding = branding_idx.get(school)
        venues_list = []
        coach_pick = None

        if branding:
            tried = set()
            for label in make_lookup_variants(branding):
                if label in tried:
                    continue
                tried.add(label)

                # Venues
                try:
                    vraw = api.venues(label)
                    vmatch = filter_venues_to_school(vraw, school)
                    if vmatch and not venues_list:
                        venues_list = vmatch
                except Exception as e:
                    log(f"[venues] {display}/{label}: {e}")

                # Coaches
                try:
                    cpayload = api.coaches(label, YEAR)
                    if cpayload and not coach_pick:
                        coach_pick = pick_head_coach(cpayload, YEAR)
                except Exception as e:
                    log(f"[coaches] {display}/{label}: {e}")

                if venues_list and coach_pick:
                    break
        else:
            log(f"[branding-miss] {display} ({school}) not in CFBD payload; leaving fields blank")

        team_el = build_team_element(display, branding, venues_list, coach_pick)
        tid = team_el.get("id")

        if team_exists(root, tid):
            skipped += 1
            log(f"Skip (exists): {display} -> {tid}")
            continue

        division_node.append(team_el)
        added += 1
        log(f"Added: {display} | branding={'Y' if branding else 'N'} "
            f"venue={'Y' if venues_list else 'N'} coach={'Y' if coach_pick else 'N'}")

    update_counts(root)
    # Atomic write to avoid partial/corrupt XML
    atomic_write(tree, xml_path)
    log(f"Done. Added {added}, skipped {skipped}. Updated {xml_path}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log(f"ERROR: {e}")
        sys.exit(1)
