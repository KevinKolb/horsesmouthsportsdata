import os
import time
import requests
from bs4 import BeautifulSoup
from lxml import etree
from urllib.parse import urljoin

BASE_URL = "https://www.nfl.com"
TEAMS_URL = f"{BASE_URL}/teams"
OUTPUT_DIR = "data"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "nfl_teams.xml")
OPERATIONS_URL = "https://operations.nfl.com/learn-the-game/nfl-basics/team-histories/"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}


def fetch_teams():
    res = requests.get(TEAMS_URL, headers=HEADERS)
    soup = BeautifulSoup(res.text, "html.parser")
    promos = soup.select("div.nfl-c-custom-promo")

    teams = []
    for promo in promos:
        try:
            name_tag = promo.select_one("h4 p")
            link_tag = promo.select_one("a[title^='View'][href*='/teams/']")
            img_tag = promo.select_one("img[alt]")

            if not (name_tag and link_tag and img_tag):
                continue

            name = name_tag.text.strip()
            href = link_tag["href"].strip()
            profile_url = urljoin(BASE_URL, href)
            slug = href.strip("/").split("/")[-1]
            logo_url = img_tag.get("data-src", "")

            content_div = promo.select_one(".nfl-c-custom-promo__content")
            background_image = ""
            if content_div:
                style_attr = content_div.get("style", "")
                if "background-image" in style_attr:
                    start = style_attr.find("url(")
                    end = style_attr.find(")", start)
                    if start != -1 and end != -1:
                        background_image = style_attr[start + 4:end].strip('"\'')  # Clean quotes

            city, state = "", ""
            if " " in name:
                parts = name.rsplit(" ", 1)
                if len(parts[1]) <= 3:
                    city, state = parts[0], parts[1]

            teams.append({
                "name": name,
                "slug": slug,
                "url": profile_url,
                "logo": logo_url,
                "background": background_image,
                "city": city,
                "state": state
            })
        except Exception as e:
            print(f"⚠️ Skipping a team due to error: {e}")
    return teams


def fetch_team_profile(team_url):
    res = requests.get(team_url, headers=HEADERS)
    soup = BeautifulSoup(res.text, "html.parser")
    section = soup.select_one(".nfl-c-team-info__content")
    data = {}
    if section:
        for li in section.select(".d3-o-list__item"):
            key_tag = li.select_one(".nfl-c-team-info__info-key")
            val_tag = li.select_one(".nfl-c-team-info__info-value")
            if key_tag and val_tag:
                key = key_tag.text.strip()
                val = val_tag.text.strip()
                data[key] = val
    return data


def fetch_conference_division():
    res = requests.get(OPERATIONS_URL, headers=HEADERS)
    soup = BeautifulSoup(res.text, "html.parser")

    team_info = {}

    for conference_section in soup.select(".team-histories__teams > h5"):
        conf_name = conference_section.text.strip().replace("Select a Team Below:", "").strip()
        conf_abbr = conference_section.select_one("abbr")
        conference = conf_abbr.text.strip() if conf_abbr else conf_name

        div_section = conference_section.find_next_sibling("div", class_="team-histories__team-category")
        while div_section:
            division_name = div_section.select_one("h6")
            division = division_name.text.strip() if division_name else ""

            for card in div_section.select(".team-histories__team"):
                name = card.text.strip()
                team_info[name] = {
                    "conference": conference,
                    "division": f"{conference} {division}"
                }

            # Move to next division section
            next_sibling = div_section.find_next_sibling()
            if next_sibling and next_sibling.name == "div" and "team-histories__team-category" in next_sibling.get("class", []):
                div_section = next_sibling
            else:
                break

    return team_info


def save_to_xml(teams):
    root = etree.Element("teams")

    for team in teams:
        t = etree.SubElement(root, "team")
        etree.SubElement(t, "name").text = team["name"]
        etree.SubElement(t, "slug").text = team["slug"]
        etree.SubElement(t, "url").text = team["url"]
        etree.SubElement(t, "logo").text = team["logo"]
        etree.SubElement(t, "background").text = team["background"]
        etree.SubElement(t, "city").text = team["city"]
        etree.SubElement(t, "state").text = team["state"]
        etree.SubElement(t, "conference").text = team.get("conference", "")
        etree.SubElement(t, "division").text = team.get("division", "")

        # Add league
        etree.SubElement(t, "league").text = "National Football League"
        etree.SubElement(t, "league_abbr").text = "NFL"

        for field in ["Head Coach", "Stadium", "Owners", "Established"]:
            if field in team:
                tag = field.lower().replace(" ", "_")
                etree.SubElement(t, tag).text = team[field]

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    tree = etree.ElementTree(root)
    tree.write(OUTPUT_FILE, pretty_print=True, encoding="utf-8", xml_declaration=True)
    print(f"\n✅ Wrote {len(teams)} teams to {OUTPUT_FILE}")


def main():
    print("📥 Scraping team list...")
    teams = fetch_teams()

    print("📘 Scraping conference/division data...")
    conf_div_map = fetch_conference_division()

    print("🔍 Scraping profile data (with delay)...")
    for team in teams:
        print(f"→ {team['name']} ({team['url']})")
        profile_data = fetch_team_profile(team["url"])
        team.update(profile_data)

        # Match team by name for conference/division
        if team["name"] in conf_div_map:
            team.update(conf_div_map[team["name"]])
        else:
            print(f"   ⚠ No conference/division info for {team['name']}")

        time.sleep(1)

    save_to_xml(teams)


if __name__ == "__main__":
    main()
