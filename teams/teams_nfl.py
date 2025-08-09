import os
import time
import shutil
import requests
from bs4 import BeautifulSoup
from lxml import etree
from urllib.parse import urljoin
from datetime import datetime
import re

BASE_URL = "https://www.nfl.com"
TEAMS_URL = f"{BASE_URL}/teams"
OUTPUT_FILE = "teams/teams.xml"
BACKUP_FILE = "teams/teamsbackup.xml"
LOG_FILE = "teams/log.txt"
OPERATIONS_BASE_URL = "https://operations.nfl.com/learn-the-game/nfl-basics/team-histories"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

def log_message(message):
    """Write message to both console and log file with timestamp"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_entry = f"[{timestamp}] {message}"
    print(message)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(log_entry + "\n")

def count_errors_in_xml():
    """Count how many ERROR values are in the generated XML file"""
    if not os.path.exists(OUTPUT_FILE):
        return 0
    try:
        with open(OUTPUT_FILE, 'r', encoding='utf-8') as f:
            content = f.read()
            error_count = content.count('>ERROR<')
            return error_count
    except Exception as e:
        log_message(f"ERROR_COUNT_FAILED: {str(e)}")
        return -1

def create_backup():
    """Create backup of current teams.xml file before making changes."""
    if not os.path.exists(OUTPUT_FILE):
        log_message("BACKUP_SKIP: No existing teams.xml file to backup")
        return False
    
    try:
        if os.path.exists(BACKUP_FILE):
            os.remove(BACKUP_FILE)
        shutil.copy2(OUTPUT_FILE, BACKUP_FILE)
        log_message(f"BACKUP_CREATED: {BACKUP_FILE}")
        return True
    except Exception as e:
        log_message(f"BACKUP_FAILED: {str(e)}")
        return False

def load_xml_teams(filepath):
    """Load teams from an XML file into a dictionary."""
    if not os.path.exists(filepath):
        return {}
    
    try:
        tree = etree.parse(filepath)
        root = tree.getroot()
        teams_dict = {}
        team_elements = root.findall('.//team')
        
        for team_elem in team_elements:
            name_elem = team_elem.find('.//name')
            if name_elem is None:
                name_elem = team_elem.find('name')
            
            name_text = name_elem.text if name_elem is not None else ''
            
            if name_text:
                team_data = {}
                for elem in team_elem.iter():
                    if elem.text and elem.tag != 'team':
                        team_data[elem.tag] = elem.text
                teams_dict[name_text] = team_data
        
        return teams_dict
    except Exception as e:
        log_message(f"XML_READ_ERROR: {filepath} - {str(e)}")
        return {}

def detect_actual_changes(current_teams, backup_teams):
    """Detect if there are actual changes between current and backup teams."""
    if not backup_teams:
        return True
    
    current_teams_dict = {team['name']: team for team in current_teams}
    
    if len(current_teams_dict) != len(backup_teams):
        log_message(f"CHANGE_DETECTED: Team count differs ({len(backup_teams)} → {len(current_teams_dict)})")
        return True
    
    fields_to_compare = ['slug', 'hometown', 'population', 'primary_color', 'secondary_color', 'conference', 'division', 'head_coach', 'stadium', 'owners', 'established', 'league', 'league_abbr', 'division_name', 'url', 'operations_url', 'logo', 'header_background_url']
    
    for team_name, current_team in current_teams_dict.items():
        if team_name not in backup_teams:
            log_message(f"CHANGE_DETECTED: New team found: {team_name}")
            return True
        
        backup_team = backup_teams[team_name]
        
        for field in fields_to_compare:
            current_val = str(current_team.get(field, ''))
            backup_val = str(backup_team.get(field, ''))
            
            if current_val != backup_val:
                log_message(f"CHANGE_DETECTED: {team_name}.{field}: '{backup_val}' → '{current_val}'")
                return True
    
    for team_name in backup_teams:
        if team_name not in current_teams_dict:
            log_message(f"CHANGE_DETECTED: Team removed: {team_name}")
            return True
    
    log_message("NO_CHANGES: Current data is identical to backup")
    return False

def fetch_nfl_league_logo():
    """Fetch the main NFL league logo from NFL.com"""
    log_message("NFL_LOGO_FETCH_START: Attempting to extract NFL league logo")
    
    try:
        # Try main NFL.com page first
        res = requests.get(BASE_URL, headers=HEADERS)
        soup = BeautifulSoup(res.text, "html.parser")
        
        # Look for NFL logo in common locations
        logo_selectors = [
            "img[alt*='NFL']",
            "img[src*='nfl-logo']",
            "img[src*='nfl_logo']",
            ".nfl-logo img",
            ".logo img",
            "header img[alt*='National Football League']",
            "nav img[alt*='NFL']",
            "img[data-src*='nfl-logo']"
        ]
        
        for selector in logo_selectors:
            logo_imgs = soup.select(selector)
            for img in logo_imgs:
                src = img.get('data-src') or img.get('src', '')
                alt = img.get('alt', '').lower()
                
                if src and ('nfl' in alt or 'logo' in src.lower()):
                    # Clean and validate the URL
                    if src.startswith('//'):
                        src = 'https:' + src
                    elif src.startswith('/'):
                        src = urljoin(BASE_URL, src)
                    
                    log_message(f"NFL_LOGO_FOUND: {src}")
                    return src
        
        # Fallback: try to find any logo from the header/nav area
        header_nav = soup.select("header, nav, .header, .nav")
        for section in header_nav:
            imgs = section.select("img")
            for img in imgs:
                src = img.get('data-src') or img.get('src', '')
                if src and any(term in src.lower() for term in ['logo', 'nfl']):
                    if src.startswith('//'):
                        src = 'https:' + src
                    elif src.startswith('/'):
                        src = urljoin(BASE_URL, src)
                    
                    log_message(f"NFL_LOGO_FOUND_FALLBACK: {src}")
                    return src
        
        # If no logo found on main page, try teams page
        log_message("NFL_LOGO_MAIN_FAILED: Trying teams page")
        res = requests.get(TEAMS_URL, headers=HEADERS)
        soup = BeautifulSoup(res.text, "html.parser")
        
        for selector in logo_selectors:
            logo_imgs = soup.select(selector)
            for img in logo_imgs:
                src = img.get('data-src') or img.get('src', '')
                alt = img.get('alt', '').lower()
                
                if src and ('nfl' in alt or 'logo' in src.lower()):
                    if src.startswith('//'):
                        src = 'https:' + src
                    elif src.startswith('/'):
                        src = urljoin(BASE_URL, src)
                    
                    log_message(f"NFL_LOGO_FOUND_TEAMS: {src}")
                    return src
        
        # Ultimate fallback - use a known NFL logo URL structure
        fallback_url = "https://static.www.nfl.com/image/private/f_auto/league/u9fltoslqdsyao8cpm0k"
        log_message(f"NFL_LOGO_FALLBACK_URL: Using fallback URL {fallback_url}")
        return fallback_url
        
    except Exception as e:
        log_message(f"NFL_LOGO_ERROR: {str(e)}")
        return "ERROR"

def fetch_teams_from_nfl():
    """Fetch basic team info from NFL.com teams page, prioritizing vibrant background images"""
    log_message("FETCH_BASIC_START: Scraping basic team list from NFL.com")
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
                log_message("TEAM_SKIP: Missing name, link, or image tag")
                continue

            name = name_tag.text.strip()
            href = link_tag["href"].strip()
            profile_url = urljoin(BASE_URL, href)
            slug = href.strip("/").split("/")[-1]
            logo_url = img_tag.get("data-src", img_tag.get("src", ""))

            content_div = promo.select_one(".nfl-c-custom-promo__content")
            background_image = "ERROR"
            if content_div:
                style_attr = content_div.get("style", "")
                if "background-image" in style_attr:
                    start = style_attr.find("url(")
                    end = style_attr.find(")", start)
                    if start != -1 and end != -1:
                        background_image = style_attr[start + 4:end].strip('"\'')
                        background_image = urljoin(BASE_URL, background_image)
                        if "f_auto/league" in background_image:
                            log_message(f"BACKGROUND_FOUND: {name} - {background_image}")
                        else:
                            log_message(f"BACKGROUND_WARNING: {name} - Found {background_image}, but it may not be the vibrant version")
                            background_image = "ERROR"
                    else:
                        log_message(f"BACKGROUND_NOT_FOUND: {name} - Invalid background-image style")
                else:
                    log_message(f"BACKGROUND_NOT_FOUND: {name} - No background-image in style")
            else:
                log_message(f"BACKGROUND_NOT_FOUND: {name} - No content div found")

            teams.append({
                "name": name,
                "slug": slug,
                "url": profile_url,
                "logo": logo_url,
                "header_background_url": background_image
            })
        except Exception as e:
            log_message(f"TEAM_SKIP_ERROR: Skipping a team due to error: {e}")
    
    log_message(f"FETCH_BASIC_COMPLETE: Found {len(teams)} teams")
    if len(teams) != 32:
        log_message(f"WARNING: Expected 32 teams, but found {len(teams)}")
    else:
        log_message("SUCCESS: Confirmed 32 teams fetched")
    
    # Validate background images
    error_backgrounds = sum(1 for team in teams if team["header_background_url"] == "ERROR")
    if error_backgrounds > 0:
        log_message(f"WARNING: {error_backgrounds} teams have missing or invalid background images")
    
    return teams

def fetch_team_profile(team_url):
    """Fetch team profile data from NFL.com team page"""
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
    """Fetch conference/division data and return mapping"""
    log_message("CONFERENCE_FETCH_START: Scraping conference/division data")
    
    operations_url = "https://operations.nfl.com/learn-the-game/nfl-basics/team-histories/"
    
    try:
        res = requests.get(operations_url, headers=HEADERS)
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
                        "conference": conference[:3].upper(),
                        "division": f"{conference[:3].upper()} {division}",
                        "division_name": division
                    }

                next_sibling = div_section.find_next_sibling()
                if next_sibling and next_sibling.name == "div" and "team-histories__team-category" in next_sibling.get("class", []):
                    div_section = next_sibling
                else:
                    break

        log_message(f"CONFERENCE_FETCH_COMPLETE: Found conference/division info for {len(team_info)} teams")
        return team_info
        
    except Exception as e:
        log_message(f"CONFERENCE_FETCH_ERROR: {str(e)}")
        return {}

def generate_operations_url(team_name, conference_info):
    """Generate operations.nfl.com URL from team data"""
    if not conference_info:
        return "ERROR"
    
    conference_abbr = conference_info.get('conference', '').lower()
    division_name = conference_info.get('division_name', '').lower()
    
    conf_url_map = {
        'afc': 'american-football-conference',
        'nfc': 'national-football-conference'
    }
    
    conference_url = conf_url_map.get(conference_abbr, '')
    if not conference_url:
        return "ERROR"
    
    team_slug = create_team_slug(team_name)
    return f"{OPERATIONS_BASE_URL}/{conference_url}/{division_name}/{team_slug}/"

def create_team_slug(team_name):
    """Create URL-friendly slug from team name"""
    slug = team_name.lower()
    slug = re.sub(r'\s+', '-', slug)
    slug = re.sub(r'[^\w\-]', '', slug)
    slug = re.sub(r'-+', '-', slug)
    slug = slug.strip('-')
    return slug

def fetch_team_operations_data(team_name, operations_url):
    """Exhaustively fetch detailed team data from operations.nfl.com team history pages"""
    if not operations_url or operations_url == "ERROR":
        log_message(f"OPERATIONS_SKIP: {team_name} - No operations URL")
        return {
            'hometown': 'ERROR',
            'population': 'ERROR',
            'primary_color': 'ERROR',
            'secondary_color': 'ERROR',
        }
    
    log_message(f"OPERATIONS_FETCH: {team_name} from {operations_url}")
    
    try:
        response = requests.get(operations_url, headers=HEADERS)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        team_data = {
            'hometown': 'ERROR',
            'population': 'ERROR',
            'primary_color': 'ERROR',
            'secondary_color': 'ERROR',
        }
        
        # Extract text for regex matching
        page_text = soup.get_text(separator=' ', strip=True)
        log_message(f"OPERATIONS_DEBUG: {team_name} - Page length: {len(page_text)} chars")
        
        # Try extracting hometown from slide2 specifically for Washington Commanders
        if "Commanders" in team_name:
            slide2 = soup.select_one(".slide2 .slide-content .content")
            if slide2:
                hometown_p = slide2.find("p")
                if hometown_p and hometown_p.text.strip():
                    hometown = hometown_p.text.strip()
                    hometown = re.sub(r'\s+', ' ', hometown)
                    # Remove trailing period if present but preserve the exact scraped format
                    hometown = hometown.rstrip('.')
                    if hometown and len(hometown) > 2:
                        team_data['hometown'] = hometown
                        log_message(f"OPERATIONS_HOMETOWN_FOUND_SLIDE2: {team_name} - {hometown}")
        
        # Fallback to regex patterns if not found or for other teams
        if team_data['hometown'] == 'ERROR':
            location_patterns = [
                r'Hometown[:\s]+([A-Za-z\s,]+?)(?:\s+population|\s*$|\n)',
                r'hometown[:\s]+([A-Za-z\s,]+?)(?:\s+population|\s*$|\n)',
                r'Location[:\s]+([A-Za-z\s,]+?)(?:\s+population|\s*$|\n)',
                r'location[:\s]+([A-Za-z\s,]+?)(?:\s+population|\s*$|\n)',
                r'Based in[:\s]+([A-Za-z\s,]+?)(?:\s+population|\s*$|\n)',
                r'based in[:\s]+([A-Za-z\s,]+?)(?:\s+population|\s*$|\n)',
                r'located in[:\s]+([A-Za-z\s,]+?)(?:\s+population|\s*$|\n)',
                r'City[:\s]+([A-Za-z\s,]+?)(?:\s+population|\s*$|\n)',
                r'city[:\s]+([A-Za-z\s,]+?)(?:\s+population|\s*$|\n)',
                r'([A-Za-z\s]+),\s*([A-Z]{2})\s*metropolitan',
                r'greater\s+([A-Za-z\s]+)\s+area',
                r'metropolitan\s+([A-Za-z\s,]+?)\s+area',
                r'(?<=Hometown\s)([A-Za-z\s,]+?)(?=\s+Population|\s*$|\n)',
                r'Washington D\.C\.(?=\s+Population|\s*$|\n)',
            ]
            
            for pattern in location_patterns:
                match = re.search(pattern, page_text, re.IGNORECASE)
                if match:
                    if pattern == r'Washington D\.C\.(?=\s+Population|\s*$|\n)':
                        hometown = "Washington D.C."
                        log_message(f"OPERATIONS_HOMETOWN_FOUND: {team_name} - {hometown} (regex)")
                    elif len(match.groups()) >= 2 and match.group(2):
                        hometown = f"{match.group(1).strip()}, {match.group(2).strip()}"
                    else:
                        hometown = match.group(1).strip()
                    
                    hometown = re.sub(r'\s+(area|region|metropolitan|population)$', '', hometown, flags=re.I)
                    hometown = re.sub(r'\s+population\s*', ' ', hometown, flags=re.I).strip()
                    hometown = re.sub(r'\s+', ' ', hometown)
                    
                    if hometown and len(hometown) > 2:
                        team_data['hometown'] = hometown
                        log_message(f"OPERATIONS_HOMETOWN_FOUND_REGEX: {team_name} - {hometown}")
                        break
        
        # Log if hometown is still missing
        if team_data['hometown'] == 'ERROR':
            log_message(f"OPERATIONS_HOMETOWN_NOT_FOUND: {team_name} - No hometown detected")
        
        color_patterns = [
            r'Team Colors[:\s]*([A-Za-z\s]+)\/([A-Za-z\s]+)',
            r'team colors[:\s]*([A-Za-z\s]+)\/([A-Za-z\s]+)',
            r'Colors[:\s]*([A-Za-z\s]+)\/([A-Za-z\s]+)',
            r'colors[:\s]*([A-Za-z\s]+)\/([A-Za-z\s]+)',
            r'Primary[:\s]*([A-Za-z\s]+)[,\s]*Secondary[:\s]*([A-Za-z\s]+)',
            r'primary[:\s]*([A-Za-z\s]+)[,\s]*secondary[:\s]*([A-Za-z\s]+)',
            r'(cardinal|dark navy|navy blue|navy|blue|red|green|gold|old gold|silver|black|white|orange|purple|yellow|maroon|teal|brown|gray|grey|action green|wolf grey)[\s]*[/\\][\s]*(cardinal|dark navy|navy blue|navy|blue|red|green|gold|old gold|silver|black|white|orange|purple|yellow|maroon|teal|brown|gray|grey|action green|wolf grey)',
            r'team colors?[:\s]+(?:are\s+)?([a-z\s]+?)(?:\s+and\s+([a-z\s]+?))?(?:\.|$|\n)',
            r'(?<!team\s)colors?[:\s]+(?:are\s+)?([a-z\s]+?)(?:\s+and\s+([a-z\s]+?))?(?:\.|$|\n)',
            r'primary\s+colors?[:\s]+([a-z\s]+?)(?:\.|$|\n)',
            r'secondary\s+colors?[:\s]+([a-z\s]+?)(?:\.|$|\n)'
        ]
        
        colors_found = False
        for i, pattern in enumerate(color_patterns):
            match = re.search(pattern, page_text, re.IGNORECASE)
            if match:
                log_message(f"OPERATIONS_COLOR_MATCH: {team_name} - Pattern {i+1} - Match: {match.group(0)}")
                
                groups = match.groups()
                if len(groups) >= 2 and groups[0] and groups[1]:
                    primary = groups[0].strip().title()
                    secondary = groups[1].strip().title()
                    
                    primary = re.sub(r'\s+', ' ', primary)
                    secondary = re.sub(r'\s+', ' ', secondary)
                    
                    if primary and secondary and len(primary) > 1 and len(secondary) > 1:
                        team_data['primary_color'] = primary
                        team_data['secondary_color'] = secondary
                        log_message(f"OPERATIONS_COLORS_FOUND: {team_name} - {primary}/{secondary}")
                        colors_found = True
                        break
                elif groups[0]:
                    primary = groups[0].strip().title()
                    primary = re.sub(r'\s+', ' ', primary)
                    if primary and len(primary) > 1:
                        team_data['primary_color'] = primary
                        log_message(f"OPERATIONS_COLOR_FOUND: {team_name} - {primary}")
                
        population_patterns = [
            r'Population[:\s]+([0-9,]+)',
            r'population[:\s]+([0-9,]+)',
            r'([0-9,]+)\s+population',
            r'metro[a-z\s]*population[:\s]+([0-9,]+)',
            r'city[a-z\s]*population[:\s]+([0-9,]+)',
            r'metropolitan[a-z\s]*area[:\s]+([0-9,]+)',
            r'([0-9,]+)\s+residents',
            r'([0-9,]+)\s+people'
        ]
        
        for pattern in population_patterns:
            match = re.search(pattern, page_text, re.IGNORECASE)
            if match:
                for group in match.groups():
                    if group and re.match(r'^[0-9,]+$', group):
                        team_data['population'] = group
                        log_message(f"OPERATIONS_POPULATION_FOUND: {team_name} - {group}")
                        break
                if team_data['population'] != 'ERROR':
                    break
        
        found_fields = len([v for v in team_data.values() if v != 'ERROR'])
        log_message(f"OPERATIONS_SUCCESS: {team_name} - Found {found_fields}/4 data fields")
        
        for field, value in team_data.items():
            if value != 'ERROR':
                log_message(f"OPERATIONS_DATA: {team_name}.{field} = {value}")
        
        return team_data
        
    except Exception as e:
        log_message(f"OPERATIONS_ERROR: {team_name} - {str(e)}")
        return {
            'hometown': 'ERROR',
            'population': 'ERROR',
            'primary_color': 'ERROR',
            'secondary_color': 'ERROR',
        }

def ensure_complete_team_data(team):
    """Ensure all teams have complete data structure with ERROR for missing fields"""
    required_fields = {
        'name': '',
        'slug': '',
        'established': 'ERROR',
        'league': 'National Football League',
        'league_abbr': 'NFL',
        'hometown': 'ERROR',
        'population': 'ERROR',
        'primary_color': 'ERROR',
        'secondary_color': 'ERROR',
        'logo': 'ERROR',
        'header_background_url': 'ERROR',
        'head_coach': 'ERROR',
        'owners': 'ERROR',
        'stadium': 'ERROR',
        'conference': 'ERROR',
        'division': 'ERROR',
        'division_name': 'ERROR',
        'url': '',
        'operations_url': 'ERROR'
    }
    
    for field, default_value in required_fields.items():
        if field not in team or not team[field]:
            team[field] = default_value
    
    return team

def enhance_teams_with_operations_data(basic_teams, conference_data):
    """Enhance basic team data with operations.nfl.com data"""
    log_message("ENHANCE_START: Adding operations data to teams")
    
    enhanced_teams = []
    
    for team in basic_teams:
        team_name = team['name']
        log_message(f"ENHANCING: {team_name}")
        
        conf_info = conference_data.get(team_name, {})
        team.update(conf_info)
        
        team['league'] = 'National Football League'
        team['league_abbr'] = 'NFL'
        
        operations_url = generate_operations_url(team_name, conf_info)
        team['operations_url'] = operations_url
        
        operations_data = fetch_team_operations_data(team_name, operations_url)
        team.update(operations_data)
        
        log_message(f"PROFILE_FETCH: {team_name}")
        profile_data = fetch_team_profile(team['url'])
        
        field_mapping = {
            'Head Coach': 'head_coach',
            'Stadium': 'stadium',
            'Owners': 'owners',
            'Established': 'established'
        }
        
        for profile_key, profile_value in profile_data.items():
            if profile_key in field_mapping:
                team[field_mapping[profile_key]] = profile_value
        
        team = ensure_complete_team_data(team)
        enhanced_teams.append(team)
        
        time.sleep(1)
    
    log_message(f"ENHANCE_COMPLETE: Enhanced {len(enhanced_teams)} teams with comprehensive data")
    return enhanced_teams

def save_to_xml_multi_league(teams, league_info=None):
    """Save teams data to XML with multi-league structure including NFL logo"""
    if league_info is None:
        league_info = {
            'name': 'National Football League',
            'abbreviation': 'NFL',
            'country': 'USA',
            'sport': 'American Football'
        }
    
    # Fetch NFL league logo
    nfl_logo_url = fetch_nfl_league_logo()
    
    root = etree.Element("sports_teams")
    root.set("last_updated", datetime.now().isoformat())
    root.set("total_teams", str(len(teams)))
    root.set("total_leagues", "1")
    
    league_elem = etree.SubElement(root, "league")
    league_elem.set("name", league_info['name'])
    league_elem.set("abbreviation", league_info['abbreviation'])
    league_elem.set("country", league_info['country'])
    league_elem.set("sport", league_info['sport'])
    
    # Add league logo to the league element
    league_branding = etree.SubElement(league_elem, "branding")
    etree.SubElement(league_branding, "logo_url").text = nfl_logo_url
    
    conferences_elem = etree.SubElement(league_elem, "conferences")
    
    conferences = {}
    for team in teams:
        conf_name = team.get('conference', 'Unknown')
        div_name = team.get('division_name', 'Unknown')
        
        if conf_name not in conferences:
            conferences[conf_name] = {}
        if div_name not in conferences[conf_name]:
            conferences[conf_name][div_name] = []
        
        conferences[conf_name][div_name].append(team)
    
    for conf_name, divisions in conferences.items():
        conf_elem = etree.SubElement(conferences_elem, "conference")
        conf_elem.set("name", f"{conf_name} Conference" if conf_name in ['AFC', 'NFC'] else conf_name)
        conf_elem.set("abbreviation", conf_name)
        
        divisions_elem = etree.SubElement(conf_elem, "divisions")
        
        for div_name, div_teams in divisions.items():
            div_elem = etree.SubElement(divisions_elem, "division")
            div_elem.set("name", div_name)
            
            for team in div_teams:
                t = etree.SubElement(div_elem, "team")
                t.set("id", f"nfl_{team.get('slug', 'unknown')}")
                
                basic = etree.SubElement(t, "basic_info")
                etree.SubElement(basic, "name").text = team.get("name", "ERROR")
                etree.SubElement(basic, "slug").text = team.get("slug", "ERROR")
                etree.SubElement(basic, "established").text = team.get("established", "ERROR")
                etree.SubElement(basic, "league").text = team.get("league", "ERROR")
                etree.SubElement(basic, "league_abbr").text = team.get("league_abbr", "ERROR")
                
                location = etree.SubElement(t, "location")
                etree.SubElement(location, "hometown").text = team.get("hometown", "ERROR")
                etree.SubElement(location, "population").text = team.get("population", "ERROR")
                
                visual = etree.SubElement(t, "visual_identity")
                etree.SubElement(visual, "primary_color").text = team.get("primary_color", "ERROR")
                etree.SubElement(visual, "secondary_color").text = team.get("secondary_color", "ERROR")
                etree.SubElement(visual, "logo_url").text = team.get("logo", "ERROR")
                etree.SubElement(visual, "header_background_url").text = team.get("header_background_url", "ERROR")
                
                org = etree.SubElement(t, "organization")
                etree.SubElement(org, "head_coach").text = team.get("head_coach", "ERROR")
                etree.SubElement(org, "owners").text = team.get("owners", "ERROR")
                
                venue = etree.SubElement(t, "venue")
                etree.SubElement(venue, "stadium").text = team.get("stadium", "ERROR")
                urls = etree.SubElement(t, "urls")
                etree.SubElement(urls, "official_url").text = team.get("url", "ERROR")
                etree.SubElement(urls, "operations_url").text = team.get("operations_url", "ERROR")

    tree = etree.ElementTree(root)
    tree.write(OUTPUT_FILE, pretty_print=True, encoding="utf-8", xml_declaration=True)
    
    error_count = count_errors_in_xml()
    log_message(f"XML_WRITTEN: {OUTPUT_FILE} ({len(teams)} teams in multi-league structure)")
    log_message(f"ERROR_SUMMARY: {error_count} ERROR values found in generated XML")
    log_message(f"NFL_LOGO_INCLUDED: League logo URL added to XML structure")

def cleanup_unnecessary_backup(backup_created, changes_detected):
    """Remove backup file if no changes were made."""
    if backup_created and not changes_detected and os.path.exists(BACKUP_FILE):
        try:
            os.remove(BACKUP_FILE)
            log_message(f"BACKUP_CLEANUP: Removed unnecessary backup file")
        except Exception:
            log_message("BACKUP_CLEANUP_FAILED: Could not remove backup file")

def main():
    run_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_FILE, "w", encoding="utf-8") as f:
        f.write(f"NFL Teams Dynamic Scraper Log - Started at {run_time}\n")
        f.write("=" * 70 + "\n\n")
    
    log_message(f"RUN_START: NFL Teams dynamic scraper started at {run_time}")
    
    try:
        backup_created = create_backup()
        existing_teams = load_xml_teams(OUTPUT_FILE)
        basic_teams = fetch_teams_from_nfl()
        conference_data = fetch_conference_division()
        teams = enhance_teams_with_operations_data(basic_teams, conference_data)
        save_to_xml_multi_league(teams)
        
        changes_detected = False
        if backup_created:
            backup_teams = load_xml_teams(BACKUP_FILE)
            changes_detected = detect_actual_changes(teams, backup_teams)
        else:
            changes_detected = True
            log_message("COMPARISON_SKIP: No backup file to compare against (new installation)")
        
        cleanup_unnecessary_backup(backup_created, changes_detected)
        
        end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_message(f"RUN_SUCCESS: Completed exhaustive scraping with {len(teams)} teams at {end_time}")
        
    except Exception as e:
        end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_message(f"RUN_FAILED: Unexpected error - {str(e)} at {end_time}")
        raise

if __name__ == "__main__":
    main()