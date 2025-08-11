#!/usr/bin/env python3
"""
Combined 2025 Football Schedules - College and NFL
Writes all data to a single games.xml file with team-based structure for both leagues
"""

import requests
from datetime import datetime
import os
import time
import xml.etree.ElementTree as ET
import xml.dom.minidom as minidom

# Constants
API_KEY = "uec6arYek/ahsRs391Jina31sNXWeXjf8U3t/y59S7lKe11gw3aVUL1BQJPr31xf"
COLLEGE_BASE_URL = "https://api.collegefootballdata.com/games"
HEADERS = {"Authorization": f"Bearer {API_KEY}"}
XML_FILE = "games.xml"

# Team mappings from teams.xml and observed opponents
college_team_mappings = {
    "LSU": {"name": "LSU Tigers", "slug": "lsu-tigers"},
    "Tulane": {"name": "Tulane Green Wave", "slug": "tulane-green-wave"},
    "Florida": {"name": "Florida Gators", "slug": "florida-gators"},
    "Ole Miss": {"name": "Ole Miss Rebels", "slug": "ole-miss-rebels"},
    "Montana": {"name": "Montana Grizzlies", "slug": "montana-grizzlies"},
    "Texas": {"name": "Texas Longhorns", "slug": "texas-longhorns"},
    "Clemson": {"name": "Clemson Tigers", "slug": "clemson-tigers"},
    "Louisiana Tech": {"name": "Louisiana Tech Bulldogs", "slug": "louisiana-tech-bulldogs"},
    "SE Louisiana": {"name": "Southeastern Louisiana Lions", "slug": "se-louisiana-lions"},
    "South Carolina": {"name": "South Carolina Gamecocks", "slug": "south-carolina-gamecocks"},
    "Vanderbilt": {"name": "Vanderbilt Commodores", "slug": "vanderbilt-commodores"},
    "Texas A&M": {"name": "Texas A&M Aggies", "slug": "texas-a-m-aggies"},
    "Alabama": {"name": "Alabama Crimson Tide", "slug": "alabama-crimson-tide"},
    "Arkansas": {"name": "Arkansas Razorbacks", "slug": "arkansas-razorbacks"},
    "Western Kentucky": {"name": "Western Kentucky Hilltoppers", "slug": "western-kentucky-hilltoppers"},
    "Oklahoma": {"name": "Oklahoma Sooners", "slug": "oklahoma-sooners"},
    "Northwestern": {"name": "Northwestern Wildcats", "slug": "northwestern-wildcats"},
    "South Alabama": {"name": "South Alabama Jaguars", "slug": "south-alabama-jaguars"},
    "Duke": {"name": "Duke Blue Devils", "slug": "duke-blue-devils"},
    "Tulsa": {"name": "Tulsa Golden Hurricane", "slug": "tulsa-golden-hurricane"},
    "East Carolina": {"name": "East Carolina Pirates", "slug": "east-carolina-pirates"},
    "Army": {"name": "Army Black Knights", "slug": "army-black-knights"},
    "UTSA": {"name": "UTSA Roadrunners", "slug": "utsa-roadrunners"}
}

nfl_team_mappings = {
    "Buffalo Bills": "buffalo-bills", "Miami Dolphins": "miami-dolphins", "New England Patriots": "new-england-patriots",
    "New York Jets": "new-york-jets", "Baltimore Ravens": "baltimore-ravens", "Cincinnati Bengals": "cincinnati-bengals",
    "Cleveland Browns": "cleveland-browns", "Pittsburgh Steelers": "pittsburgh-steelers", "Houston Texans": "houston-texans",
    "Indianapolis Colts": "indianapolis-colts", "Jacksonville Jaguars": "jacksonville-jaguars", "Tennessee Titans": "tennessee-titans",
    "Denver Broncos": "denver-broncos", "Kansas City Chiefs": "kansas-city-chiefs", "Las Vegas Raiders": "las-vegas-raiders",
    "Los Angeles Chargers": "los-angeles-chargers", "Dallas Cowboys": "dallas-cowboys", "New York Giants": "new-york-giants",
    "Philadelphia Eagles": "philadelphia-eagles", "Washington Commanders": "washington-commanders", "Chicago Bears": "chicago-bears",
    "Detroit Lions": "detroit-lions", "Green Bay Packers": "green-bay-packers", "Minnesota Vikings": "minnesota-vikings",
    "Atlanta Falcons": "atlanta-falcons", "Carolina Panthers": "carolina-panthers", "New Orleans Saints": "new-orleans-saints",
    "Tampa Bay Buccaneers": "tampa-bay-buccaneers", "Arizona Cardinals": "arizona-cardinals", "Los Angeles Rams": "los-angeles-rams",
    "San Francisco 49ers": "san-francisco-49ers", "Seattle Seahawks": "seattle-seahawks"
}

def escape_xml(value):
    """Escape XML attribute values"""
    return str(value).replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;').replace('"', '&quot;').replace("'", '&apos;')

def normalize_team_name(team_name):
    """Normalize team names to match college_team_mappings keys"""
    if not team_name:
        return "Unknown"
    variations = {
        "Louisiana State": "LSU",
        "LSU Tigers": "LSU",
        "Tulane Green Wave": "Tulane",
        "University of Florida": "Florida",
        "Florida Gators": "Florida",
        "Mississippi": "Ole Miss",
        "Ole Miss Rebels": "Ole Miss",
        "Montana Grizzlies": "Montana",
        "Texas Longhorns": "Texas",
        "Clemson Tigers": "Clemson",
        "Alabama Crimson Tide": "Alabama",
        "South Carolina Gamecocks": "South Carolina",
        "Texas A&amp;M": "Texas A&M",
        "Southeastern Louisiana": "SE Louisiana",
        "Western Kentucky Hilltoppers": "Western Kentucky",
        "Oklahoma Sooners": "Oklahoma",
        "Northwestern Wildcats": "Northwestern",
        "South Alabama Jaguars": "South Alabama",
        "Duke Blue Devils": "Duke",
        "Tulsa Golden Hurricane": "Tulsa",
        "East Carolina Pirates": "East Carolina",
        "Army Black Knights": "Army",
        "UTSA Roadrunners": "UTSA"
    }
    normalized = variations.get(team_name.strip(), team_name.strip())
    print(f"Normalized team name: '{team_name}' → '{normalized}'")
    return normalized

def generate_fallback_slug(team_name):
    """Generate a fallback slug for teams not in mappings"""
    if not team_name or team_name == "Unknown":
        return "unknown"
    slug = team_name.lower().replace(' ', '-').replace('&', 'and').replace('.', '')
    print(f"Generated fallback slug: '{team_name}' → '{slug}'")
    return slug

def get_team_schedule(team_name, team_display_name):
    """Get college team schedule from API"""
    url = f"{COLLEGE_BASE_URL}?year=2025&seasonType=regular&team={team_name}"
    
    try:
        print(f"Fetching {team_display_name} schedule...")
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        
        games = response.json()
        print(f"Retrieved {len(games)} games for {team_display_name}")
        for game in games:
            game['venue'] = game.get('venue', game.get('stadium', 'N/A'))
            game['start_date'] = game.get('start_date', game.get('startDate', 'N/A'))
            game['week'] = str(game.get('week', 'N/A'))
            game['home_team'] = normalize_team_name(game.get('homeTeam', 'Unknown'))
            game['away_team'] = normalize_team_name(game.get('awayTeam', 'Unknown'))
            print(f"Game: {game['home_team']} vs {game['away_team']}, Week: {game['week']}, Venue: {game['venue']}")
        return games
        
    except Exception as e:
        print(f"❌ ERROR for {team_display_name}: {e}")
        return None

def fetch_nfl_schedule(year, week, seasontype=2):
    """Fetch NFL schedule from ESPN API"""
    url = f"https://cdn.espn.com/core/nfl/schedule?xhr=1&year={year}&seasontype={seasontype}&week={week}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
    }
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch data for week {week}: {response.status_code}")
    return response.json()

def parse_game_data(json_data, week):
    """Parse NFL game data"""
    games = []
    try:
        for date_key, schedule in json_data['content']['schedule'].items():
            for game in schedule['games']:
                game_info = {
                    'week': str(week),
                    'away_team': normalize_team_name(game['competitions'][0]['competitors'][1]['team']['displayName']),
                    'home_team': normalize_team_name(game['competitions'][0]['competitors'][0]['team']['displayName']),
                    'start_date': game['date'],
                    'venue': game['competitions'][0]['venue']['fullName'],
                    'odds': game['competitions'][0].get('odds', [{}])[0].get('details', 'N/A')
                }
                games.append(game_info)
                print(f"NFL Game: {game_info['home_team']} vs {game_info['away_team']}, Week: {week}, Venue: {game_info['venue']}")
    except KeyError:
        print(f"No game data found for week {week}.")
    return games

def save_to_xml(college_teams_data, nfl_teams_data):
    """Save all leagues and teams to XML file using ElementTree"""
    root = ET.Element("games")
    root.set("year", "2025")
    timestamp = datetime.now().isoformat()
    root.set("generated", timestamp)
    root.set("source", "CollegeFootballData.com API and ESPN API")
    root.set("last_updated", timestamp)
    root.set("total_teams", str(len(college_teams_data) + len(nfl_teams_data)))

    # College league
    college_league = ET.SubElement(root, "league")
    college_league.set("name", "College")
    for team_code, team_info in college_teams_data.items():
        team_elem = ET.SubElement(college_league, "team")
        team_elem.set("code", team_code)
        team_elem.set("name", team_info['name'])
        team_elem.set("slug", college_team_mappings.get(team_code, {}).get('slug', generate_fallback_slug(team_code)))
        team_elem.set("total_games", str(len(team_info['games'])))
        team_elem.set("updated", team_info['updated'])
        
        for i, game_data in enumerate(team_info['games'], 1):
            game_elem = ET.SubElement(team_elem, "game")
            game_elem.set("seq", str(i))
            
            home = game_data.get('home_team', 'Unknown')
            away = game_data.get('away_team', 'Unknown')
            print(f"Processing college game {i} for {team_info['name']}: {home} vs {away}")
            
            home_full_name = college_team_mappings.get(home, {}).get('name', home)
            home_slug = college_team_mappings.get(home, {}).get('slug', generate_fallback_slug(home))
            game_elem.set("home_team", escape_xml(home))
            game_elem.set("home_full_name", escape_xml(home_full_name))
            game_elem.set("home_slug", home_slug)
            print(f"  Home: {home} → full_name: {home_full_name}, slug: {home_slug}")
            
            away_full_name = college_team_mappings.get(away, {}).get('name', away)
            away_slug = college_team_mappings.get(away, {}).get('slug', generate_fallback_slug(away))
            game_elem.set("away_team", escape_xml(away))
            game_elem.set("away_full_name", escape_xml(away_full_name))
            game_elem.set("away_slug", away_slug)
            print(f"  Away: {away} → full_name: {away_full_name}, slug: {away_slug}")
            
            for key, value in game_data.items():
                if key not in ['home_team', 'away_team'] and value is not None and str(value) != "":
                    clean_key = key.replace(' ', '_').replace('-', '_').lower()
                    game_elem.set(clean_key, escape_xml(str(value)))

    # NFL league
    nfl_league = ET.SubElement(root, "league")
    nfl_league.set("name", "NFL")
    for team_name, team_info in nfl_teams_data.items():
        team_elem = ET.SubElement(nfl_league, "team")
        team_elem.set("name", team_name)
        team_elem.set("slug", team_info['slug'])
        team_elem.set("total_games", str(len(team_info['games'])))
        team_elem.set("updated", team_info['updated'])
        
        for i, game_data in enumerate(team_info['games'], 1):
            game_elem = ET.SubElement(team_elem, "game")
            game_elem.set("seq", str(i))
            
            home = game_data.get('home_team', 'Unknown')
            away = game_data.get('away_team', 'Unknown')
            print(f"Processing NFL game {i} for {team_name}: {home} vs {away}")
            
            home_slug = nfl_team_mappings.get(home, generate_fallback_slug(home))
            away_slug = nfl_team_mappings.get(away, generate_fallback_slug(away))
            game_elem.set("home_team", escape_xml(home))
            game_elem.set("home_slug", home_slug)
            game_elem.set("away_team", escape_xml(away))
            game_elem.set("away_slug", away_slug)
            print(f"  Home: {home} → slug: {home_slug}")
            print(f"  Away: {away} → slug: {away_slug}")
            
            for key, value in game_data.items():
                if key not in ['home_team', 'away_team'] and value is not None and str(value) != "":
                    clean_key = key.replace(' ', '_').replace('-', '_').lower()
                    game_elem.set(clean_key, escape_xml(str(value)))

    # Prettify and save
    xml_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), XML_FILE)
    rough_string = ET.tostring(root, 'utf-8')
    parsed = minidom.parseString(rough_string)
    pretty_xml = parsed.toprettyxml(indent="  ", encoding="utf-8").decode("utf-8")
    
    with open(xml_path, "w", encoding="utf-8") as f:
        f.write(pretty_xml)
    
    print(f"💾 Saved to {xml_path}")

def main():
    """Process college and NFL schedules and save to single XML file"""
    
    teams = [
        ("LSU", "LSU Tigers"),
        ("Tulane", "Tulane Green Wave"),
        ("Florida", "Florida Gators"),
        ("Ole Miss", "Ole Miss Rebels"),
        ("Montana", "Montana Grizzlies"),
        ("Texas", "Texas Longhorns")
    ]
    
    print("COLLEGE FOOTBALL 2025 SCHEDULE GENERATOR")
    print("=" * 50)
    
    college_teams_data = {}
    successful_college = 0
    for i, (team_name, display_name) in enumerate(teams, 1):
        print(f"\n[{i}/{len(teams)}] Processing {display_name}...")
        
        games = get_team_schedule(team_name, display_name)
        
        if games:
            college_teams_data[team_name] = {
                'name': display_name,
                'games': games,
                'updated': datetime.now().isoformat()
            }
            successful_college += 1
            print(f"✅ Added {display_name} with {len(games)} games")
        
        if i < len(teams):
            time.sleep(2)
    
    print(f"\n{'='*50}")
    print(f"COLLEGE COMPLETE: {successful_college}/{len(teams)} teams processed")
    
    year = 2025
    weeks = range(1, 19)
    seasontype = 2
    all_games = []
    
    print("\nNFL FOOTBALL 2025 SCHEDULE GENERATOR")
    print("=" * 50)
    
    for week in weeks:
        try:
            print(f"Fetching schedule for week {week}...")
            json_data = fetch_nfl_schedule(year, week, seasontype)
            games = parse_game_data(json_data, week)
            if games:
                all_games.extend(games)
            else:
                print(f"No games found for week {week}.")
            time.sleep(1)
        except Exception as e:
            print(f"Error for week {week}: {e}")
            time.sleep(2)
    
    unique_teams = set()
    for g in all_games:
        unique_teams.add(g['home_team'])
        unique_teams.add(g['away_team'])
    
    nfl_teams_data = {}
    for team in sorted(unique_teams):
        team_games = [g for g in all_games if g['home_team'] == team or g['away_team'] == team]
        team_games.sort(key=lambda g: (g['week'], g['start_date']))
        nfl_teams_data[team] = {
            'slug': nfl_team_mappings.get(team, generate_fallback_slug(team)),
            'games': team_games,
            'updated': datetime.now().isoformat()
        }
    
    print(f"\n{'='*50}")
    print(f"NFL COMPLETE: {len(nfl_teams_data)} teams processed with {len(all_games)} total games")
    
    if college_teams_data or nfl_teams_data:
        save_to_xml(college_teams_data, nfl_teams_data)

if __name__ == "__main__":
    main()