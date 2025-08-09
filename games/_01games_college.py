#!/usr/bin/env python3
"""
Multiple Teams 2025 Football Schedule - College Football Data API
Writes all teams to a single collegegames.xml file with one line per game
"""

import requests
from datetime import datetime
import os
import time

# Constants
API_KEY = "uec6arYek/ahsRs391Jina31sNXWeXjf8U3t/y59S7lKe11gw3aVUL1BQJPr31xf"
BASE_URL = "https://api.collegefootballdata.com/games"
HEADERS = {"Authorization": f"Bearer {API_KEY}"}
XML_FILE = "collegegames.xml"

def escape_xml(value):
    """Escape XML attribute values"""
    return str(value).replace('"', '&quot;').replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')

def get_team_schedule(team_name, team_display_name):
    """Get team schedule from API"""
    url = f"{BASE_URL}?year=2025&seasonType=regular&team={team_name}"
    
    try:
        print(f"Fetching {team_display_name} schedule...")
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        
        games = response.json()
        print(f"Retrieved {len(games)} games")
        return games
        
    except Exception as e:
        print(f"❌ ERROR for {team_display_name}: {e}")
        return None

def load_existing_teams():
    """Load existing teams from XML file"""
    xml_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), XML_FILE)
    teams_data = {}
    
    if os.path.exists(xml_path):
        try:
            with open(xml_path, 'r', encoding='utf-8') as f:
                content = f.read()
                # Simple parsing to extract existing teams
                lines = content.split('\n')
                current_team = None
                for line in lines:
                    if '<team ' in line:
                        # Extract team code
                        if 'code="' in line:
                            start = line.find('code="') + 6
                            end = line.find('"', start)
                            current_team = line[start:end]
                            teams_data[current_team] = []
                    elif '<game ' in line and current_team:
                        teams_data[current_team].append(line.strip())
            print(f"Loaded existing {XML_FILE} with {len(teams_data)} teams")
        except:
            print(f"Error reading existing {XML_FILE}, will create new file")
    
    return teams_data

def save_all_teams_xml(all_teams_data):
    """Save all teams to XML file"""
    xml_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), XML_FILE)
    
    # Build XML manually
    lines = ['<?xml version="1.0" ?>']
    
    # Root element
    timestamp = datetime.now().isoformat()
    lines.append(f'<games generated="{timestamp}" source="CollegeFootballData.com API" year="2025" last_updated="{timestamp}" total_teams="{len(all_teams_data)}">')
    
    # Add each team
    for team_code, team_info in all_teams_data.items():
        team_name = team_info['name']
        games = team_info['games']
        updated = team_info['updated']
        
        lines.append(f'  <team code="{team_code}" name="{team_name}" total_games="{len(games)}" updated="{updated}">')
        
        # Add each game as a single line
        for i, game_data in enumerate(games, 1):
            attrs = [f'id="{i}"']
            
            # Add all game attributes
            for key, value in game_data.items():
                if value is not None and value != "":
                    clean_key = str(key).replace(' ', '_').replace('-', '_').lower()
                    escaped_value = escape_xml(value)
                    attrs.append(f'{clean_key}="{escaped_value}"')
            
            lines.append(f'    <game {" ".join(attrs)} />')
        
        lines.append('  </team>')
    
    lines.append('</games>')
    
    # Save to file
    with open(xml_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines))
    
    print(f"💾 Saved to {xml_path}")

def main():
    """Process all teams and save to single XML file"""
    
    teams = [
        ("LSU", "LSU Tigers"),
        ("Tulane", "Tulane Green Wave"), 
        ("Florida", "Florida Gators"),
        ("Ole Miss", "Ole Miss Rebels"),
        ("Montana", "Montana Grizzlies")
    ]
    
    print("COLLEGE FOOTBALL 2025 SCHEDULE GENERATOR")
    print("=" * 50)
    print(f"Output: {XML_FILE}")
    
    # Load existing teams data
    all_teams_data = {}
    
    successful = 0
    for i, (team_name, display_name) in enumerate(teams, 1):
        print(f"\n[{i}/{len(teams)}] Processing {display_name}...")
        
        # Get schedule from API
        games = get_team_schedule(team_name, display_name)
        
        if games:
            # Store team data
            all_teams_data[team_name] = {
                'name': display_name,
                'games': games,
                'updated': datetime.now().isoformat()
            }
            successful += 1
            print(f"✅ Added {display_name} with {len(games)} games")
        
        if i < len(teams):
            time.sleep(2)
    
    # Save all teams to XML
    if all_teams_data:
        save_all_teams_xml(all_teams_data)
    
    print(f"\n{'='*50}")
    print(f"COMPLETE: {successful}/{len(teams)} teams saved to {XML_FILE}")

if __name__ == "__main__":
    main()