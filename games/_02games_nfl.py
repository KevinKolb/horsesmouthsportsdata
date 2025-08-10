import requests
import xml.etree.ElementTree as ET
import xml.dom.minidom as minidom
import os
import time

def fetch_nfl_schedule(year, week, seasontype=2):
    url = f"https://cdn.espn.com/core/nfl/schedule?xhr=1&year={year}&seasontype={seasontype}&week={week}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
    }
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch data for week {week}: {response.status_code}")
    return response.json()

def parse_game_data(json_data, week):
    games = []
    try:
        for date_key, schedule in json_data['content']['schedule'].items():
            for game in schedule['games']:
                game_info = {
                    'week': week,
                    'away_team': game['competitions'][0]['competitors'][1]['team']['displayName'],
                    'home_team': game['competitions'][0]['competitors'][0]['team']['displayName'],
                    'game_time': game['date'],
                    'location': game['competitions'][0]['venue']['fullName'],
                    'odds': game['competitions'][0].get('odds', [{}])[0].get('details', 'N/A')
                }
                games.append(game_info)
    except KeyError:
        print(f"No game data found for week {week}.")
    return games

def save_to_xml(all_games, year, folder="games", filename="games.xml"):
    # Ensure the games folder exists
    os.makedirs(folder, exist_ok=True)
    filepath = os.path.join(folder, filename)
    
    root = ET.Element("NFLSchedule")
    root.set("year", str(year))
    
    for game in all_games:
        game_elem = ET.SubElement(root, "Game")
        game_elem.set("week", str(game['week']))
        game_elem.set("away_team", game['away_team'])
        game_elem.set("home_team", game['home_team'])
        game_elem.set("game_time", game['game_time'])
        game_elem.set("location", game['location'])
        game_elem.set("odds", game['odds'])
    
    # Convert ElementTree to string and prettify
    rough_string = ET.tostring(root, 'utf-8')
    parsed = minidom.parseString(rough_string)
    pretty_xml = parsed.toprettyxml(indent="    ", encoding="utf-8").decode("utf-8")
    
    # Write to file
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(pretty_xml)
    print(f"All schedules saved to {filepath}")

def main():
    year = 2025
    weeks = range(1, 19)  # NFL regular season typically has 18 weeks
    seasontype = 2  # Regular season
    all_games = []
    
    for week in weeks:
        try:
            print(f"Fetching schedule for week {week}...")
            json_data = fetch_nfl_schedule(year, week, seasontype)
            games = parse_game_data(json_data, week)
            if games:
                all_games.extend(games)
            else:
                print(f"No games found for week {week}.")
            time.sleep(1)  # Avoid overwhelming the API
        except Exception as e:
            print(f"Error for week {week}: {e}")
            time.sleep(2)  # Wait longer if there's an error
    
    if all_games:
        save_to_xml(all_games, year, folder="games", filename="games.xml")
    else:
        print("No games found for any week, no XML file created.")

if __name__ == "__main__":
    main()