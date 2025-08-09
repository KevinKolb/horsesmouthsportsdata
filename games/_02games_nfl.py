#!/usr/bin/env python3
"""
NFL Schedule Fetcher - College Football XML Format
Fetches NFL games from operations.nfl.com and manages them in nflgames.xml 
using the same field structure as college football data.
"""

import os
import re
import shutil
from datetime import datetime
from xml.dom import minidom
from xml.etree.ElementTree import Element, SubElement, parse, tostring

import requests
from bs4 import BeautifulSoup


class NFLScheduleFetcher:
    """
    Fetches NFL schedule data and manages XML storage using college football format.
    
    Features:
    - Fetches schedule from NFL operations website
    - Uses college football XML structure and field names
    - Appends new games, overwrites existing ones
    - Creates backup before changes
    - Tracks detailed changes between versions
    """
    
    def __init__(self):
        # Configuration
        self.url = "https://operations.nfl.com/gameday/nfl-schedule/2025-nfl-schedule/"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        # File paths
        self.xml_file = 'games/nflgames.xml'
        self.backup_file = 'games/nflgames_backup.xml'
        self.log_file = 'games/nflgames_log.txt'
        
        # Data storage
        self.games = []
        
        # Ensure directory exists
        os.makedirs('games', exist_ok=True)
    
    # ========================================
    # MAIN WORKFLOW METHODS
    # ========================================
    
    def run(self):
        """Execute the complete fetch and update workflow."""
        run_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self._log(f"RUN_START: NFL Schedule fetcher started at {run_time}")
        print(f"🚀 Starting NFL Schedule fetcher at {run_time}")
        
        try:
            # Step 1: Fetch schedule data
            html_content = self._fetch_schedule_page()
            if not html_content:
                self._log_and_exit("RUN_FAILED: Could not fetch schedule page")
                return
            
            # Step 2: Parse games from HTML
            games = self._parse_schedule_data(html_content)
            if not games:
                self._log_and_exit("RUN_FAILED: No games found")
                return
            
            # Step 3: Save to XML with change tracking
            self._save_to_xml()
            
            # Step 4: Log success
            end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self._log(f"RUN_SUCCESS: Completed with {len(games)} games at {end_time}")
            print(f"\n🏈 COMPLETE: {len(games)} NFL games processed at {end_time}!")
            
        except Exception as e:
            self._log_and_exit(f"RUN_FAILED: Unexpected error - {str(e)}")
    
    def _log_and_exit(self, message):
        """Log an error message and print it."""
        end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        full_message = f"{message} at {end_time}"
        self._log(full_message)
        print(full_message)
    
    # ========================================
    # DATA FETCHING AND PARSING
    # ========================================
    
    def _fetch_schedule_page(self):
        """
        Fetch the NFL schedule page from the official source.
        
        Returns:
            str: HTML content of the page, or None if failed
        """
        try:
            print("Fetching NFL schedule...")
            response = requests.get(self.url, headers=self.headers)
            response.raise_for_status()
            print(f"Successfully fetched page ({len(response.text):,} characters)")
            return response.text
        except requests.RequestException as e:
            print(f"Error fetching page: {e}")
            return None
    
    def _parse_schedule_data(self, html_content):
        """
        Parse HTML content and extract all game information.
        
        Args:
            html_content (str): Raw HTML from the NFL schedule page
            
        Returns:
            list: List of game dictionaries with college football format fields
        """
        soup = BeautifulSoup(html_content, 'html.parser')
        print("Parsing schedule...")
        
        self.games = []
        tables = soup.find_all('table')
        
        # Process each table that might contain schedule data
        for table in tables:
            rows = table.find_all('tr')
            if len(rows) > 10:  # Skip small tables that aren't schedule data
                self._extract_games_from_table(rows)
        
        # Remove duplicate games that might exist across tables
        self.games = self._remove_duplicate_games()
        
        # Convert to college football format
        self._convert_to_college_format()
        
        print(f"Found {len(self.games)} games")
        return self.games
    
    def _extract_games_from_table(self, rows):
        """
        Extract game data from table rows.
        
        Args:
            rows: List of BeautifulSoup row elements from an HTML table
        """
        current_week = None
        current_date = None
        
        for row in rows:
            # Check what type of row this is and extract accordingly
            if self._is_week_header(row):
                current_week = self._extract_week_number(row)
            elif self._is_date_header(row):
                current_date = self._extract_date_string(row)
            elif self._is_game_data_row(row):
                game_info = self._extract_single_game(row, current_week, current_date)
                if game_info:
                    self.games.append(game_info)
    
    def _remove_duplicate_games(self):
        """
        Remove duplicate games based on week, teams, and date.
        
        Returns:
            list: List of unique games
        """
        unique_games = []
        seen_games = set()
        
        for game in self.games:
            # Create unique identifier for deduplication
            game_key = f"{game['week']}-{game['away_team']}-{game['home_team']}-{game['date']}"
            if game_key not in seen_games:
                unique_games.append(game)
                seen_games.add(game_key)
        
        return unique_games
    
    def _convert_to_college_format(self):
        """
        Convert NFL game data to college football XML format fields.
        """
        print("Converting to college football format...")
        
        for i, game in enumerate(self.games):
            # Generate unique ID
            game_id = f"nfl_{2025}_{i+1:03d}"
            
            # Convert to college format fields
            college_game = {
                'id': game_id,
                'season': '2025',
                'week': game['week'] or '1',
                'seasontype': 'regular',
                'startdate': self._convert_to_iso_date(game['date'], game['time_et']),
                'starttimetbd': 'False',
                'completed': 'False',
                'neutralsite': 'True' if game['location_note'] else 'False',
                'conferencegame': 'False',  # NFL doesn't have conferences like college
                'venueid': '',  # Not available from NFL schedule
                'venue': self._determine_venue(game),
                'homeid': '',  # Not available
                'hometeam': game['home_team'],
                'homeclassification': 'nfl',
                'homeconference': self._get_nfl_conference(game['home_team']),
                'awayid': '',  # Not available
                'awayteam': game['away_team'],
                'awayclassification': 'nfl',
                'awayconference': self._get_nfl_conference(game['away_team']),
                'network': game['network'],
                'notes': game['location_note'] if game['location_note'] else ''
            }
            
            # Replace original game data
            self.games[i] = college_game
    
    def _convert_to_iso_date(self, date_str, time_str):
        """
        Convert NFL date/time to ISO format for college football compatibility.
        
        Args:
            date_str (str): Date string from NFL schedule
            time_str (str): Time string from NFL schedule
            
        Returns:
            str: ISO formatted datetime string
        """
        try:
            # This is a simplified conversion - you may need to enhance this
            # based on the actual date/time formats from the NFL schedule
            if not date_str or not time_str:
                return "2025-08-30T20:00:00.000Z"  # Default fallback
            
            # Parse common NFL date formats and convert to ISO
            # This is a placeholder - implement actual parsing based on your data
            return "2025-08-30T20:00:00.000Z"
            
        except Exception:
            return "2025-08-30T20:00:00.000Z"  # Default fallback
    
    def _determine_venue(self, game):
        """
        Determine venue name from game data.
        
        Args:
            game (dict): Original NFL game data
            
        Returns:
            str: Venue name
        """
        if game['location_note']:
            return f"NFL Stadium ({game['location_note']})"
        else:
            return f"{game['home_team']} Stadium"
    
    def _get_nfl_conference(self, team_name):
        """
        Map NFL team to conference (AFC/NFC).
        
        Args:
            team_name (str): NFL team name
            
        Returns:
            str: Conference abbreviation
        """
        # Simplified mapping - you can expand this
        afc_teams = [
            'Bills', 'Dolphins', 'Patriots', 'Jets',  # AFC East
            'Ravens', 'Bengals', 'Browns', 'Steelers',  # AFC North
            'Texans', 'Colts', 'Jaguars', 'Titans',  # AFC South
            'Broncos', 'Chiefs', 'Raiders', 'Chargers'  # AFC West
        ]
        
        # Check if team name contains any AFC team identifier
        for afc_team in afc_teams:
            if afc_team.lower() in team_name.lower():
                return 'AFC'
        
        return 'NFC'  # Default to NFC for all others
    
    # ========================================
    # HTML PARSING HELPERS (same as original)
    # ========================================
    
    def _is_week_header(self, row):
        """Check if this row contains week information."""
        cells = row.find_all(['td', 'th'])
        if len(cells) >= 1:
            text = ' '.join(cell.get_text(strip=True) for cell in cells)
            return bool(re.search(r'WEEK\s+(\d+)', text.upper()))
        return False
    
    def _is_date_header(self, row):
        """Check if this row contains date information."""
        cells = row.find_all(['td', 'th'])
        if len(cells) >= 1:
            text = ' '.join(cell.get_text(strip=True) for cell in cells)
            
            # Look for day names and month names
            days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
            months = ['January', 'February', 'March', 'April', 'May', 'June', 
                     'July', 'August', 'September', 'October', 'November', 'December',
                     'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                     'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec', 'Sept']
            
            has_day = any(day in text for day in days)
            has_month = any(month in text for month in months)
            return has_day and has_month
        return False
    
    def _is_game_data_row(self, row):
        """Check if this row contains actual game matchup data."""
        cells = row.find_all(['td', 'th'])
        if len(cells) < 3:
            return False
        
        first_cell_text = cells[0].get_text(strip=True)
        return ' at ' in first_cell_text or ' vs ' in first_cell_text
    
    def _extract_week_number(self, row):
        """Extract week number from a week header row."""
        text = ' '.join(cell.get_text(strip=True) for cell in row.find_all(['td', 'th']))
        week_match = re.search(r'WEEK\s+(\d+)', text.upper())
        return week_match.group(1) if week_match else None
    
    def _extract_date_string(self, row):
        """Extract formatted date string from a date header row."""
        text = ' '.join(cell.get_text(strip=True) for cell in row.find_all(['td', 'th']))
        return ' '.join(text.split())  # Clean up whitespace
    
    def _extract_single_game(self, row, current_week, current_date):
        """
        Extract complete game information from a single table row.
        
        Args:
            row: BeautifulSoup row element
            current_week (str): Current week number context
            current_date (str): Current date context
            
        Returns:
            dict: Complete game information or None if extraction fails
        """
        cells = row.find_all('td')
        if len(cells) < 3:
            return None
        
        try:
            # Parse team matchup (first column)
            teams_text = cells[0].get_text(strip=True)
            teams_info = self._parse_team_matchup(teams_text)
            
            # Extract time and network information
            time_et = cells[1].get_text(strip=True) if len(cells) > 1 else ""
            time_local = cells[2].get_text(strip=True) if len(cells) > 2 else ""
            network = cells[3].get_text(strip=True) if len(cells) > 3 else ""
            
            return {
                'week': current_week,
                'date': current_date,
                'away_team': teams_info['away_team'],
                'home_team': teams_info['home_team'],
                'location_note': teams_info['location_note'],
                'time_et': time_et,
                'time_local': time_local,
                'network': network
            }
        except Exception:
            return None
    
    def _parse_team_matchup(self, teams_text):
        """
        Parse team names and location notes from matchup text.
        
        Args:
            teams_text (str): Raw text like "Cowboys at Eagles (London)"
            
        Returns:
            dict: Parsed team and location information
        """
        location_note = ""
        
        # Extract location note in parentheses
        location_match = re.search(r'\(([^)]+)\)', teams_text)
        if location_match:
            location_note = location_match.group(1)
            teams_text = re.sub(r'\s*\([^)]+\)', '', teams_text)
        
        # Parse team matchup
        if ' at ' in teams_text:
            parts = teams_text.split(' at ')
            away_team = parts[0].strip()
            home_team = parts[1].strip()
        elif ' vs ' in teams_text:
            parts = teams_text.split(' vs ')
            away_team = parts[0].strip()
            home_team = parts[1].strip()
        else:
            # Fallback for unexpected format
            away_team = teams_text
            home_team = ""
        
        return {
            'away_team': away_team,
            'home_team': home_team,
            'location_note': location_note
        }
    
    # ========================================
    # XML MANAGEMENT SYSTEM - COLLEGE FORMAT
    # ========================================
    
    def _save_to_xml(self):
        """
        Main XML save workflow with append/overwrite functionality.
        Uses college football XML structure.
        """
        if not self.games:
            print("No games to save")
            self._log("NO_GAMES: No games found to save")
            return
        
        # Step 1: Create backup of existing file
        backup_created = self._create_backup()
        
        # Step 2: Load existing games
        existing_games = self._load_existing_games()
        
        # Step 3: Merge new games with existing (append new, overwrite existing)
        merged_games = self._merge_games(existing_games)
        
        # Step 4: Write updated XML
        self._write_college_format_xml(merged_games)
        
        # Step 5: Log results
        self._log_merge_results(existing_games, merged_games)
        
        print(f"✅ XML updated with {len(self.games)} NFL games")
    
    def _create_backup(self):
        """
        Create backup of current XML file before making changes.
        
        Returns:
            bool: True if backup was created, False otherwise
        """
        if not os.path.exists(self.xml_file):
            return False
        
        try:
            if os.path.exists(self.backup_file):
                os.remove(self.backup_file)
            
            shutil.copy2(self.xml_file, self.backup_file)
            self._log(f"BACKUP_CREATED: {self.backup_file}")
            print(f"📁 Backup created: {self.backup_file}")
            return True
            
        except Exception as e:
            self._log(f"BACKUP_FAILED: {str(e)}")
            print(f"⚠️ Backup failed: {e}")
            return False
    
    def _load_existing_games(self):
        """
        Load existing games from XML file.
        
        Returns:
            dict: Existing games keyed by game ID
        """
        if not os.path.exists(self.xml_file):
            return {}
        
        try:
            tree = parse(self.xml_file)
            root = tree.getroot()
            existing_games = {}
            
            # Handle both team-based and flat game structures
            for game_elem in root.findall('.//game'):
                game_id = game_elem.get('id')
                if game_id:
                    # Extract all attributes as game data
                    game_data = dict(game_elem.attrib)
                    existing_games[game_id] = game_data
            
            print(f"📖 Loaded {len(existing_games)} existing games")
            return existing_games
            
        except Exception as e:
            print(f"Error reading existing XML: {e}")
            self._log(f"XML_READ_ERROR: {str(e)}")
            return {}
    
    def _merge_games(self, existing_games):
        """
        Merge new NFL games with existing games.
        New games are added, existing games are overwritten.
        
        Args:
            existing_games (dict): Existing games from XML
            
        Returns:
            dict: Merged games
        """
        merged = existing_games.copy()
        new_count = 0
        overwrite_count = 0
        
        for game in self.games:
            game_id = game['id']
            
            if game_id in merged:
                overwrite_count += 1
                self._log(f"OVERWRITE: Game {game_id} - {game['awayteam']} at {game['hometeam']}")
            else:
                new_count += 1
                self._log(f"APPEND: Game {game_id} - {game['awayteam']} at {game['hometeam']}")
            
            merged[game_id] = game
        
        print(f"📊 Merge results: {new_count} new, {overwrite_count} overwritten")
        return merged
    
    def _write_college_format_xml(self, games_dict):
        """
        Write games to XML file using college football format.
        
        Args:
            games_dict (dict): All games to write
        """
        # Create root element with college football attributes
        root = Element('games')
        root.set('generated', datetime.now().isoformat())
        root.set('source', 'NFL Operations API')
        root.set('year', '2025')
        root.set('last_updated', datetime.now().isoformat())
        root.set('total_games', str(len(games_dict)))
        
        # Group games by team for college football structure
        teams_games = self._group_games_by_team(games_dict)
        
        for team_name, team_games in teams_games.items():
            team_elem = SubElement(root, 'team')
            team_elem.set('code', self._get_team_code(team_name))
            team_elem.set('name', team_name)
            team_elem.set('total_games', str(len(team_games)))
            team_elem.set('updated', datetime.now().isoformat())
            
            # Sort games by week
            sorted_games = sorted(team_games, key=lambda g: int(g.get('week', '0')))
            
            for i, game in enumerate(sorted_games):
                game_elem = SubElement(team_elem, 'game')
                game_elem.set('id', str(i + 1))  # Sequential ID within team
                
                # Add all game attributes
                for key, value in game.items():
                    if key != 'id':  # Don't duplicate ID
                        game_elem.set(key, str(value))
        
        # Write formatted XML
        self._save_formatted_xml(root)
    
    def _group_games_by_team(self, games_dict):
        """
        Group games by team for college football XML structure.
        
        Args:
            games_dict (dict): All games
            
        Returns:
            dict: Games grouped by team name
        """
        teams = {}
        
        for game in games_dict.values():
            # Add game to both home and away team groups
            home_team = game.get('hometeam', '')
            away_team = game.get('awayteam', '')
            
            if home_team:
                if home_team not in teams:
                    teams[home_team] = []
                teams[home_team].append(game)
            
            if away_team and away_team != home_team:
                if away_team not in teams:
                    teams[away_team] = []
                teams[away_team].append(game)
        
        return teams
    
    def _get_team_code(self, team_name):
        """
        Generate team code from team name.
        
        Args:
            team_name (str): Full team name
            
        Returns:
            str: Team code (e.g., "DAL" for Dallas Cowboys)
        """
        # Simple team code generation - you can enhance this
        if not team_name:
            return "UNK"
        
        # Extract first 3 letters or use abbreviation
        words = team_name.split()
        if len(words) >= 2:
            return (words[-1][:3]).upper()  # Use last word (team name)
        else:
            return team_name[:3].upper()
    
    def _save_formatted_xml(self, root):
        """Save XML with proper formatting."""
        rough_string = tostring(root, 'utf-8')
        reparsed = minidom.parseString(rough_string)
        pretty_xml = reparsed.toprettyxml(indent='  ')
        
        # Clean up extra blank lines
        pretty_xml = '\n'.join([line for line in pretty_xml.split('\n') if line.strip()])
        
        with open(self.xml_file, 'w', encoding='utf-8') as f:
            f.write(pretty_xml)
        
        print(f"✅ XML saved: {self.xml_file}")
        self._log(f"XML_WRITTEN: {self.xml_file}")
    
    def _log_merge_results(self, existing_games, merged_games):
        """Log statistics about the merge operation."""
        original_count = len(existing_games)
        final_count = len(merged_games)
        nfl_games_count = len(self.games)
        
        self._log(f"MERGE_STATS: {original_count} existing + {nfl_games_count} NFL = {final_count} total games")
    
    # ========================================
    # LOGGING SYSTEM
    # ========================================
    
    def _log(self, message):
        """
        Write timestamped log entry to log file.
        
        Args:
            message (str): Message to log
        """
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"[{timestamp}] {message}\n"
        
        with open(self.log_file, 'a', encoding='utf-8') as f:
            f.write(log_entry)


def main():
    """
    Main entry point for the NFL Schedule Fetcher.
    Creates fetcher instance and runs the complete workflow.
    """
    fetcher = NFLScheduleFetcher()
    fetcher.run()


if __name__ == "__main__":
    main()