#!/usr/bin/env python3
"""
NFL Schedule Fetcher - XML Management System
Fetches NFL games from operations.nfl.com and manages them in games.xml 
with intelligent backup and change tracking capabilities.
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
    Fetches NFL schedule data and manages XML storage with backup and logging.
    
    Features:
    - Fetches schedule from NFL operations website
    - Creates static backup before changes
    - Tracks detailed changes between versions
    - Logs all operations with timestamps
    - Uses year-prefixed game IDs for uniqueness
    """
    
    def __init__(self):
        # Configuration
        self.url = "https://operations.nfl.com/gameday/nfl-schedule/2025-nfl-schedule/"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        # File paths
        self.xml_file = 'games.xml'
        self.backup_file = 'games_backup.xml'
        self.log_file = 'games_log.txt'
        
        # Data storage
        self.games = []
    
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
            list: List of game dictionaries with all details
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
        
        # Add comprehensive game identification systems
        self._generate_game_identifiers()
        
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
    
    # ========================================
    # HTML PARSING HELPERS
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
    # GAME IDENTIFICATION SYSTEM
    # ========================================
    
    def _generate_game_identifiers(self):
        """
        Add comprehensive identification systems to all games.
        Creates multiple ID formats for different use cases.
        """
        print("Adding game IDs...")
        
        # Sort games chronologically for consistent numbering
        self.games.sort(key=lambda g: (
            int(g['week']) if g['week'] and g['week'].isdigit() else 999,
            g['date'] or '',
            g['time_et'] or ''
        ))
        
        week_game_counters = {}
        
        for i, game in enumerate(self.games):
            week = game['week'] or 'WX'
            
            # Track games per week for week-based numbering
            if week not in week_game_counters:
                week_game_counters[week] = 0
            week_game_counters[week] += 1
            
            # Generate all ID formats
            game['game_id'] = f"2025-{i + 1:03d}"  # Sequential with year: 2025-001, 2025-002
            game['week_game_id'] = f"2025-W{week}G{week_game_counters[week]}"  # Week-based: 2025-W1G1
            game['game_code'] = self._create_team_based_code(game, week)  # Team-based: 2025W1-dallas-cowboys@philadelphia-eagles
            game['nfl_game_key'] = self._create_nfl_style_key(week, i + 1)  # NFL-style: 2025010101
    
    def _create_team_based_code(self, game, week):
        """Create a team-based game code with URL-friendly team slugs."""
        away_slug = self._get_team_abbreviation(game['away_team'])
        home_slug = self._get_team_abbreviation(game['home_team'])
        return f"2025W{week or 'X'}-{away_slug}@{home_slug}"
    
    def _create_nfl_style_key(self, week, sequence):
        """Create an NFL-style numeric game key."""
        week_padded = week.zfill(2) if week.isdigit() else '00'
        return f"2025{week_padded}{sequence:02d}"
    
    def _get_team_abbreviation(self, team_name):
        """
        Convert full team name to URL-friendly slug format.
        
        Args:
            team_name (str): Full team name like "New York Jets"
            
        Returns:
            str: URL-friendly slug like "new-york-jets"
        """
        if not team_name:
            return "unknown-team"
        
        # Convert to lowercase and replace spaces and special characters with hyphens
        slug = team_name.lower()
        
        # Replace common special characters and spaces
        slug = re.sub(r'[^\w\s-]', '', slug)  # Remove punctuation except hyphens
        slug = re.sub(r'[-\s]+', '-', slug)   # Replace spaces and multiple hyphens with single hyphen
        slug = slug.strip('-')                # Remove leading/trailing hyphens
        
        # Handle empty result
        if not slug:
            slug = "unknown-team"
        
        return slug
    
    # ========================================
    # XML MANAGEMENT SYSTEM
    # ========================================
    
    def _save_to_xml(self):
        """
        Main XML save workflow with intelligent change detection.
        Creates backup, analyzes changes, and updates file only when necessary.
        """
        if not self.games:
            print("No games to save")
            self._log("NO_GAMES: No games found to save")
            return
        
        # Step 1: Create backup of existing file
        backup_created = self._create_backup()
        
        # Step 2: Load and compare with existing data
        existing_games = self._load_xml_games(self.xml_file)
        
        if not existing_games:
            # No existing file - create new one
            print("Creating new XML file...")
            self._write_xml_file()
            self._log(f"FILE_CREATED: New file with {len(self.games)} games")
            
        elif self._detect_changes(existing_games):
            # Changes detected - update file and log details
            print("Games have changed, updating XML...")
            self._update_xml_with_changes(existing_games, backup_created)
            
        else:
            # No changes - clean up unnecessary backup but still compare
            print("No changes detected, XML file is up to date")
            self._log(f"NO_CHANGES: File up to date with {len(self.games)} games")
            
            # Still compare with backup for logging (even if no changes)
            if backup_created:
                self._compare_with_backup()
            
            self._cleanup_unnecessary_backup(backup_created)
    
    def _create_backup(self):
        """
        Create backup of current XML file before making changes.
        
        Returns:
            bool: True if backup was created, False otherwise
        """
        if not os.path.exists(self.xml_file):
            return False
        
        try:
            # Remove old backup and create new one
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
    
    def _detect_changes(self, existing_games):
        """
        Check if current games differ from existing XML data.
        
        Args:
            existing_games (dict): Games loaded from current XML file
            
        Returns:
            bool: True if changes detected, False otherwise
        """
        if len(existing_games) != len(self.games):
            return True
        
        # Compare each game's data
        for game in self.games:
            game_id = str(game.get('game_id'))
            if game_id not in existing_games:
                return True
            
            existing = existing_games[game_id]
            # Check all relevant fields for changes
            fields_to_compare = ['week', 'date', 'away_team', 'home_team', 
                               'location_note', 'time_et', 'time_local', 'network']
            
            for field in fields_to_compare:
                if str(game.get(field, '')) != str(existing.get(field, '')):
                    return True
        
        return False
    
    def _update_xml_with_changes(self, existing_games, backup_created):
        """
        Update XML file and log detailed change information.
        
        Args:
            existing_games (dict): Previous game data for comparison
            backup_created (bool): Whether backup was successfully created
        """
        # Analyze what changed
        stats = self._analyze_change_statistics(existing_games)
        
        # Write updated XML
        self._write_xml_file()
        
        # Log change summary
        self._log_change_summary(stats)
        
        # Show console summary
        print(f"📊 Changes: {stats['new_games']} new, {stats['edited_games']} edited, {stats['unchanged_games']} unchanged")
        
        # Compare with backup for detailed change log
        if backup_created:
            self._compare_with_backup()
    
    def _cleanup_unnecessary_backup(self, backup_created):
        """Remove backup file if no changes were made."""
        if backup_created and os.path.exists(self.backup_file):
            try:
                os.remove(self.backup_file)
                print(f"🗑️ Removed unnecessary backup: {self.backup_file}")
            except Exception:
                pass  # Ignore cleanup errors
    
    # ========================================
    # XML FILE OPERATIONS
    # ========================================
    
    def _load_xml_games(self, filepath):
        """
        Load games from an XML file into a dictionary.
        
        Args:
            filepath (str): Path to XML file
            
        Returns:
            dict: Game data keyed by game_id, empty dict if file doesn't exist or fails
        """
        if not os.path.exists(filepath):
            return {}
        
        try:
            tree = parse(filepath)
            root = tree.getroot()
            games_dict = {}
            
            # Navigate through the XML structure: week -> date -> game
            for week_elem in root.findall('week'):
                week_number = week_elem.get('number', '')
                
                for date_elem in week_elem.findall('date'):
                    date_value = date_elem.get('value', '')
                    
                    for game_elem in date_elem.findall('game'):
                        game_id = game_elem.get('id')
                        if not game_id:
                            continue  # Skip games without IDs
                        
                        # Extract all game data from XML structure
                        game_data = {
                            'game_id': game_elem.get('id', ''),
                            'week_game_id': game_elem.get('week_game_id', ''),
                            'game_code': game_elem.get('game_code', ''),
                            'nfl_game_key': game_elem.get('nfl_game_key', ''),
                            'week': week_number,
                            'date': date_value,
                            'away_team': self._get_xml_text(game_elem, './/away_team'),
                            'home_team': self._get_xml_text(game_elem, './/home_team'),
                            'location_note': self._get_xml_text(game_elem, 'location_note'),
                            'time_et': self._get_xml_text(game_elem, './/local_kickoff'),
                            'time_local': self._get_xml_text(game_elem, './/et_kickoff'),
                            'network': self._get_xml_text(game_elem, 'network')
                        }
                        games_dict[game_id] = game_data
            
            return games_dict
            
        except Exception as e:
            print(f"Error reading XML file {filepath}: {e}")
            self._log(f"XML_READ_ERROR: {filepath} - {str(e)}")
            return {}
    
    def _get_xml_text(self, element, xpath):
        """Safely extract text from XML element."""
        found = element.find(xpath)
        return found.text if found is not None else ''
    
    def _write_xml_file(self):
        """Write current games data to XML file with proper structure."""
        root = Element('nfl_schedule')
        root.set('season', '2025')
        root.set('total_games', str(len(self.games)))
        root.set('last_updated', datetime.now().isoformat())
        
        # Group games by week for organized structure
        weeks_data = self._group_games_by_week()
        
        # Create XML structure
        for week_num in sorted(weeks_data.keys(), key=lambda x: int(x) if x.isdigit() else float('inf')):
            week_elem = SubElement(root, 'week')
            week_elem.set('number', str(week_num))
            
            # Group games by date within each week
            dates_data = self._group_games_by_date(weeks_data[week_num])
            
            for date_str, date_games in dates_data.items():
                date_elem = SubElement(week_elem, 'date')
                date_elem.set('value', date_str)
                
                # Add each game to the date
                for game in date_games:
                    self._add_game_to_xml(date_elem, game)
        
        # Write formatted XML to file
        self._save_formatted_xml(root)
    
    def _group_games_by_week(self):
        """Group all games by week number."""
        weeks = {}
        for game in self.games:
            week_num = game['week'] if game['week'] else 'Unknown'
            if week_num not in weeks:
                weeks[week_num] = []
            weeks[week_num].append(game)
        return weeks
    
    def _group_games_by_date(self, week_games):
        """Group games within a week by date."""
        dates = {}
        for game in week_games:
            date = game['date'] or 'Unknown'
            if date not in dates:
                dates[date] = []
            dates[date].append(game)
        return dates
    
    def _add_game_to_xml(self, parent_elem, game):
        """Add a single game's data to XML structure."""
        game_elem = SubElement(parent_elem, 'game')
        
        # Add game IDs as attributes for easy access
        game_elem.set('id', str(game.get('game_id', '')))
        game_elem.set('week_game_id', game.get('week_game_id', ''))
        game_elem.set('game_code', game.get('game_code', ''))
        game_elem.set('nfl_game_key', game.get('nfl_game_key', ''))
        
        # Add team information
        teams_elem = SubElement(game_elem, 'teams')
        away_elem = SubElement(teams_elem, 'away_team')
        away_elem.text = game['away_team']
        home_elem = SubElement(teams_elem, 'home_team')
        home_elem.text = game['home_team']
        
        # Add location note if it exists
        if game['location_note']:
            location_elem = SubElement(game_elem, 'location_note')
            location_elem.text = game['location_note']
        
        # Add timing information
        times_elem = SubElement(game_elem, 'times')
        kickoff_elem = SubElement(times_elem, 'local_kickoff')
        kickoff_elem.text = game['time_et']
        et_kickoff_elem = SubElement(times_elem, 'et_kickoff')
        et_kickoff_elem.text = game['time_local']
        
        # Add network information
        network_elem = SubElement(game_elem, 'network')
        network_elem.text = game['network']
    
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
        self._log(f"XML_WRITTEN: {self.xml_file} ({len(self.games)} games)")
    
    # ========================================
    # CHANGE TRACKING AND LOGGING
    # ========================================
    
    def _analyze_change_statistics(self, existing_games):
        """
        Analyze what types of changes occurred between versions.
        
        Args:
            existing_games (dict): Previous game data
            
        Returns:
            dict: Statistics about changes detected
        """
        stats = {
            'new_games': 0,
            'edited_games': 0,
            'unchanged_games': 0,
            'total_existing': len(existing_games),
            'total_current': len(self.games)
        }
        
        for game in self.games:
            game_id = str(game.get('game_id'))
            
            if game_id not in existing_games:
                stats['new_games'] += 1
            else:
                # Check if any field changed
                existing = existing_games[game_id]
                fields_to_check = ['week', 'date', 'away_team', 'home_team', 
                                 'location_note', 'time_et', 'time_local', 'network']
                
                is_changed = any(
                    str(game.get(field, '')) != str(existing.get(field, ''))
                    for field in fields_to_check
                )
                
                if is_changed:
                    stats['edited_games'] += 1
                else:
                    stats['unchanged_games'] += 1
        
        return stats
    
    def _log_change_summary(self, stats):
        """Log summary of changes detected."""
        if stats['new_games'] > 0:
            self._log(f"GAMES_ADDED: {stats['new_games']} new games")
        if stats['edited_games'] > 0:
            self._log(f"GAMES_EDITED: {stats['edited_games']} games modified")
        
        total_changes = stats['new_games'] + stats['edited_games']
        self._log(f"FILE_UPDATED: {total_changes} total changes ({stats['new_games']} new, {stats['edited_games']} edited, {stats['unchanged_games']} unchanged)")
    
    def _compare_with_backup(self):
        """
        Compare current games with backup file and log detailed differences.
        This provides granular change tracking for audit purposes.
        """
        backup_games = self._load_xml_games(self.backup_file)
        
        if not backup_games:
            self._log("COMPARISON: No backup file to compare against")
            return
        
        # Initialize comparison statistics
        comparison_stats = {
            'new_games': 0,
            'edited_games': 0,
            'unchanged_games': 0,
            'removed_games': 0,
            'backup_total': len(backup_games),
            'current_total': len(self.games),
            'edited_details': []
        }
        
        current_game_ids = set()
        backup_game_ids = set(backup_games.keys())
        
        # Analyze each current game vs backup
        for game in self.games:
            game_id = str(game.get('game_id'))
            current_game_ids.add(game_id)
            
            if game_id not in backup_games:
                comparison_stats['new_games'] += 1
            else:
                # Compare all fields for changes
                existing = backup_games[game_id]
                changes = []
                
                fields_to_compare = ['week', 'date', 'away_team', 'home_team', 
                                   'location_note', 'time_et', 'time_local', 'network']
                
                for field in fields_to_compare:
                    old_val = str(existing.get(field, ''))
                    new_val = str(game.get(field, ''))
                    if old_val != new_val:
                        changes.append(f"{field}: '{old_val}' → '{new_val}'")
                
                if changes:
                    comparison_stats['edited_games'] += 1
                    comparison_stats['edited_details'].append(f"Game {game_id}: {', '.join(changes)}")
                else:
                    comparison_stats['unchanged_games'] += 1
        
        # Check for games that were removed
        removed_games = backup_game_ids - current_game_ids
        comparison_stats['removed_games'] = len(removed_games)
        
        # Log all comparison results
        self._log_comparison_results(comparison_stats, removed_games)
        
        # Show console summary
        print(f"📊 Comparison with backup: {comparison_stats['new_games']} new, "
              f"{comparison_stats['edited_games']} edited, {comparison_stats['removed_games']} removed, "
              f"{comparison_stats['unchanged_games']} unchanged")
    
    def _log_comparison_results(self, stats, removed_games):
        """Log detailed comparison results."""
        self._log(f"COMPARISON_VS_BACKUP: {stats['backup_total']} → {stats['current_total']} games")
        
        if stats['new_games'] > 0:
            self._log(f"COMPARISON_NEW: {stats['new_games']} games added")
        
        if stats['edited_games'] > 0:
            self._log(f"COMPARISON_EDITED: {stats['edited_games']} games modified")
            for detail in stats['edited_details']:
                self._log(f"COMPARISON_DETAIL: {detail}")
        
        if stats['removed_games'] > 0:
            self._log(f"COMPARISON_REMOVED: {stats['removed_games']} games removed: {', '.join(removed_games)}")
        
        if stats['unchanged_games'] > 0:
            self._log(f"COMPARISON_UNCHANGED: {stats['unchanged_games']} games unchanged")
        
        total_changes = stats['new_games'] + stats['edited_games'] + stats['removed_games']
        if total_changes == 0:
            self._log("COMPARISON_RESULT: No changes detected")
        else:
            self._log(f"COMPARISON_RESULT: {total_changes} total changes detected")
    
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