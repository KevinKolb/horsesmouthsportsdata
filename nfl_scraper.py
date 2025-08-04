#!/usr/bin/env python3
"""
NFL Schedule Scraper
Fetches and parses the 2025 NFL schedule from operations.nfl.com
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import re
import json
import os
from typing import Optional, Dict, Any
import csv
import shutil

class NFLScheduleScraper:
    def __init__(self, data_folder='data'):
        self.url = "https://operations.nfl.com/gameday/nfl-schedule/2025-nfl-schedule/"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        self.games = []
        self.data_folder = data_folder
        
        # Create data folder if it doesn't exist
        if not os.path.exists(self.data_folder):
            os.makedirs(self.data_folder)
            print(f"Created data folder: {self.data_folder}")
        else:
            # Backup existing files before new scraping
            self._backup_existing_files()
    
    def _backup_existing_files(self):
        """Backup existing files in data folder to a backup subfolder"""
        if not os.path.exists(self.data_folder):
            return
        
        # Get list of existing files
        existing_files = [f for f in os.listdir(self.data_folder) 
                         if os.path.isfile(os.path.join(self.data_folder, f))]
        
        if not existing_files:
            print(f"No existing files in '{self.data_folder}' folder")
            return
        
        # Create backup folder with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_folder = os.path.join(self.data_folder, f"backup_{timestamp}")
        
        try:
            os.makedirs(backup_folder)
            
            # Copy all existing files to backup folder
            files_backed_up = 0
            for file in existing_files:
                # Skip backup folders themselves
                if file.startswith('backup_'):
                    continue
                    
                source_path = os.path.join(self.data_folder, file)
                dest_path = os.path.join(backup_folder, file)
                
                try:
                    shutil.copy2(source_path, dest_path)  # copy2 preserves metadata
                    files_backed_up += 1
                except Exception as e:
                    print(f"Warning: Could not backup {file}: {e}")
            
            if files_backed_up > 0:
                print(f"💾 Backed up {files_backed_up} existing files to: {backup_folder}")
            else:
                # Remove empty backup folder
                os.rmdir(backup_folder)
                
        except Exception as e:
            print(f"Warning: Could not create backup folder: {e}")
    
    def _get_filepath(self, filename):
        """Get full file path within the data folder"""
        return os.path.join(self.data_folder, filename)
    
    def fetch_page(self):
        """Fetch the NFL schedule page"""
        try:
            print(f"🌐 Fetching NFL schedule from: {self.url}")
            response = requests.get(self.url, headers=self.headers)
            response.raise_for_status()
            
            print(f"✅ Successfully fetched page ({len(response.text):,} characters)")
            
            # Save raw HTML for debugging if needed
            debug_file = os.path.join(self.data_folder, 'debug_raw_html.txt')
            try:
                with open(debug_file, 'w', encoding='utf-8') as f:
                    f.write(response.text[:5000])  # First 5000 chars
                print(f"📄 Saved HTML sample to: {debug_file}")
            except:
                pass
            
            return response.text
        except requests.RequestException as e:
            print(f"❌ Error fetching the page: {e}")
            return None
    
    def parse_schedule(self, html_content):
        """Parse the HTML content and extract game information"""
        soup = BeautifulSoup(html_content, 'html.parser')
        
        print("🔍 Analyzing HTML structure...")
        
        # Clear any existing games to start fresh
        self.games = []
        
        # First, let's try to find ALL possible table structures
        tables = soup.find_all('table')
        print(f"Found {len(tables)} table(s) in HTML")
        
        # Try multiple strategies to find schedule data
        total_games_found = 0
        
        # Strategy 1: Look for tbody
        tbody = soup.find('tbody')
        if tbody:
            rows = tbody.find_all('tr')
            print(f"📊 Found tbody with {len(rows)} rows")
            games_found = self._parse_table_rows(rows, "tbody")
            total_games_found += games_found
        
        # Strategy 2: Try each table individually (might have multiple schedule tables)
        for i, table in enumerate(tables):
            rows = table.find_all('tr')
            print(f"📊 Table {i+1}: {len(rows)} rows")
            if len(rows) > 10:  # Skip tiny tables
                print(f"🎯 Analyzing table {i+1}")
                games_found = self._parse_table_rows(rows, f"table-{i+1}")
                total_games_found += games_found
        
        # Strategy 3: If still low count, try all tr elements
        if len(self.games) < 50:
            print("🔄 Trying all <tr> elements in document...")
            all_rows = soup.find_all('tr')
            print(f"📊 Found {len(all_rows)} total <tr> elements")
            games_found = self._parse_table_rows(all_rows, "all-rows")
            total_games_found += games_found
        
        # Remove duplicates (in case we parsed the same game multiple times)
        unique_games = []
        seen_games = set()
        for game in self.games:
            game_key = f"{game['week']}-{game['away_team']}-{game['home_team']}-{game['date']}"
            if game_key not in seen_games:
                unique_games.append(game)
                seen_games.add(game_key)
        
        self.games = unique_games
        
        print(f"✅ Total unique games extracted: {len(self.games)}")
        
        # Show week distribution
        week_counts = {}
        for game in self.games:
            week = game['week'] or 'Unknown'
            week_counts[week] = week_counts.get(week, 0) + 1
        
        print("📊 Games by week:")
        for week in sorted(week_counts.keys(), key=lambda x: int(x) if x.isdigit() else 999):
            print(f"   Week {week}: {week_counts[week]} games")
        
        if len(self.games) < 100:  # Still too few games
            print("⚠️  Still found fewer games than expected!")
            self._detailed_html_analysis(soup)
        
        return self.games
    
    def _parse_table_rows(self, rows, source_name="unknown"):
        """Parse table rows and extract games"""
        current_week = None
        current_date = None
        games_before = len(self.games)
        
        print(f"📋 Processing {len(rows)} rows from {source_name}...")
        
        for i, row in enumerate(rows):
            row_text = row.get_text(strip=True)
            
            # Debug first few rows
            if i < 3:
                print(f"Row {i+1}: {row_text[:80]}...")
            
            # Check row type and process accordingly
            if self._is_week_row(row):
                current_week = self._extract_week(row)
                if current_week:
                    print(f"📅 Week: {current_week}")
                continue
            
            if self._is_date_row(row):
                current_date = self._extract_date(row)
                if current_date:
                    print(f"📆 Date: {current_date}")
                continue
            
            if self._is_game_row(row):
                game_info = self._extract_game_info(row, current_week, current_date)
                if game_info:
                    self.games.append(game_info)
                    # Show first few games found from each source
                    games_from_source = len(self.games) - games_before
                    if games_from_source <= 3:
                        print(f"🏈 Game: {game_info['away_team']} at {game_info['home_team']}")
        
        games_found = len(self.games) - games_before
        print(f"📈 Found {games_found} games from {source_name}")
        return games_found
    
    def _try_div_parsing(self, soup):
        """Try to parse div-based schedule layouts"""
        print("🔍 Looking for div-based schedule...")
        
        # Look for divs containing team names
        game_divs = soup.find_all('div', text=re.compile(r'\w+\s+(at|vs)\s+\w+'))
        if game_divs:
            print(f"Found {len(game_divs)} potential game divs")
        
        # Look for elements with schedule-related classes
        schedule_elements = soup.find_all(attrs={'class': re.compile(r'schedule|game|match|week', re.I)})
        if schedule_elements:
            print(f"Found {len(schedule_elements)} schedule-related elements")
    
    def _detailed_html_analysis(self, soup):
        """Perform detailed analysis when games are missing"""
        print("\n🔬 DETAILED ANALYSIS - Why games might be missing:")
        
        # Check for JavaScript-loaded content
        scripts = soup.find_all('script')
        js_schedule = 0
        for script in scripts:
            if script.string and any(term in script.string.lower() for term in ['schedule', 'game', 'week', 'team']):
                js_schedule += 1
        
        if js_schedule > 0:
            print(f"⚠️  Found {js_schedule} scripts with schedule-related content")
            print("💡 The schedule might be loaded dynamically via JavaScript")
        
        # Look for common NFL team names in the HTML
        nfl_teams = ['Cowboys', 'Eagles', 'Patriots', 'Chiefs', 'Packers', 'Saints', 'Rams', 'Bills', 'Lions', 'Bears']
        teams_found = []
        html_text = soup.get_text().lower()
        
        for team in nfl_teams:
            if team.lower() in html_text:
                teams_found.append(team)
        
        print(f"🏈 Found {len(teams_found)} NFL team names in HTML: {', '.join(teams_found[:5])}...")
        
        # Save more HTML for debugging
        debug_file = os.path.join(self.data_folder, 'full_debug_html.txt')
        try:
            with open(debug_file, 'w', encoding='utf-8') as f:
                f.write(str(soup))
            print(f"📄 Saved full HTML to: {debug_file}")
        except Exception as e:
            print(f"Could not save debug file: {e}")
        
        # Check if the URL might have changed or requires different approach
        print(f"🌐 Current URL: {self.url}")
        print("💡 Possible issues:")
        print("   - Website structure changed")
        print("   - Content loaded via JavaScript")
        print("   - Different URL needed for 2025 schedule")
        print("   - Anti-scraping measures in place")
    
    def _is_week_row(self, row):
        """Check if row contains week information"""
        cells = row.find_all(['td', 'th'])
        if len(cells) >= 1:
            text = ' '.join(cell.get_text(strip=True) for cell in cells)
            # Look for week patterns - be more flexible
            if re.search(r'WEEK\s+(\d+)', text.upper()):
                return True
            # Also check for playoff weeks
            if any(term in text.upper() for term in ['WILDCARD', 'DIVISIONAL', 'CONFERENCE', 'SUPER BOWL']):
                return True
        return False
    
    def _is_date_row(self, row):
        """Check if row contains date information"""
        cells = row.find_all(['td', 'th'])
        if len(cells) >= 1:
            text = ' '.join(cell.get_text(strip=True) for cell in cells)
            # Look for day of week followed by date - be more flexible
            days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
            months = ['January', 'February', 'March', 'April', 'May', 'June', 
                     'July', 'August', 'September', 'October', 'November', 'December',
                     'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                     'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec', 'Sept']
            
            # Check if text contains day and month
            has_day = any(day in text for day in days)
            has_month = any(month in text for month in months)
            
            return has_day and has_month
        return False
    
    def _is_game_row(self, row):
        """Check if row contains game information"""
        cells = row.find_all(['td', 'th'])
        
        # Must have at least 3-4 cells for a game row
        if len(cells) < 3:
            return False
        
        # First cell should contain team names
        first_cell_text = cells[0].get_text(strip=True)
        
        # Look for team matchup patterns
        if ' at ' in first_cell_text or ' vs ' in first_cell_text:
            return True
        
        # Alternative: Look for team names (basic check)
        if len(first_cell_text) > 10 and any(word in first_cell_text for word in ['Eagles', 'Cowboys', 'Patriots', 'Chiefs', 'Packers', 'Saints', 'Rams', 'Bills', 'Dolphins', 'Jets']):
            return True
        
        return False
    
    def _extract_week(self, row):
        """Extract week number from week row"""
        text = ' '.join(cell.get_text(strip=True) for cell in row.find_all(['td', 'th']))
        
        # Look for regular week numbers
        week_match = re.search(r'WEEK\s+(\d+)', text.upper())
        if week_match:
            return week_match.group(1)
        
        # Look for playoff weeks
        playoff_weeks = {
            'WILDCARD': 'Wildcard',
            'WILD CARD': 'Wildcard', 
            'DIVISIONAL': 'Divisional',
            'CONFERENCE': 'Conference',
            'SUPER BOWL': 'Super Bowl'
        }
        
        for playoff_term, week_name in playoff_weeks.items():
            if playoff_term in text.upper():
                return week_name
        
        return None
    
    def _extract_date(self, row):
        """Extract date from date row"""
        text = ' '.join(cell.get_text(strip=True) for cell in row.find_all(['td', 'th']))
        # Remove extra whitespace and normalize
        date_text = ' '.join(text.split())
        return date_text
    
    def _extract_game_info(self, row, week, date):
        """Extract game information from game row"""
        cells = row.find_all('td')
        if len(cells) < 4:
            return None
        
        try:
            # Extract teams
            teams_text = cells[0].get_text(strip=True)
            teams_info = self._parse_teams(teams_text)
            
            # Extract times
            time_et = cells[1].get_text(strip=True) if len(cells) > 1 else ""
            time_local = cells[2].get_text(strip=True) if len(cells) > 2 else ""
            
            # Extract network
            network = cells[3].get_text(strip=True) if len(cells) > 3 else ""
            
            game_info = {
                'week': week,
                'date': date,
                'away_team': teams_info['away_team'],
                'home_team': teams_info['home_team'],
                'location_note': teams_info['location_note'],
                'time_et': time_et,
                'time_local': time_local,
                'network': network
            }
            
            return game_info
            
        except Exception as e:
            print(f"Error parsing game row: {e}")
            return None
    
    def _parse_teams(self, teams_text):
        """Parse team information from teams text"""
        # Handle special cases like international games
        location_note = ""
        
        # Check for location notes in parentheses
        location_match = re.search(r'\(([^)]+)\)', teams_text)
        if location_match:
            location_note = location_match.group(1)
            teams_text = re.sub(r'\s*\([^)]+\)', '', teams_text)
        
        # Split teams by 'at' or 'vs'
        if ' at ' in teams_text:
            parts = teams_text.split(' at ')
            away_team = parts[0].strip()
            home_team = parts[1].strip()
        elif ' vs ' in teams_text:
            parts = teams_text.split(' vs ')
            away_team = parts[0].strip()
            home_team = parts[1].strip()
        else:
            # Fallback - try to split by common patterns
            parts = teams_text.split()
            if len(parts) >= 2:
                mid_point = len(parts) // 2
                away_team = ' '.join(parts[:mid_point])
                home_team = ' '.join(parts[mid_point:])
            else:
                away_team = teams_text
                home_team = ""
        
        return {
            'away_team': away_team,
            'home_team': home_team,
            'location_note': location_note
        }
    
    def save_to_csv(self, filename='nfl_schedule_2025.csv'):
        """Save games to CSV file"""
        if not self.games:
            print("No games to save")
            return
        
        filepath = self._get_filepath(filename)
        df = pd.DataFrame(self.games)
        df.to_csv(filepath, index=False)
        print(f"Schedule saved to {filepath}")
    
    def save_to_json(self, filename='nfl_schedule_2025.json'):
        """Save games to JSON file"""
        if not self.games:
            print("No games to save")
            return
        
        filepath = self._get_filepath(filename)
        with open(filepath, 'w') as f:
            json.dump(self.games, f, indent=2)
        print(f"Schedule saved to {filepath}")
    
    def save_to_xml(self, filename='nfl_schedule_2025.xml'):
        """Save games to XML file"""
        if not self.games:
            print("No games to save")
            return
        
        from xml.etree.ElementTree import Element, SubElement, tostring
        from xml.dom import minidom
        
        # Create root element
        root = Element('nfl_schedule')
        root.set('season', '2025')
        root.set('total_games', str(len(self.games)))
        
        # Group games by week
        weeks = {}
        for game in self.games:
            week_num = game['week'] if game['week'] else 'Unknown'
            if week_num not in weeks:
                weeks[week_num] = []
            weeks[week_num].append(game)
        
        # Create XML structure
        for week_num in sorted(weeks.keys(), key=lambda x: int(x) if x.isdigit() else float('inf')):
            week_elem = SubElement(root, 'week')
            week_elem.set('number', str(week_num))
            week_elem.set('game_count', str(len(weeks[week_num])))
            
            current_date = None
            date_elem = None
            
            for game in weeks[week_num]:
                # Create new date element if date changes
                if game['date'] != current_date:
                    current_date = game['date']
                    date_elem = SubElement(week_elem, 'date')
                    date_elem.set('value', current_date or 'Unknown')
                
                # Create game element
                game_elem = SubElement(date_elem, 'game')
                
                # Add teams
                teams_elem = SubElement(game_elem, 'teams')
                away_elem = SubElement(teams_elem, 'away_team')
                away_elem.text = game['away_team']
                home_elem = SubElement(teams_elem, 'home_team')
                home_elem.text = game['home_team']
                
                # Add location note if exists
                if game['location_note']:
                    location_elem = SubElement(game_elem, 'location_note')
                    location_elem.text = game['location_note']
                
                # Add times
                times_elem = SubElement(game_elem, 'times')
                et_time_elem = SubElement(times_elem, 'eastern_time')
                et_time_elem.text = game['time_et']
                local_time_elem = SubElement(times_elem, 'local_time')
                local_time_elem.text = game['time_local']
                
                # Add network
                network_elem = SubElement(game_elem, 'network')
                network_elem.text = game['network']
        
        # Pretty print XML
        rough_string = tostring(root, 'utf-8')
        reparsed = minidom.parseString(rough_string)
        pretty_xml = reparsed.toprettyxml(indent='  ')
        
        # Remove empty lines
        pretty_xml = '\n'.join([line for line in pretty_xml.split('\n') if line.strip()])
        
        filepath = self._get_filepath(filename)
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(pretty_xml)
        print(f"Schedule saved to {filepath}")
    
    def create_database_schema(self, cursor, db_type='mysql'):
        """Create the database schema for NFL schedule"""
        if db_type.lower() == 'mysql':
            schema_sql = """
            CREATE TABLE IF NOT EXISTS nfl_schedule (
                id INT AUTO_INCREMENT PRIMARY KEY,
                season YEAR DEFAULT 2025,
                week_number VARCHAR(10),
                game_date VARCHAR(100),
                away_team VARCHAR(100) NOT NULL,
                home_team VARCHAR(100) NOT NULL,
                location_note VARCHAR(200),
                time_et VARCHAR(50),
                time_local VARCHAR(50),
                network VARCHAR(50),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                INDEX idx_week (week_number),
                INDEX idx_teams (away_team, home_team),
                INDEX idx_date (game_date),
                INDEX idx_network (network)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
            """
        else:  # PostgreSQL
            schema_sql = """
            CREATE TABLE IF NOT EXISTS nfl_schedule (
                id SERIAL PRIMARY KEY,
                season INTEGER DEFAULT 2025,
                week_number VARCHAR(10),
                game_date VARCHAR(100),
                away_team VARCHAR(100) NOT NULL,
                home_team VARCHAR(100) NOT NULL,
                location_note VARCHAR(200),
                time_et VARCHAR(50),
                time_local VARCHAR(50),
                network VARCHAR(50),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            CREATE INDEX IF NOT EXISTS idx_nfl_week ON nfl_schedule(week_number);
            CREATE INDEX IF NOT EXISTS idx_nfl_teams ON nfl_schedule(away_team, home_team);
            CREATE INDEX IF NOT EXISTS idx_nfl_date ON nfl_schedule(game_date);
            CREATE INDEX IF NOT EXISTS idx_nfl_network ON nfl_schedule(network);
            """
        
        cursor.execute(schema_sql)
        print(f"Database schema created/verified for {db_type}")
    
    def save_to_rds(self, connection_params: Dict[str, Any], db_type='mysql', clear_existing=True):
        """Save games to RDS (MySQL or PostgreSQL)"""
        if not self.games:
            print("No games to save")
            return
        
        try:
            if db_type.lower() == 'mysql':
                import mysql.connector
                connection = mysql.connector.connect(**connection_params)
            else:  # PostgreSQL
                import psycopg2
                connection = psycopg2.connect(**connection_params)
            
            cursor = connection.cursor()
            
            # Create schema
            self.create_database_schema(cursor, db_type)
            
            # Clear existing data if requested
            if clear_existing:
                cursor.execute("DELETE FROM nfl_schedule WHERE season = 2025")
                print("Cleared existing 2025 schedule data")
            
            # Prepare insert statement
            if db_type.lower() == 'mysql':
                insert_sql = """
                INSERT INTO nfl_schedule 
                (week_number, game_date, away_team, home_team, location_note, time_et, time_local, network)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """
            else:  # PostgreSQL
                insert_sql = """
                INSERT INTO nfl_schedule 
                (week_number, game_date, away_team, home_team, location_note, time_et, time_local, network)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """
            
            # Insert games
            game_data = []
            for game in self.games:
                game_data.append((
                    game['week'],
                    game['date'],
                    game['away_team'],
                    game['home_team'],
                    game['location_note'] if game['location_note'] else None,
                    game['time_et'],
                    game['time_local'],
                    game['network']
                ))
            
            cursor.executemany(insert_sql, game_data)
            connection.commit()
            
            print(f"Successfully saved {len(self.games)} games to {db_type.upper()} database")
            
            # Verify the insert
            cursor.execute("SELECT COUNT(*) FROM nfl_schedule WHERE season = 2025")
            count = cursor.fetchone()[0]
            print(f"Total games in database: {count}")
            
        except Exception as e:
            print(f"Error saving to RDS: {e}")
            if 'connection' in locals():
                connection.rollback()
        finally:
            if 'cursor' in locals():
                cursor.close()
            if 'connection' in locals():
                connection.close()
    
    def save_to_yaml(self, filename='nfl_schedule_2025.yaml'):
        """Save games to YAML file"""
        if not self.games:
            print("No games to save")
            return
        
        try:
            import yaml
            
            # Structure data by weeks
            structured_data = {
                'nfl_schedule': {
                    'season': 2025,
                    'total_games': len(self.games),
                    'weeks': {}
                }
            }
            
            # Group by week
            for game in self.games:
                week = game['week'] or 'Unknown'
                if week not in structured_data['nfl_schedule']['weeks']:
                    structured_data['nfl_schedule']['weeks'][week] = []
                
                structured_data['nfl_schedule']['weeks'][week].append({
                    'date': game['date'],
                    'matchup': f"{game['away_team']} at {game['home_team']}",
                    'away_team': game['away_team'],
                    'home_team': game['home_team'],
                    'location_note': game['location_note'],
                    'time_et': game['time_et'],
                    'time_local': game['time_local'],
                    'network': game['network']
                })
            
            with open(filename, 'w') as f:
                yaml.dump(structured_data, f, default_flow_style=False, sort_keys=False)
            print(f"Schedule saved to {filename}")
            
        except ImportError:
            print("PyYAML not installed. Run: pip install PyYAML")
    
    def save_to_parquet(self, filename='nfl_schedule_2025.parquet'):
        """Save games to Parquet file (Apache Arrow format)"""
        if not self.games:
            print("No games to save")
            return
        
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
            
            df = pd.DataFrame(self.games)
            filepath = self._get_filepath(filename)
            df.to_parquet(filepath, engine='pyarrow', compression='snappy')
            print(f"Schedule saved to {filepath}")
            
        except ImportError:
            print("PyArrow not installed. Run: pip install pyarrow")
    
    def save_to_avro(self, filename='nfl_schedule_2025.avro'):
        """Save games to Avro file"""
        if not self.games:
            print("No games to save")
            return
        
        try:
            import avro.schema
            import avro.io
            import avro.datafile
            
            # Define Avro schema
            schema = avro.schema.parse("""
            {
                "type": "record",
                "name": "NFLGame",
                "fields": [
                    {"name": "week", "type": ["null", "string"]},
                    {"name": "date", "type": ["null", "string"]},
                    {"name": "away_team", "type": "string"},
                    {"name": "home_team", "type": "string"},
                    {"name": "location_note", "type": ["null", "string"]},
                    {"name": "time_et", "type": ["null", "string"]},
                    {"name": "time_local", "type": ["null", "string"]},
                    {"name": "network", "type": ["null", "string"]}
                ]
            }
            """)
            
            filepath = self._get_filepath(filename)
            with open(filepath, 'wb') as f:
                writer = avro.datafile.DataFileWriter(f, avro.io.DatumWriter(schema))
                for game in self.games:
                    writer.append(game)
                writer.close()
            
            print(f"Schedule saved to {filepath}")
            
        except ImportError:
            print("Avro library not installed. Run: pip install avro-python3")
    
    def save_to_excel(self, filename='nfl_schedule_2025.xlsx'):
        """Save games to Excel file with multiple sheets"""
        if not self.games:
            print("No games to save")
            return
        
        try:
            df = pd.DataFrame(self.games)
            
            filepath = self._get_filepath(filename)
            with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
                # All games sheet
                df.to_excel(writer, sheet_name='All Games', index=False)
                
                # Sheet by week
                weeks = df['week'].dropna().unique()
                for week in sorted(weeks, key=lambda x: int(x) if x.isdigit() else float('inf')):
                    week_df = df[df['week'] == week]
                    sheet_name = f'Week {week}'
                    if len(sheet_name) <= 31:  # Excel sheet name limit
                        week_df.to_excel(writer, sheet_name=sheet_name, index=False)
                
                # Summary sheet
                summary_data = {
                    'Metric': ['Total Games', 'Total Weeks', 'Networks', 'Teams'],
                    'Count': [
                        len(df),
                        len(df['week'].dropna().unique()),
                        len(df['network'].dropna().unique()),
                        len(set(df['away_team'].tolist() + df['home_team'].tolist()))
                    ]
                }
                summary_df = pd.DataFrame(summary_data)
                summary_df.to_excel(writer, sheet_name='Summary', index=False)
            
            print(f"Schedule saved to {filepath}")
            
        except ImportError:
            print("OpenPyXL not installed. Run: pip install openpyxl")
    
    def save_to_ics(self, filename='nfl_schedule_2025.ics'):
        """Save games to ICS calendar file"""
        if not self.games:
            print("No games to save")
            return
        
        try:
            from icalendar import Calendar, Event
            from datetime import datetime, timedelta
            import pytz
            
            cal = Calendar()
            cal.add('prodid', '-//NFL Schedule Scraper//mxm.dk//')
            cal.add('version', '2.0')
            cal.add('x-wr-calname', 'NFL 2025 Schedule')
            cal.add('x-wr-caldesc', 'Complete 2025 NFL Regular Season Schedule')
            
            for game in self.games:
                event = Event()
                
                # Create event title
                title = f"{game['away_team']} @ {game['home_team']}"
                if game['location_note']:
                    title += f" ({game['location_note']})"
                
                event.add('summary', title)
                event.add('description', f"Network: {game['network']}\nWeek: {game['week']}")
                
                # Parse date and time (simplified - would need more robust parsing)
                try:
                    # This is a simplified example - real implementation would need better date parsing
                    event.add('dtstart', datetime.now())  # Placeholder
                    event.add('dtend', datetime.now() + timedelta(hours=3))  # Placeholder
                except:
                    continue
                
                event.add('location', f"{game['home_team']} Stadium")
                
                cal.add_component(event)
            
            filepath = self._get_filepath(filename)
            with open(filepath, 'wb') as f:
                f.write(cal.to_ical())
            
            print(f"Schedule saved to {filepath}")
            
        except ImportError:
            print("icalendar not installed. Run: pip install icalendar")
    
    def save_to_tsv(self, filename='nfl_schedule_2025.tsv'):
        """Save games to Tab-Separated Values file"""
        if not self.games:
            print("No games to save")
            return
        
        filepath = self._get_filepath(filename)
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            if self.games:
                writer = csv.DictWriter(f, fieldnames=self.games[0].keys(), delimiter='\t')
                writer.writeheader()
                writer.writerows(self.games)
        
        print(f"Schedule saved to {filepath}")
    
    def save_to_pickle(self, filename='nfl_schedule_2025.pkl'):
        """Save games to Python Pickle file"""
        if not self.games:
            print("No games to save")
            return
        
        import pickle
        
        data = {
            'metadata': {
                'scraped_at': datetime.now().isoformat(),
                'total_games': len(self.games),
                'season': 2025
            },
            'games': self.games
        }
        
        filepath = self._get_filepath(filename)
        with open(filepath, 'wb') as f:
            pickle.dump(data, f)
        
        print(f"Schedule saved to {filepath}")
    
    def save_to_hdf5(self, filename='nfl_schedule_2025.h5'):
        """Save games to HDF5 file (hierarchical data format)"""
        if not self.games:
            print("No games to save")
            return
        
        try:
            df = pd.DataFrame(self.games)
            filepath = self._get_filepath(filename)
            df.to_hdf(filepath, key='nfl_schedule', mode='w', complevel=9, complib='zlib')
            print(f"Schedule saved to {filepath}")
            
        except ImportError:
            print("PyTables not installed. Run: pip install tables")
    
    def save_to_feather(self, filename='nfl_schedule_2025.feather'):
        """Save games to Feather file (fast columnar format)"""
        if not self.games:
            print("No games to save")
            return
        
        try:
            df = pd.DataFrame(self.games)
            filepath = self._get_filepath(filename)
            df.to_feather(filepath)
            print(f"Schedule saved to {filepath}")
            
        except ImportError:
            print("PyArrow not installed. Run: pip install pyarrow")
    
    def save_to_sqlite(self, filename='nfl_schedule_2025.db'):
        """Save games to SQLite database file"""
        if not self.games:
            print("No games to save")
            return
        
        import sqlite3
        
        filepath = self._get_filepath(filename)
        conn = sqlite3.connect(filepath)
        df = pd.DataFrame(self.games)
        df.to_sql('nfl_schedule', conn, if_exists='replace', index=False)
        
        # Create indexes for better query performance
        cursor = conn.cursor()
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_week ON nfl_schedule(week)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_teams ON nfl_schedule(away_team, home_team)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_network ON nfl_schedule(network)')
        
        conn.commit()
        conn.close()
        
        print(f"Schedule saved to {filepath}")
    
    def save_all_formats(self):
        """Save games to all supported formats"""
        print(f"Saving to all supported formats in '{self.data_folder}' folder...")
        
        # Core formats
        self.save_to_csv()
        self.save_to_json()
        self.save_to_xml()
        
        # Additional formats
        self.save_to_yaml()
        self.save_to_excel()
        self.save_to_tsv()
        self.save_to_pickle()
        self.save_to_sqlite()
        
        # High-performance formats
        self.save_to_parquet()
        self.save_to_feather()
        self.save_to_hdf5()
        
        # Specialized formats
        self.save_to_avro()
        self.save_to_ics()
        
        print(f"All formats saved successfully in '{self.data_folder}' folder!")
        
        # Print folder contents
        files = [f for f in os.listdir(self.data_folder) if os.path.isfile(os.path.join(self.data_folder, f))]
        print(f"\n📁 Files created in '{self.data_folder}':")
        for file in sorted(files):
            file_path = os.path.join(self.data_folder, file)
            size = os.path.getsize(file_path)
            size_str = f"{size:,} bytes" if size < 1024 else f"{size/1024:.1f} KB" if size < 1048576 else f"{size/1048576:.1f} MB"
            print(f"   📄 {file:<35} ({size_str})")
    
    def prompt_for_additional_formats(self):
        """Prompt user to save additional formats after core formats are saved"""
        print(f"\n🎯 CORE FORMATS SAVED (CSV, JSON, XML)")
        print(f"📁 Files saved to '{self.data_folder}' folder")
        
        # Show available additional formats
        additional_formats = {
            'yaml': 'YAML - Human-readable configuration format',
            'excel': 'Excel - Multi-sheet workbook with summary',
            'parquet': 'Parquet - High-performance columnar format',
            'avro': 'Avro - Schema evolution friendly format',
            'ics': 'ICS - Calendar format for scheduling apps',
            'tsv': 'TSV - Tab-separated values',
            'pickle': 'Pickle - Python serialization format',
            'hdf5': 'HDF5 - Scientific computing format',
            'feather': 'Feather - Fast columnar I/O format',
            'sqlite': 'SQLite - Embedded database'
        }
        
        print(f"\n📋 ADDITIONAL FORMATS AVAILABLE:")
        for fmt, desc in additional_formats.items():
            print(f"   {fmt:<10} : {desc}")
        
        # Prompt for additional formats
        while True:
            print(f"\n❓ Would you like to save additional formats?")
            print(f"   • Type format names (e.g., 'excel yaml sqlite')")
            print(f"   • Type 'all' for all additional formats")
            print(f"   • Type 'none' or press Enter to skip")
            
            choice = input("📝 Your choice: ").strip().lower()
            
            if choice == '' or choice == 'none':
                print("✅ Skipping additional formats")
                break
            elif choice == 'all':
                print("💾 Saving all additional formats...")
                self.save_to_yaml()
                self.save_to_excel()
                self.save_to_parquet()
                self.save_to_avro()
                self.save_to_ics()
                self.save_to_tsv()
                self.save_to_pickle()
                self.save_to_hdf5()
                self.save_to_feather()
                self.save_to_sqlite()
                print("✅ All additional formats saved!")
                break
            else:
                # Parse individual format choices
                requested_formats = choice.split()
                valid_formats = []
                invalid_formats = []
                
                for fmt in requested_formats:
                    if fmt in additional_formats:
                        valid_formats.append(fmt)
                    else:
                        invalid_formats.append(fmt)
                
                if invalid_formats:
                    print(f"❌ Invalid formats: {', '.join(invalid_formats)}")
                    print(f"   Available: {', '.join(additional_formats.keys())}")
                    continue
                
                if valid_formats:
                    print(f"💾 Saving formats: {', '.join(valid_formats)}")
                    
                    for fmt in valid_formats:
                        try:
                            if fmt == 'yaml':
                                self.save_to_yaml()
                            elif fmt == 'excel':
                                self.save_to_excel()
                            elif fmt == 'parquet':
                                self.save_to_parquet()
                            elif fmt == 'avro':
                                self.save_to_avro()
                            elif fmt == 'ics':
                                self.save_to_ics()
                            elif fmt == 'tsv':
                                self.save_to_tsv()
                            elif fmt == 'pickle':
                                self.save_to_pickle()
                            elif fmt == 'hdf5':
                                self.save_to_hdf5()
                            elif fmt == 'feather':
                                self.save_to_feather()
                            elif fmt == 'sqlite':
                                self.save_to_sqlite()
                        except Exception as e:
                            print(f"❌ Error saving {fmt}: {e}")
                    
                    print("✅ Requested formats saved!")
                    break
        
        # Show final file summary
        self._show_final_file_summary()
    
    def _show_final_file_summary(self):
        """Show final summary of all saved files"""
        if os.path.exists(self.data_folder):
            files = [f for f in os.listdir(self.data_folder) if os.path.isfile(os.path.join(self.data_folder, f))]
            if files:
                print(f"\n📁 FINAL FILE SUMMARY - '{self.data_folder}' folder:")
                total_size = 0
                for file in sorted(files):
                    file_path = os.path.join(self.data_folder, file)
                    size = os.path.getsize(file_path)
                    total_size += size
                    size_str = f"{size:,} bytes" if size < 1024 else f"{size/1024:.1f} KB" if size < 1048576 else f"{size/1048576:.1f} MB"
                    print(f"   📄 {file:<35} ({size_str})")
                
                total_str = f"{total_size:,} bytes" if total_size < 1024 else f"{total_size/1024:.1f} KB" if total_size < 1048576 else f"{total_size/1048576:.1f} MB"
                print(f"\n💾 Total size: {total_str} | Files: {len(files)}")
    
    def query_rds_games(self, connection_params: Dict[str, Any], db_type='mysql', 
                       week: Optional[str] = None, team: Optional[str] = None) -> list:
        """Query games from RDS database"""
        try:
            if db_type.lower() == 'mysql':
                import mysql.connector
                connection = mysql.connector.connect(**connection_params)
            else:  # PostgreSQL
                import psycopg2
                connection = psycopg2.connect(**connection_params)
            
            cursor = connection.cursor(dictionary=True if db_type.lower() == 'mysql' else None)
            
            # Build query
            base_query = """
            SELECT week_number, game_date, away_team, home_team, 
                   location_note, time_et, time_local, network
            FROM nfl_schedule 
            WHERE season = 2025
            """
            
            params = []
            if week:
                base_query += " AND week_number = %s"
                params.append(week)
            
            if team:
                base_query += " AND (away_team LIKE %s OR home_team LIKE %s)"
                params.extend([f"%{team}%", f"%{team}%"])
            
            base_query += " ORDER BY week_number, game_date, time_et"
            
            cursor.execute(base_query, params)
            
            if db_type.lower() == 'mysql':
                results = cursor.fetchall()
            else:  # PostgreSQL
                columns = [desc[0] for desc in cursor.description]
                results = [dict(zip(columns, row)) for row in cursor.fetchall()]
            
            return results
            
        except Exception as e:
            print(f"Error querying RDS: {e}")
            return []
        finally:
            if 'cursor' in locals():
                cursor.close()
            if 'connection' in locals():
                connection.close()
    
    def print_schedule(self, limit=None):
        """Print the schedule to console"""
        if not self.games:
            print("No games found")
            return
        
        games_to_show = self.games if not limit else self.games[:limit]
        
        current_week = None
        for game in games_to_show:
            if game['week'] != current_week:
                current_week = game['week']
                print(f"\n=== WEEK {current_week} ===")
            
            print(f"{game['date']}")
            print(f"  {game['away_team']} at {game['home_team']}")
            if game['location_note']:
                print(f"  Location: {game['location_note']}")
            print(f"  Time: {game['time_et']} | Network: {game['network']}")
            print()

def get_rds_config_from_env():
    """Get RDS configuration from environment variables"""
    return {
        'mysql': {
            'host': os.getenv('RDS_MYSQL_HOST', 'localhost'),
            'port': int(os.getenv('RDS_MYSQL_PORT', 3306)),
            'user': os.getenv('RDS_MYSQL_USER', 'root'),
            'password': os.getenv('RDS_MYSQL_PASSWORD', ''),
            'database': os.getenv('RDS_MYSQL_DATABASE', 'nfl_data'),
            'charset': 'utf8mb4'
        },
        'postgresql': {
            'host': os.getenv('RDS_POSTGRES_HOST', 'localhost'),
            'port': int(os.getenv('RDS_POSTGRES_PORT', 5432)),
            'user': os.getenv('RDS_POSTGRES_USER', 'postgres'),
            'password': os.getenv('RDS_POSTGRES_PASSWORD', ''),
            'database': os.getenv('RDS_POSTGRES_DATABASE', 'nfl_data')
        }
    }

def create_sample_config():
    """Create a sample configuration file for RDS connections"""
    config_content = """# NFL Schedule Scraper - RDS Configuration
# Copy this to .env file and update with your RDS credentials

# MySQL/MariaDB RDS Configuration
RDS_MYSQL_HOST=your-mysql-rds-endpoint.region.rds.amazonaws.com
RDS_MYSQL_PORT=3306
RDS_MYSQL_USER=admin
RDS_MYSQL_PASSWORD=your-password
RDS_MYSQL_DATABASE=nfl_data

# PostgreSQL RDS Configuration
RDS_POSTGRES_HOST=your-postgres-rds-endpoint.region.rds.amazonaws.com
RDS_POSTGRES_PORT=5432
RDS_POSTGRES_USER=postgres
RDS_POSTGRES_PASSWORD=your-password
RDS_POSTGRES_DATABASE=nfl_data

# Usage Examples:
# python nfl_schedule_scraper.py --rds mysql
# python nfl_schedule_scraper.py --rds postgresql
# python nfl_schedule_scraper.py --query-week 1 --rds mysql
# python nfl_schedule_scraper.py --query-team "Cowboys" --rds postgresql
"""
    
    with open('rds_config_sample.env', 'w') as f:
        f.write(config_content)
    print("Sample RDS configuration created: rds_config_sample.env")
    print("Copy this to .env and update with your credentials")

def main():
    """Main function to run the scraper"""
    import argparse
    
    parser = argparse.ArgumentParser(description='NFL Schedule Scraper with RDS support')
    parser.add_argument('--rds', choices=['mysql', 'postgresql'], 
                       help='Save to RDS database (mysql or postgresql)')
    parser.add_argument('--query-week', type=str, 
                       help='Query specific week from RDS')
    parser.add_argument('--query-team', type=str, 
                       help='Query games for specific team from RDS')
    parser.add_argument('--create-config', action='store_true', 
                       help='Create sample RDS configuration file')
    parser.add_argument('--no-scrape', action='store_true', 
                       help='Skip scraping, only query database')
    parser.add_argument('--format', choices=[
        'csv', 'json', 'xml', 'yaml', 'excel', 'parquet', 'avro', 
        'ics', 'tsv', 'pickle', 'hdf5', 'feather', 'sqlite', 'all'
    ], help='Specific format to save (default: csv, json, xml)')
    parser.add_argument('--list-formats', action='store_true',
                       help='List all supported formats and their descriptions')
    parser.add_argument('--interactive', action='store_true',
                       help='Generate core formats (CSV, JSON, XML) then prompt for additional formats')
    parser.add_argument('--data-folder', default='data',
                       help='Folder to save all output files (default: data)')
    
    args = parser.parse_args()
    
    if args.list_formats:
        print("📊 SUPPORTED DATA FORMATS:")
        print("="*50)
        formats = {
            'csv': 'Comma-Separated Values - Universal spreadsheet format',
            'json': 'JavaScript Object Notation - Web API standard',
            'xml': 'eXtensible Markup Language - Enterprise data exchange',
            'yaml': 'YAML Ain\'t Markup Language - Human-readable config format',
            'excel': 'Microsoft Excel - Multiple sheets with summary',
            'parquet': 'Apache Parquet - High-performance columnar format',
            'avro': 'Apache Avro - Schema evolution friendly format',
            'ics': 'iCalendar - Calendar format for scheduling apps',
            'tsv': 'Tab-Separated Values - Alternative to CSV',
            'pickle': 'Python Pickle - Native Python serialization',
            'hdf5': 'Hierarchical Data Format - Scientific computing',
            'feather': 'Apache Arrow Feather - Fast columnar I/O',
            'sqlite': 'SQLite Database - Embedded SQL database',
            'mysql': 'MySQL/MariaDB RDS - Cloud relational database',
            'postgresql': 'PostgreSQL RDS - Advanced relational database'
        }
        
        for fmt, desc in formats.items():
            print(f"  {fmt:12} : {desc}")
        
        print(f"\n💡 USAGE EXAMPLES:")
        print(f"  python {os.path.basename(__file__)} --format csv")
        print(f"  python {os.path.basename(__file__)} --format all")
        print(f"  python {os.path.basename(__file__)} --rds mysql")
        return
    
    if args.create_config:
        create_sample_config()
        return
    
    scraper = NFLScheduleScraper(data_folder=args.data_folder)
    rds_config = get_rds_config_from_env()
    
    # Handle database queries without scraping
    if args.no_scrape and (args.query_week or args.query_team or args.rds):
        if not args.rds:
            print("Please specify --rds mysql or --rds postgresql for queries")
            return
        
        print(f"Querying {args.rds.upper()} database...")
        results = scraper.query_rds_games(
            rds_config[args.rds], 
            args.rds, 
            week=args.query_week, 
            team=args.query_team
        )
        
        if results:
            print(f"\nFound {len(results)} games:")
            for game in results:
                print(f"Week {game['week_number']}: {game['away_team']} at {game['home_team']}")
                print(f"  {game['game_date']} | {game['time_et']} | {game['network']}")
                if game['location_note']:
                    print(f"  Location: {game['location_note']}")
                print()
        else:
            print("No games found matching criteria")
        return
    
    # Scrape the schedule
    print("Fetching NFL schedule...")
    html_content = scraper.fetch_page()
    
    if html_content:
        print("Parsing schedule...")
        games = scraper.parse_schedule(html_content)
        
        if games:
            print(f"Found {len(games)} games")
            
            # Always generate core formats first (CSV, JSON, XML)
            print("🚀 Generating core formats (CSV, JSON, XML)...")
            scraper.save_to_csv()
            scraper.save_to_json()
            scraper.save_to_xml()
            
            # Handle command line format specifications
            if args.format:
                if args.format == 'all':
                    scraper.save_all_formats()
                elif args.format == 'csv':
                    pass  # Already saved
                elif args.format == 'json':
                    pass  # Already saved
                elif args.format == 'xml':
                    pass  # Already saved
                elif args.format == 'yaml':
                    scraper.save_to_yaml()
                elif args.format == 'excel':
                    scraper.save_to_excel()
                elif args.format == 'parquet':
                    scraper.save_to_parquet()
                elif args.format == 'avro':
                    scraper.save_to_avro()
                elif args.format == 'ics':
                    scraper.save_to_ics()
                elif args.format == 'tsv':
                    scraper.save_to_tsv()
                elif args.format == 'pickle':
                    scraper.save_to_pickle()
                elif args.format == 'hdf5':
                    scraper.save_to_hdf5()
                elif args.format == 'feather':
                    scraper.save_to_feather()
                elif args.format == 'sqlite':
                    scraper.save_to_sqlite()
            else:
                # Always prompt for additional formats when no specific format specified
                scraper.prompt_for_additional_formats()
            
            # Save to RDS if specified
            if args.rds:
                print(f"\nSaving to {args.rds.upper()} RDS...")
                try:
                    scraper.save_to_rds(rds_config[args.rds], args.rds)
                except ImportError:
                    if args.rds == 'mysql':
                        print("Error: mysql-connector-python not installed. Run: pip install mysql-connector-python")
                    else:
                        print("Error: psycopg2 not installed. Run: pip install psycopg2-binary")
                except Exception as e:
                    print(f"RDS Error: {e}")
                    print("Make sure your .env file has correct RDS credentials")
            
            # Handle specific queries
            if args.rds and (args.query_week or args.query_team):
                print(f"\nQuerying {args.rds.upper()} database...")
                results = scraper.query_rds_games(
                    rds_config[args.rds], 
                    args.rds, 
                    week=args.query_week, 
                    team=args.query_team
                )
                
                if results:
                    print(f"\nQuery Results ({len(results)} games):")
                    for game in results:
                        print(f"Week {game['week_number']}: {game['away_team']} at {game['home_team']}")
                        print(f"  {game['game_date']} | {game['time_et']} | {game['network']}")
                        if game['location_note']:
                            print(f"  Location: {game['location_note']}")
                        print()
            
            # Print summary
            weeks = set(game['week'] for game in games if game['week'])
            networks = set(game['network'] for game in games if game['network'])
            
            print(f"\nSUMMARY:")
            print(f"Total games: {len(games)}")
            print(f"Weeks covered: {sorted(weeks) if weeks else 'None'}")
            print(f"Networks: {sorted(networks) if networks else 'None'}")
            
            # Show all games without limit (unless querying specific data)
            if not (args.query_week or args.query_team):
                print(f"\n=== ALL {len(games)} GAMES SCRAPED ===")
                scraper.print_schedule()
                
            # Show final summary if no interactive prompting occurred
            if args.format and os.path.exists(args.data_folder):
                files = [f for f in os.listdir(args.data_folder) if os.path.isfile(os.path.join(args.data_folder, f))]
                if files:
                    print(f"\n📁 All files saved to '{args.data_folder}' folder:")
                    for file in sorted(files):
                        file_path = os.path.join(args.data_folder, file)
                        size = os.path.getsize(file_path)
                        size_str = f"{size:,} bytes" if size < 1024 else f"{size/1024:.1f} KB" if size < 1048576 else f"{size/1048576:.1f} MB"
                        print(f"   📄 {file:<35} ({size_str})")
            
        else:
            print("No games found. The page structure may have changed.")
    else:
        print("Failed to fetch the schedule page.")

if __name__ == "__main__":
    main()