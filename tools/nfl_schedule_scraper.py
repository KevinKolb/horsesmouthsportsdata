#!/usr/bin/env python3
"""
NFL Schedule Scraper - Simple Version
Scrapes all games and saves to ALL formats automatically
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import re
import json
import os
import csv
import shutil

class NFLScheduleScraper:
    def __init__(self, data_folder='data'):
        self.url = "https://operations.nfl.com/gameday/nfl-schedule/2025-nfl-schedule/"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        self.games = []
        self.data_folder = os.path.abspath(data_folder)
        
        # Create data folder if it doesn't exist
        if not os.path.exists(self.data_folder):
            os.makedirs(self.data_folder)
            print(f"Created data folder: {self.data_folder}")
        else:
            self._backup_existing_files()
    
    def _backup_existing_files(self):
        """Backup existing files in data folder to a backup subfolder INSIDE data"""
        existing_files = [f for f in os.listdir(self.data_folder) 
                         if os.path.isfile(os.path.join(self.data_folder, f)) 
                         and not f.startswith('backup_')]
        
        if not existing_files:
            return
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_folder = os.path.join(self.data_folder, f"backup_{timestamp}")
        
        try:
            os.makedirs(backup_folder)
            files_backed_up = 0
            for file in existing_files:
                source_path = os.path.join(self.data_folder, file)
                dest_path = os.path.join(backup_folder, file)
                try:
                    shutil.copy2(source_path, dest_path)
                    files_backed_up += 1
                except:
                    pass
            
            if files_backed_up > 0:
                print(f"Backed up {files_backed_up} files to: {backup_folder}")
        except:
            pass
    
    def fetch_page(self):
        """Fetch the NFL schedule page"""
        try:
            print("Fetching NFL schedule...")
            response = requests.get(self.url, headers=self.headers)
            response.raise_for_status()
            print(f"Successfully fetched page ({len(response.text):,} characters)")
            return response.text
        except requests.RequestException as e:
            print(f"Error fetching page: {e}")
            return None
    
    def parse_schedule(self, html_content):
        """Parse the HTML content and extract game information"""
        soup = BeautifulSoup(html_content, 'html.parser')
        
        print("Parsing schedule...")
        self.games = []
        tables = soup.find_all('table')
        
        # Try each table
        for i, table in enumerate(tables):
            rows = table.find_all('tr')
            if len(rows) > 10:  # Skip tiny tables
                self._parse_table_rows(rows)
        
        # Remove duplicates
        unique_games = []
        seen_games = set()
        for game in self.games:
            game_key = f"{game['week']}-{game['away_team']}-{game['home_team']}-{game['date']}"
            if game_key not in seen_games:
                unique_games.append(game)
                seen_games.add(game_key)
        
        self.games = unique_games
        print(f"Found {len(self.games)} games")
        return self.games
    
    def _parse_table_rows(self, rows):
        """Parse table rows and extract games"""
        current_week = None
        current_date = None
        
        for row in rows:
            if self._is_week_row(row):
                current_week = self._extract_week(row)
                continue
            
            if self._is_date_row(row):
                current_date = self._extract_date(row)
                continue
            
            if self._is_game_row(row):
                game_info = self._extract_game_info(row, current_week, current_date)
                if game_info:
                    self.games.append(game_info)
    
    def _is_week_row(self, row):
        """Check if row contains week information"""
        cells = row.find_all(['td', 'th'])
        if len(cells) >= 1:
            text = ' '.join(cell.get_text(strip=True) for cell in cells)
            if re.search(r'WEEK\s+(\d+)', text.upper()):
                return True
        return False
    
    def _is_date_row(self, row):
        """Check if row contains date information"""
        cells = row.find_all(['td', 'th'])
        if len(cells) >= 1:
            text = ' '.join(cell.get_text(strip=True) for cell in cells)
            days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
            months = ['January', 'February', 'March', 'April', 'May', 'June', 
                     'July', 'August', 'September', 'October', 'November', 'December',
                     'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                     'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec', 'Sept']
            
            has_day = any(day in text for day in days)
            has_month = any(month in text for month in months)
            return has_day and has_month
        return False
    
    def _is_game_row(self, row):
        """Check if row contains game information"""
        cells = row.find_all(['td', 'th'])
        if len(cells) < 3:
            return False
        
        first_cell_text = cells[0].get_text(strip=True)
        if ' at ' in first_cell_text or ' vs ' in first_cell_text:
            return True
        return False
    
    def _extract_week(self, row):
        """Extract week number from week row"""
        text = ' '.join(cell.get_text(strip=True) for cell in row.find_all(['td', 'th']))
        week_match = re.search(r'WEEK\s+(\d+)', text.upper())
        return week_match.group(1) if week_match else None
    
    def _extract_date(self, row):
        """Extract date from date row"""
        text = ' '.join(cell.get_text(strip=True) for cell in row.find_all(['td', 'th']))
        return ' '.join(text.split())
    
    def _extract_game_info(self, row, current_week, current_date):
        """Extract game information from game row"""
        cells = row.find_all('td')
        if len(cells) < 3:
            return None
        
        try:
            teams_text = cells[0].get_text(strip=True)
            teams_info = self._parse_teams(teams_text)
            
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
        except:
            return None
    
    def _parse_teams(self, teams_text):
        """Parse team information from teams text"""
        location_note = ""
        
        location_match = re.search(r'\(([^)]+)\)', teams_text)
        if location_match:
            location_note = location_match.group(1)
            teams_text = re.sub(r'\s*\([^)]+\)', '', teams_text)
        
        if ' at ' in teams_text:
            parts = teams_text.split(' at ')
            away_team = parts[0].strip()
            home_team = parts[1].strip()
        elif ' vs ' in teams_text:
            parts = teams_text.split(' vs ')
            away_team = parts[0].strip()
            home_team = parts[1].strip()
        else:
            away_team = teams_text
            home_team = ""
        
        return {
            'away_team': away_team,
            'home_team': home_team,
            'location_note': location_note
        }
    
    def save_all_formats(self):
        """Save games to all supported formats"""
        if not self.games:
            print("No games to save")
            return
        
        print("Saving to all formats...")
        
        # Core formats
        self._save_csv()
        self._save_json()
        self._save_xml()
        self._save_yaml()
        
        # Additional formats
        self._save_excel()
        self._save_tsv()
        self._save_sqlite()
        
        # High-performance formats (with error handling)
        try:
            self._save_parquet()
        except ImportError:
            print("Skipped Parquet (PyArrow not installed)")
        except:
            print("Skipped Parquet (error)")
        
        try:
            self._save_feather()
        except ImportError:
            print("Skipped Feather (PyArrow not installed)")
        except:
            print("Skipped Feather (error)")
        
        try:
            self._save_hdf5()
        except ImportError:
            print("Skipped HDF5 (PyTables not installed)")
        except:
            print("Skipped HDF5 (error)")
        
        # Specialized formats
        try:
            self._save_ics()
        except ImportError:
            print("Skipped ICS (icalendar not installed)")
        except:
            print("Skipped ICS (error)")
        
        self._save_pickle()
        
        print("All supported formats saved!")
        self._show_file_summary()
    
    def _save_csv(self):
        df = pd.DataFrame(self.games)
        filepath = os.path.join(self.data_folder, 'nfl_schedule_2025.csv')
        df.to_csv(filepath, index=False)
        print(f"✅ CSV: {filepath}")
    
    def _save_json(self):
        filepath = os.path.join(self.data_folder, 'nfl_schedule_2025.json')
        with open(filepath, 'w') as f:
            json.dump(self.games, f, indent=2)
        print(f"✅ JSON: {filepath}")
    
    def _save_xml(self):
        from xml.etree.ElementTree import Element, SubElement, tostring
        from xml.dom import minidom
        
        root = Element('nfl_schedule')
        root.set('season', '2025')
        root.set('total_games', str(len(self.games)))
        
        weeks = {}
        for game in self.games:
            week_num = game['week'] if game['week'] else 'Unknown'
            if week_num not in weeks:
                weeks[week_num] = []
            weeks[week_num].append(game)
        
        for week_num in sorted(weeks.keys(), key=lambda x: int(x) if x.isdigit() else float('inf')):
            week_elem = SubElement(root, 'week')
            week_elem.set('number', str(week_num))
            
            current_date = None
            date_elem = None
            
            for game in weeks[week_num]:
                if game['date'] != current_date:
                    current_date = game['date']
                    date_elem = SubElement(week_elem, 'date')
                    date_elem.set('value', current_date or 'Unknown')
                
                game_elem = SubElement(date_elem, 'game')
                
                teams_elem = SubElement(game_elem, 'teams')
                away_elem = SubElement(teams_elem, 'away_team')
                away_elem.text = game['away_team']
                home_elem = SubElement(teams_elem, 'home_team')
                home_elem.text = game['home_team']
                
                if game['location_note']:
                    location_elem = SubElement(game_elem, 'location_note')
                    location_elem.text = game['location_note']
                
                times_elem = SubElement(game_elem, 'times')
                et_time_elem = SubElement(times_elem, 'eastern_time')
                et_time_elem.text = game['time_et']
                local_time_elem = SubElement(times_elem, 'local_time')
                local_time_elem.text = game['time_local']
                
                network_elem = SubElement(game_elem, 'network')
                network_elem.text = game['network']
        
        rough_string = tostring(root, 'utf-8')
        reparsed = minidom.parseString(rough_string)
        pretty_xml = reparsed.toprettyxml(indent='  ')
        pretty_xml = '\n'.join([line for line in pretty_xml.split('\n') if line.strip()])
        
        filepath = os.path.join(self.data_folder, 'nfl_schedule_2025.xml')
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(pretty_xml)
        print(f"✅ XML: {filepath}")
    
    def _save_yaml(self):
        try:
            import yaml
            
            structured_data = {
                'nfl_schedule': {
                    'season': 2025,
                    'total_games': len(self.games),
                    'weeks': {}
                }
            }
            
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
            
            filepath = os.path.join(self.data_folder, 'nfl_schedule_2025.yaml')
            with open(filepath, 'w', encoding='utf-8') as f:
                yaml.dump(structured_data, f, default_flow_style=False, sort_keys=False)
            print(f"✅ YAML: {filepath}")
            
        except ImportError:
            print("Skipped YAML (PyYAML not installed)")
    
    def _save_excel(self):
        try:
            df = pd.DataFrame(self.games)
            filepath = os.path.join(self.data_folder, 'nfl_schedule_2025.xlsx')
            
            with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name='All Games', index=False)
                
                weeks = df['week'].dropna().unique()
                for week in sorted(weeks, key=lambda x: int(x) if x.isdigit() else float('inf')):
                    week_df = df[df['week'] == week]
                    sheet_name = f'Week {week}'
                    if len(sheet_name) <= 31:
                        week_df.to_excel(writer, sheet_name=sheet_name, index=False)
            
            print(f"✅ Excel: {filepath}")
        except ImportError:
            print("Skipped Excel (openpyxl not installed)")
    
    def _save_tsv(self):
        filepath = os.path.join(self.data_folder, 'nfl_schedule_2025.tsv')
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            if self.games:
                writer = csv.DictWriter(f, fieldnames=self.games[0].keys(), delimiter='\t')
                writer.writeheader()
                writer.writerows(self.games)
        print(f"✅ TSV: {filepath}")
    
    def _save_sqlite(self):
        import sqlite3
        filepath = os.path.join(self.data_folder, 'nfl_schedule_2025.db')
        conn = sqlite3.connect(filepath)
        df = pd.DataFrame(self.games)
        df.to_sql('nfl_schedule', conn, if_exists='replace', index=False)
        conn.close()
        print(f"✅ SQLite: {filepath}")
    
    def _save_parquet(self):
        import pyarrow.parquet as pq
        df = pd.DataFrame(self.games)
        filepath = os.path.join(self.data_folder, 'nfl_schedule_2025.parquet')
        df.to_parquet(filepath, engine='pyarrow', compression='snappy')
        print(f"✅ Parquet: {filepath}")
    
    def _save_feather(self):
        df = pd.DataFrame(self.games)
        filepath = os.path.join(self.data_folder, 'nfl_schedule_2025.feather')
        df.to_feather(filepath)
        print(f"✅ Feather: {filepath}")
    
    def _save_hdf5(self):
        df = pd.DataFrame(self.games)
        filepath = os.path.join(self.data_folder, 'nfl_schedule_2025.h5')
        df.to_hdf(filepath, key='nfl_schedule', mode='w', complevel=9, complib='zlib')
        print(f"✅ HDF5: {filepath}")
    
    def _save_ics(self):
        from icalendar import Calendar, Event
        
        cal = Calendar()
        cal.add('prodid', '-//NFL Schedule Scraper//mxm.dk//')
        cal.add('version', '2.0')
        cal.add('x-wr-calname', 'NFL 2025 Schedule')
        
        for game in self.games:
            event = Event()
            title = f"{game['away_team']} @ {game['home_team']}"
            if game['location_note']:
                title += f" ({game['location_note']})"
            
            event.add('summary', title)
            event.add('description', f"Network: {game['network']}\nWeek: {game['week']}")
            event.add('dtstart', datetime.now())
            event.add('dtend', datetime.now())
            cal.add_component(event)
        
        filepath = os.path.join(self.data_folder, 'nfl_schedule_2025.ics')
        with open(filepath, 'wb') as f:
            f.write(cal.to_ical())
        print(f"✅ ICS: {filepath}")
    
    def _save_pickle(self):
        import pickle
        data = {
            'metadata': {
                'scraped_at': datetime.now().isoformat(),
                'total_games': len(self.games),
                'season': 2025
            },
            'games': self.games
        }
        filepath = os.path.join(self.data_folder, 'nfl_schedule_2025.pkl')
        with open(filepath, 'wb') as f:
            pickle.dump(data, f)
        print(f"✅ Pickle: {filepath}")
    
    def _show_file_summary(self):
        """Show final summary of all saved files"""
        files = [f for f in os.listdir(self.data_folder) if os.path.isfile(os.path.join(self.data_folder, f)) and not f.startswith('backup_')]
        if files:
            print(f"\n📁 FILES SAVED TO '{self.data_folder}':")
            total_size = 0
            for file in sorted(files):
                file_path = os.path.join(self.data_folder, file)
                size = os.path.getsize(file_path)
                total_size += size
                size_str = f"{size:,} bytes" if size < 1024 else f"{size/1024:.1f} KB" if size < 1048576 else f"{size/1048576:.1f} MB"
                print(f"   📄 {file:<35} ({size_str})")
            
            total_str = f"{total_size:,} bytes" if total_size < 1024 else f"{total_size/1024:.1f} KB" if total_size < 1048576 else f"{total_size/1048576:.1f} MB"
            print(f"\n💾 Total: {total_str} | Files: {len(files)}")

def main():
    """Main function"""
    scraper = NFLScheduleScraper()
    
    html_content = scraper.fetch_page()
    if html_content:
        games = scraper.parse_schedule(html_content)
        if games:
            scraper.save_all_formats()
            print(f"\n🏈 COMPLETE: {len(games)} NFL games saved to all formats!")
        else:
            print("No games found")
    else:
        print("Failed to fetch schedule")

if __name__ == "__main__":
    main()