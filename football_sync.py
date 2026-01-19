import os
import requests
from datetime import datetime, timedelta
from supabase import create_client, Client
import schedule
import time

# Configuration
SUPABASE_URL = os.getenv("SUPABASE_URL")  # Your Supabase project URL
SUPABASE_KEY = os.getenv("SUPABASE_KEY")  # Your Supabase anon/service key
FOOTBALL_API_KEY = os.getenv("FOOTBALL_API_KEY")  # API-Football key (optional)

# Initialize Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

class FootballDataPipeline:
    def __init__(self):
        self.base_url = "https://api.football-data.org/v4"
        self.headers = {}
        
        # If you have an API key, add it (free tier: 10 requests/minute)
        if FOOTBALL_API_KEY:
            self.headers["X-Auth-Token"] = FOOTBALL_API_KEY
    
    def get_premier_league_standings(self):
        """Fetch current Premier League standings"""
        # Premier League competition code: PL (2021)
        url = f"{self.base_url}/competitions/PL/standings"
        
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            data = response.json()
            
            standings = []
            if 'standings' in data and len(data['standings']) > 0:
                for team in data['standings'][0]['table']:
                    standings.append({
                        'position': team['position'],
                        'team_name': team['team']['name'],
                        'team_id': team['team']['id'],
                        'played_games': team['playedGames'],
                        'won': team['won'],
                        'draw': team['draw'],
                        'lost': team['lost'],
                        'points': team['points'],
                        'goals_for': team['goalsFor'],
                        'goals_against': team['goalsAgainst'],
                        'goal_difference': team['goalDifference'],
                        'updated_at': datetime.now().isoformat()
                    })
            
            return standings
        except requests.exceptions.RequestException as e:
            print(f"Error fetching standings: {e}")
            return []
    
    def get_recent_matches(self, days_back=7):
        """Fetch recent Premier League matches"""
        date_from = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
        date_to = datetime.now().strftime('%Y-%m-%d')
        
        url = f"{self.base_url}/competitions/PL/matches"
        params = {
            'dateFrom': date_from,
            'dateTo': date_to
        }
        
        try:
            response = requests.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            data = response.json()
            
            matches = []
            for match in data.get('matches', []):
                matches.append({
                    'match_id': match['id'],
                    'match_date': match['utcDate'],
                    'status': match['status'],
                    'matchday': match['matchday'],
                    'home_team': match['homeTeam']['name'],
                    'home_team_id': match['homeTeam']['id'],
                    'away_team': match['awayTeam']['name'],
                    'away_team_id': match['awayTeam']['id'],
                    'home_score': match['score']['fullTime']['home'],
                    'away_score': match['score']['fullTime']['away'],
                    'updated_at': datetime.now().isoformat()
                })
            
            return matches
        except requests.exceptions.RequestException as e:
            print(f"Error fetching matches: {e}")
            return []
    
    def sync_standings_to_supabase(self):
        """Update standings table in Supabase"""
        standings = self.get_premier_league_standings()
        
        if not standings:
            print("No standings data to sync")
            return
        
        try:
            # Delete old standings for today
            supabase.table('standings').delete().gte(
                'updated_at', datetime.now().date().isoformat()
            ).execute()
            
            # Insert new standings
            result = supabase.table('standings').insert(standings).execute()
            print(f"✓ Synced {len(standings)} teams to standings table")
            
        except Exception as e:
            print(f"Error syncing standings: {e}")
    
    def sync_matches_to_supabase(self):
        """Update matches table in Supabase"""
        matches = self.get_recent_matches()
        
        if not matches:
            print("No match data to sync")
            return
        
        try:
            # Upsert matches (update if exists, insert if new)
            for match in matches:
                supabase.table('matches').upsert(
                    match,
                    on_conflict='match_id'
                ).execute()
            
            print(f"✓ Synced {len(matches)} matches to matches table")
            
        except Exception as e:
            print(f"Error syncing matches: {e}")
    
    def run_daily_sync(self):
        """Main sync function to run daily"""
        print(f"\n{'='*50}")
        print(f"Starting daily sync at {datetime.now()}")
        print(f"{'='*50}")
        
        self.sync_standings_to_supabase()
        self.sync_matches_to_supabase()
        
        print(f"{'='*50}")
        print(f"Sync completed at {datetime.now()}")
        print(f"{'='*50}\n")

def setup_database_tables():
    """
    Create tables in Supabase (run this once via Supabase SQL Editor):
    
    -- Standings table
    CREATE TABLE IF NOT EXISTS standings (
        id BIGSERIAL PRIMARY KEY,
        position INTEGER,
        team_name TEXT,
        team_id INTEGER UNIQUE,
        played_games INTEGER,
        won INTEGER,
        draw INTEGER,
        lost INTEGER,
        points INTEGER,
        goals_for INTEGER,
        goals_against INTEGER,
        goal_difference INTEGER,
        updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
    );
    
    -- Matches table
    CREATE TABLE IF NOT EXISTS matches (
        id BIGSERIAL PRIMARY KEY,
        match_id INTEGER UNIQUE,
        match_date TIMESTAMP WITH TIME ZONE,
        status TEXT,
        matchday INTEGER,
        home_team TEXT,
        home_team_id INTEGER,
        away_team TEXT,
        away_team_id INTEGER,
        home_score INTEGER,
        away_score INTEGER,
        updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
    );
    
    -- Create indexes
    CREATE INDEX IF NOT EXISTS idx_standings_team_id ON standings(team_id);
    CREATE INDEX IF NOT EXISTS idx_matches_match_id ON matches(match_id);
    CREATE INDEX IF NOT EXISTS idx_matches_date ON matches(match_date);
    """
    print("Please create tables using the SQL above in Supabase SQL Editor")

if __name__ == "__main__":
    pipeline = FootballDataPipeline()
    
    # Run immediately on start
    pipeline.run_daily_sync()
    
    # Schedule daily sync at 6 AM
    schedule.every().day.at("06:00").do(pipeline.run_daily_sync)
    
    # Alternative: Run every 24 hours from now
    # schedule.every(24).hours.do(pipeline.run_daily_sync)
    
    print("Scheduler started. Running daily sync at 06:00...")
    print("Press Ctrl+C to stop")
    
    # Keep the script running
    while True:
        schedule.run_pending()
        time.sleep(60)  # Check every minute