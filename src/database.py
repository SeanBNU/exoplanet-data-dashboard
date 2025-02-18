import sqlite3
import pandas as pd
from pathlib import Path
from typing import List, Dict, Any, Optional

class ExoplanetsDB:
    def __init__(self, db_path: str = "exoplanets.db"):
        """Initialize database connection."""
        self.db_path = db_path
        self.conn = None
        self.cursor = None

    def connect(self):
        """Create a database connection."""
        try:
            self.conn = sqlite3.connect(self.db_path)
            self.cursor = self.conn.cursor()
        except sqlite3.Error as e:
            print(f"Error connecting to database: {e}")
            raise

    def close(self):
        """Close the database connection."""
        if self.conn:
            self.conn.close()

    def create_tables(self):
        """Create the exoplanets table if it doesn't exist."""
        try:
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS exoplanets (
                    pl_name TEXT PRIMARY KEY,
                    hostname TEXT,
                    pl_bmasse REAL,  -- Planet mass in Earth masses
                    pl_orbper REAL,  -- Orbital period in days
                    discoverymethod TEXT,
                    disc_year INTEGER,
                    pl_rade REAL     -- Planet radius in Earth radii
                )
            """)
            self.conn.commit()
        except sqlite3.Error as e:
            print(f"Error creating tables: {e}")
            raise

    def import_data(self, csv_path: str):
        """Import data from CSV file into the database."""
        try:
            # Read CSV file
            df = pd.read_csv(csv_path)
            
            # Select only the columns we need
            columns = ['pl_name', 'hostname', 'pl_bmasse', 'pl_orbper', 
                      'discoverymethod', 'disc_year', 'pl_rade']
            df_subset = df[columns]
            
            # Remove any rows where pl_name is null as it's our primary key
            df_subset = df_subset.dropna(subset=['pl_name'])
            
            # Insert data into database
            df_subset.to_sql('exoplanets', self.conn, if_exists='replace', index=False)
            
            print(f"Successfully imported {len(df_subset)} rows")
        except Exception as e:
            print(f"Error importing data: {e}")
            raise

    def get_all_exoplanets(self) -> pd.DataFrame:
        """Retrieve all exoplanet data."""
        try:
            query = "SELECT * FROM exoplanets"
            return pd.read_sql_query(query, self.conn)
        except sqlite3.Error as e:
            print(f"Error retrieving data: {e}")
            raise

    def get_discovery_methods_by_year(self) -> pd.DataFrame:
        """Get count of discovery methods by year."""
        try:
            query = """
                SELECT disc_year, discoverymethod, COUNT(*) as count
                FROM exoplanets
                GROUP BY disc_year, discoverymethod
                ORDER BY disc_year
            """
            return pd.read_sql_query(query, self.conn)
        except sqlite3.Error as e:
            print(f"Error retrieving discovery methods: {e}")
            raise

    def get_basic_stats(self) -> Dict[str, Any]:
        """Get basic statistics about the dataset."""
        try:
            stats = {}
            
            # Total number of planets
            self.cursor.execute("SELECT COUNT(*) FROM exoplanets")
            stats['total_planets'] = self.cursor.fetchone()[0]
            
            # Average planet mass
            self.cursor.execute("SELECT AVG(pl_bmasse) FROM exoplanets WHERE pl_bmasse IS NOT NULL")
            stats['avg_planet_mass'] = self.cursor.fetchone()[0]
            
            # Number of unique host stars
            self.cursor.execute("SELECT COUNT(DISTINCT hostname) FROM exoplanets")
            stats['unique_host_stars'] = self.cursor.fetchone()[0]
            
            return stats
        except sqlite3.Error as e:
            print(f"Error retrieving stats: {e}")
            raise

def initialize_database(csv_path: str, db_path: str = "exoplanets.db") -> ExoplanetsDB:
    """Initialize the database and import data."""
    db = ExoplanetsDB(db_path)
    db.connect()
    db.create_tables()
    db.import_data(csv_path)
    return db

if __name__ == "__main__":
    # Example usage
    csv_path = Path("data/exoplanets.csv")
    db = initialize_database(csv_path)
    
    # Test queries
    print("\nBasic stats:")
    print(db.get_basic_stats())
    
    print("\nSample of discovery methods by year:")
    print(db.get_discovery_methods_by_year().head())
    
    db.close()