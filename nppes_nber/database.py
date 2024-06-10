# test_database.py

import sqlite3
import pandas as pd

# Define the columns to keep
COLUMNS_TO_KEEP = [
    'npi', 'entity', 'replacement_npi', 'ein', 'porgname', 'pcredential',
    'pmailstatename', 'pmailzip', 'pmailcountry', 'plocstatename', 'ploczip', 
    'ploccountry', 'penumdate', 'lastupdate', 'npideactreason', 'npideactdate', 
    'npireactdate', 'pgender'
]

class TestNppesDatabase:
    def __init__(self, db_file):
        self.db_file = db_file
        self.conn = self._create_connection()
        self._create_table()

    def _create_connection(self):
        conn = sqlite3.connect(self.db_file)
        return conn

    def _create_table(self):
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS nppes (
            npi TEXT PRIMARY KEY,
            entity TEXT,
            replacement_npi TEXT,
            ein TEXT,
            porgname TEXT,
            pcredential TEXT,
            pmailstatename TEXT,
            pmailzip TEXT,
            pmailcountry TEXT,
            plocstatename TEXT,
            ploczip TEXT,
            ploccountry TEXT,
            penumdate TEXT,
            lastupdate TEXT,
            npideactreason TEXT,
            npideactdate TEXT,
            npireactdate TEXT,
            pgender TEXT
        );
        """
        with self.conn:
            self.conn.execute(create_table_sql)
        print("Created table nppes")

    def insert_data(self, df):
        df.to_sql('nppes', self.conn, if_exists='append', index=False)
        print("Inserted data into nppes")

    def load_csv_to_db(self, csv_file):
        df = pd.read_csv(csv_file)
        # Filter data to keep only rows where entity is 1
        df = df[df['entity'] == 1]
        # Keep only the specified columns
        df = df[COLUMNS_TO_KEEP]
        # Remove duplicates based on the 'npi' column
        df.drop_duplicates(subset=['npi'], inplace=True)
        print(f"Loaded {len(df)} rows from {csv_file}")
        return df

    def close_connection(self):
        self.conn.close()

    def subset_and_insert_data(self, df, n=100):
        subset_df = df.sample(n=n, random_state=1)
        self.insert_data(subset_df)
        print(f"Inserted a subset of {len(subset_df)} rows into the database")

    def insert_all_data(self, df):
        self.insert_data(df)
        print(f"Inserted all {len(df)} rows into the database")

    def query_fraud_deactivated(self):
        query = "SELECT * FROM nppes WHERE npideactreason = 'FR'"
        df = pd.read_sql_query(query, self.conn)
        if df.empty:
            print("No rows found where npideactreason is 'FR'.")
        else:
            print(f"Found {len(df)} rows where npideactreason is 'FR'.")
        return df

# Example usage
if __name__ == "__main__":
    db_file = "nppes.db"
    csv_file = "../nppes_data/2007/npi_2007.csv"

    test_db = TestNppesDatabase(db_file)
    
    # Load data from the local CSV file
    df = test_db.load_csv_to_db(csv_file)
    
    # Insert all data into the database
    test_db.insert_all_data(df)

    # Query for fraud deactivated records
    fraud_df = test_db.query_fraud_deactivated()

    test_db.close_connection()
