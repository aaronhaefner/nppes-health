import sqlite3
import pandas as pd

# Define the columns to keep for the main table
COLUMNS_TO_KEEP = [
    'npi', 'entity', 'replacement_npi', 'ein', 'porgname', 'pcredential',
    'pmailstatename', 'pmailzip', 'pmailcountry', 'plocstatename', 'ploczip', 
    'ploccountry', 'penumdate', 'lastupdate', 'npideactreason', 'npideactdate', 
    'npireactdate', 'pgender'
]

class NppesDatabase:
    def __init__(self, db_file):
        self.db_file = db_file
        self.conn = self._create_connection()
        self._create_main_table()
        self._create_taxonomy_table()
        self._create_medicare_table()

    def _create_connection(self):
        conn = sqlite3.connect(self.db_file)
        return conn

    def _create_main_table(self):
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

    def _create_taxonomy_table(self):
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS nppes_taxonomy (
            npi TEXT,
            ptaxcode TEXT,
            physician INTEGER,
            PRIMARY KEY (npi, ptaxcode),
            FOREIGN KEY (npi) REFERENCES nppes (npi)
        );
        """
        with self.conn:
            self.conn.execute(create_table_sql)
        print("Created table nppes_taxonomy")

    def _create_medicare_table(self):
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS medicare (
            rndrng_npi TEXT PRIMARY KEY,
            rndrng_prvdr_ent_cd TEXT,
            tot_benes INTEGER,
            rndrng_prvdr_mdcr_prtcptg_ind TEXT,
            mdcr_provider INTEGER
        );
        """
        with self.conn:
            self.conn.execute(create_table_sql)
        print("Created table medicare")

    # def subset_and_insert_data(self, df, n=100):
    #     subset_df = df.sample(n=n, random_state=1)
    #     self.insert_data(subset_df)
    #     print(f"Inserted a subset of {len(subset_df)} rows into the database")

    def insert_main_data(self, df):
        subset_df = df.sample(n=100000, random_state=1)
        df.to_sql('nppes', self.conn, if_exists='append', index=False)
        print("Inserted data into nppes")

    def insert_taxonomy_data(self, df):
        df.to_sql('nppes_taxonomy', self.conn, if_exists='append', index=False)
        print("Inserted data into nppes_taxonomy")

    def insert_medicare_data(self, df):
        df.to_sql('medicare', self.conn, if_exists='append', index=False)
        print("Inserted data into medicare")

    def query_fraud_deactivated(self):
        query = "SELECT * FROM nppes WHERE npideactreason = 'FR'"
        df = pd.read_sql_query(query, self.conn)
        if df.empty:
            print("No rows found where npideactreason is 'FR'.")
        else:
            print(f"Found {len(df)} rows where npideactreason is 'FR'.")
        return df

    def close_connection(self):
        self.conn.close()
