import sqlite3
import pandas as pd


class NppesDatabase:
    def __init__(self, db_file):
        self.db_file = db_file
        self.conn = self._create_connection()
        self._create_tables()

    def _create_connection(self):
        conn = sqlite3.connect(self.db_file)
        return conn

    def _create_tables(self):
        self._create_main_table()
        self._create_taxonomy_table()
        self._create_medicare_table()

    def _create_main_table(self):
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS nppes (
            npi TEXT PRIMARY KEY,
            entity TEXT,
            replacement_npi TEXT,
            ein TEXT,
            porgname TEXT,
            pcredential TEXT,
            plocstatename TEXT,
            ploczip TEXT,
            penumdate TEXT,
            lastupdate TEXT,
            npideactreason TEXT,
            npideactdate TEXT,
            npireactdate TEXT,
            pgender TEXT,
            ptaxcode TEXT
        );
        """
        with self.conn:
            self.conn.execute(create_table_sql)

    def _create_taxonomy_table(self):
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS taxonomy (
            ptaxcode TEXT,
            physician INTEGER,
            student INTEGER,
            np_type TEXT,
            np INTEGER,
            Grouping, TEXT,
            PRIMARY KEY (ptaxcode)
        );
        """
        with self.conn:
            self.conn.execute(create_table_sql)

    def _create_medicare_table(self):
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS medicare (
            npi TEXT PRIMARY KEY,
            mdcr_provider INTEGER
        );
        """
        with self.conn:
            self.conn.execute(create_table_sql)

    def insert_main_data(self, df):
        df.to_sql("nppes", self.conn, if_exists="append", index=False)
        print("Inserted data into nppes")

    def insert_taxonomy_data(self, df):
        df.to_sql("taxonomy", self.conn, if_exists="append", index=False)
        print("Inserted data into taxonomy")

    def insert_medicare_data(self, df):
        df.to_sql("medicare", self.conn, if_exists="append", index=False)
        print("Inserted data into medicare")

    def run_query(self, query):
        df = pd.read_sql_query(query, self.conn)
        print(df)
        return df

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
