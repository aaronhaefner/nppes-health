# database.py

import sqlite3
import pandas as pd
from utils import _download_file

class NppesDatabase:
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
            NPI TEXT PRIMARY KEY,
            Entity_Type_Code TEXT,
            Replacement_NPI TEXT,
            Employer_Identification_Number TEXT,
            Provider_Organization_Name TEXT,
            Provider_Last_Name TEXT,
            Provider_First_Name TEXT,
            Provider_Middle_Name TEXT,
            Provider_Name_Prefix_Text TEXT,
            Provider_Name_Suffix_Text TEXT,
            Provider_Credential_Text TEXT,
            Provider_Other_Organization_Name TEXT,
            Provider_Other_Organization_Name_Type_Code TEXT,
            Provider_Other_Last_Name TEXT,
            Provider_Other_First_Name TEXT,
            Provider_Other_Middle_Name TEXT,
            Provider_Other_Name_Prefix_Text TEXT,
            Provider_Other_Name_Suffix_Text TEXT,
            Provider_Other_Credential_Text TEXT,
            Provider_Other_Last_Name_Type_Code TEXT
            -- Add other necessary columns here
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
        self.insert_data(df)

    def close_connection(self):
        self.conn.close()

    @staticmethod
    def _private_download_and_store(url, dest_folder, db_file):
        csv_file = _download_file(url, dest_folder)
        db = NppesDatabase(db_file)
        db.load_csv_to_db(csv_file)
        db.close_connection()
