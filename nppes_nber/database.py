# database.py

import sqlite3
import pandas as pd

def create_connection(db_file):
    conn = sqlite3.connect(db_file)
    return conn

def create_table(conn):
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
    with conn:
        conn.execute(create_table_sql)
    print("Created table nppes")

def insert_data(conn, df):
    df.to_sql('nppes', conn, if_exists='append', index=False)
    print("Inserted data into nppes")

def load_csv_to_db(csv_file, db_file):
    conn = create_connection(db_file)
    df = pd.read_csv(csv_file)
    create_table(conn)
    insert_data(conn, df)
    conn.close()
