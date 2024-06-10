import sqlite3
import pandas as pd

# Define the columns to keep for the main table
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
        self._create_main_table()
        self._create_taxonomy_table()

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

    def insert_main_data(self, df):
        df.to_sql('nppes', self.conn, if_exists='append', index=False)
        print("Inserted data into nppes")

    def insert_taxonomy_data(self, df):
        df.to_sql('nppes_taxonomy', self.conn, if_exists='append', index=False)
        print("Inserted data into nppes_taxonomy")

    def load_csv(self, csv_file):
        df = pd.read_csv(csv_file)
        print(f"Loaded {len(df)} rows from {csv_file}")
        return df

    def load_taxonomy_csv(self, csv_file):
        encodings = ['utf-8', 'latin1', 'cp1252']
        for encoding in encodings:
            try:
                df = pd.read_csv(csv_file, encoding=encoding)
                print(f"Loaded {len(df)} rows from {csv_file} with encoding {encoding}")
                return df
            except UnicodeDecodeError:
                print(f"Failed to load {csv_file} with encoding {encoding}")
        raise UnicodeDecodeError("All encodings failed to decode the CSV file.")

    def process_main_data(self, df):
        # Filter data to keep only rows where entity is 1
        df = df[df['entity'] == 1]
        # Keep only the specified columns
        df = df[COLUMNS_TO_KEEP]
        # Remove duplicates based on the 'npi' column
        df.drop_duplicates(subset=['npi'], inplace=True)
        return df

    def process_taxonomy_data(self, df, taxonomy_df):
        if 'ptaxcode1' in df.columns:
            temp_df = df[['npi', 'ptaxcode1']].dropna()
            temp_df.rename(columns={'ptaxcode1': 'ptaxcode'}, inplace=True)
            temp_df = temp_df.merge(taxonomy_df[['Code', 'Type']], left_on='ptaxcode', right_on='Code', how='left')
            temp_df['physician'] = temp_df['Type'].apply(lambda x: 1 if 'Allopathic & Osteopathic Physicians' in str(x) else 0)
            temp_df.drop(columns=['Code', 'Type'], inplace=True)
            return temp_df.drop_duplicates()
        return pd.DataFrame(columns=['npi', 'ptaxcode', 'physician'])

    def close_connection(self):
        self.conn.close()

    def subset_and_insert_data(self, df, n=100):
        subset_df = df.sample(n=n, random_state=1)
        self.insert_main_data(subset_df)
        print(f"Inserted a subset of {len(subset_df)} rows into the database")

    def insert_all_data(self, df):
        self.insert_main_data(df)
        print(f"Inserted all {len(df)} rows into the database")

    def insert_all_taxonomy_data(self, df):
        self.insert_taxonomy_data(df)
        print(f"Inserted all taxonomy data into the database")

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
    npi_csv_file = "../nppes_data/2007/npi_2007.csv"
    taxonomy_csv_file = "../nppes_data/taxonomy/nucc_taxonomy_2007.csv"

    test_db = TestNppesDatabase(db_file)
    
    # Load NPI data from the local CSV file
    npi_df = test_db.load_csv(npi_csv_file)
    
    # Load taxonomy data from the local CSV file
    taxonomy_df = test_db.load_taxonomy_csv(taxonomy_csv_file)
    
    # Process and insert main data into the database
    main_df = test_db.process_main_data(npi_df)
    test_db.subset_and_insert_data(main_df, n=100000)

    # Process and insert taxonomy data into the database
    taxonomy_data_df = test_db.process_taxonomy_data(npi_df, taxonomy_df)
    test_db.insert_all_taxonomy_data(taxonomy_data_df)

    # Query for fraud deactivated records
    fraud_df = test_db.query_fraud_deactivated()

    test_db.close_connection()
