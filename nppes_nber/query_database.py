# query_database.py

import sqlite3
import pandas as pd

class QueryNppesDatabase:
    def __init__(self, db_file):
        self.db_file = db_file
        self.conn = self._create_connection()

    def _create_connection(self):
        return sqlite3.connect(self.db_file)

    def query_all(self, table_name='nppes'):
        query = f"SELECT * FROM {table_name}"
        return self._execute_query(query)

    def query_by_condition(self, table_name='nppes', condition='1=1'):
        query = f"SELECT * FROM {table_name} WHERE {condition}"
        return self._execute_query(query)

    def query_fraud_deactivated(self, table_name='nppes'):
        condition = "npideactreason = 'FR'"
        return self.query_by_condition(table_name, condition)

    def query_random_subset(self, table_name='nppes', n=10000):
        query = f"SELECT * FROM {table_name} ORDER BY RANDOM() LIMIT {n}"
        return self._execute_query(query)

    def query_subset_with_fraud(self, table_name='nppes', n=10000):
        # Query random subset
        random_subset_df = self.query_random_subset(table_name, n)

        # Query fraud deactivated records
        fraud_df = self.query_fraud_deactivated(table_name)

        # Combine the two DataFrames, ensuring no duplicates
        combined_df = pd.concat([random_subset_df, fraud_df]).drop_duplicates(subset='npi')

        print(f"Combined subset contains {len(combined_df)} rows.")
        return combined_df

    def _execute_query(self, query):
        return pd.read_sql_query(query, self.conn)

    def close_connection(self):
        self.conn.close()

# Example usage
if __name__ == "__main__":
    db_file = "test_nppes.db"
    query_db = QueryNppesDatabase(db_file)
    
    # Query all data
    all_data_df = query_db.query_all()
    print(all_data_df.head())

    # Query data by condition
    condition = "pgender = 'M'"
    condition_data_df = query_db.query_by_condition(condition=condition)
    print(condition_data_df.head())

    # Query fraud deactivated records
    fraud_df = query_db.query_fraud_deactivated()
    print(fraud_df.head())

    # Query random subset
    random_subset_df = query_db.query_random_subset()
    print(random_subset_df.head())

    # Query subset with fraud deactivated records
    subset_with_fraud_df = query_db.query_subset_with_fraud()
    print(subset_with_fraud_df.head())

    query_db.close_connection()
