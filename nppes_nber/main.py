# main.py

from database import NppesDatabase
from utils import _download_file

def download_csv(url, dest_folder):
    return _download_file(url, dest_folder)

def store_csv(csv_file, db_file):
    db = NppesDatabase(db_file)
    db.load_csv_to_db(csv_file)
    db.close_connection()

def main():
    url_2007 = "https://data.nber.org/npi/webdir/csv/2007/npi200711.csv"
    dest_folder = "nppes_data"
    db_file = "nppes.db"

    # Download the CSV file
    csv_file = download_csv(url_2007, dest_folder)

    # Store the CSV file in the database
    store_csv(csv_file, db_file)

if __name__ == "__main__":
    main()
