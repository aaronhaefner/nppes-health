# main.py

from utils import download_file
from database import load_csv_to_db

def download_and_store(url, dest_folder, db_file):
    csv_file = download_file(url, dest_folder)
    load_csv_to_db(csv_file, db_file)

if __name__ == "__main__":
    url_2007 = "https://data.nber.org/npi/webdir/csv/2007/npi200711.csv"
    dest_folder = "nppes_data"
    db_file = "nppes.db"

    download_and_store(url_2007, dest_folder, db_file)
