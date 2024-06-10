# main.py

from database import NppesDatabase

def main():
    url_2007 = "https://data.nber.org/npi/webdir/csv/2007/npi200711.csv"
    dest_folder = "nppes_data"
    db_file = "nppes.db"

    # Private method to download and store data (should not be called by end-users)
    NppesDatabase._private_download_and_store(url_2007, dest_folder, db_file)

if __name__ == "__main__":
    main()

