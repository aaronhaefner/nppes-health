import os
from global_variables import MAIN_TABLE_COLS_MAPPING, COLS_TO_KEEP
from database import NppesDatabase
from utils import load_csv_to_dataframe, process_main_data, process_taxonomy_data, process_medicare_data, download_file

def process_and_store_nppes_data(npi_csv_file, taxonomy_csv_file, db_file, test=False):
    db = NppesDatabase(db_file)
    # Load and process NPPES data to DB
    npi_df = load_csv_to_dataframe(npi_csv_file, test)
    main_df = process_main_data(npi_df, COLS_TO_KEEP)
    db.insert_main_data(main_df)
    taxonomy_df = load_csv_to_dataframe(taxonomy_csv_file)
    taxonomy_data_df = process_taxonomy_data(taxonomy_df)
    db.insert_taxonomy_data(taxonomy_data_df)

    medicare_df = load_csv_to_dataframe(medicare_csv_file, test)
    # medicare_data_df = process_medicare_data(medicare_df)
    # db.insert_medicare_data(medicare_data_df)

    db.close_connection()


def files_by_year(year: int) -> list[str, str, str]:
    npi_csv_file = f"../input/npi_{year}.csv"
    taxonomy_csv_file = f"../input/nucc_taxonomy_{year}.csv"
    # medicare_csv_file = f"../input/MUP_PHY_R19_P04_V10_D{str(year)[-2:]}_Prov.csv"
    return npi_csv_file, taxonomy_csv_file


def main():
    year = "2013"
    db_file = "nppes.db"
    if os.path.exists(db_file):
        os.remove(db_file)
    
    # npi_csv_file, taxonomy_csv_file, medicare_csv_file = files_by_year(year)
    npi_csv_file, taxonomy_csv_file = files_by_year(year)

    # Process and store NPPES data
    process_and_store_nppes_data(npi_csv_file, taxonomy_csv_file, db_file)

if __name__ == "__main__":
    main()
