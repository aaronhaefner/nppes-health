# Description: Main script to run the NPPES pipeline
import os
import sys
from database import NppesDatabase
from global_variables import MAIN_TABLE_COLS_MAPPING, COLS_TO_KEEP
from utils import load_csv_to_df, process_indiv_data, process_taxonomy_data


def nppes_pipeline(npi_csv_file, taxonomy_csv_file, db_file, test=False):
    """
    NPPES pipeline to load and process NPPES data and insert into SQL database

    Args:
        npi_csv_file (str): Path to the NPI CSV file
        taxonomy_csv_file (str): Path to the taxonomy CSV file
        db_file (str): Path to the SQLite database file
        test (bool): Test flag to load only a subset of the data for testing

    Returns: None
    """
    db = NppesDatabase(db_file)
    # Load and process NPPES data to DB
    npi_df = load_csv_to_df(npi_csv_file, test)
    main_df = process_indiv_data(npi_df, COLS_TO_KEEP)
    db.insert_main_data(main_df)
    taxonomy_df = load_csv_to_df(taxonomy_csv_file)
    taxonomy_data_df = process_taxonomy_data(taxonomy_df)
    db.insert_taxonomy_data(taxonomy_data_df)

    db.close_connection()


def files_by_year(year: int) -> tuple:
    """
    Get the NPI and taxonomy CSV files for a given year

    Args:
        year (int): Year to get the files for

    Returns:
        tuple: NPI and taxonomy CSV file paths
    """
    npi_csv_file = f"../input/npi_{year}.csv"
    taxonomy_csv_file = f"../input/nucc_taxonomy_{year}.csv"
    return (npi_csv_file, taxonomy_csv_file)


def main(year: int, db_file: str = None):
    """
    Main function to run the NPPES pipeline
    
    Args:
        year (int): Year of the NPPES data
        db_file (str): Path to the SQLite database file
    
    Returns: None
    """
    if os.path.exists(db_file):
        os.remove(db_file)
    npi_csv_file, taxonomy_csv_file = files_by_year(year)
    nppes_pipeline(npi_csv_file, taxonomy_csv_file, db_file)


if __name__ == "__main__":
    # Check for sys args, if none then year = 2024
    if len(sys.argv) > 1:
        year = sys.argv[1]
    else:
        year = 2024
    db_file = "../output/nppes.db"
    main(year, db_file)
