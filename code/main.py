# Description: Main script to run the NPPES pipeline
import os
import sys
import pandas as pd
from database import NppesDatabase
from global_variables import MAIN_TABLE_COLS_MAPPING, COLS_TO_KEEP
from utils import load_csv_to_df, process_indiv_data, process_taxonomy_data


def nppes_table(npi_csv_file: str,
                cols: list,
                test: bool = False) -> pd.DataFrame:
    """
    Load and process NPPES main data

    Args:
        npi_csv_file (str): Path to the NPI CSV file
        cols (list): Columns to keep in the processed DataFrame
        test (bool): Load only a subset of the data for testing

    Returns:
        pd.DataFrame: Processed NPPES main data
    """
    return process_indiv_data(load_csv_to_df(npi_csv_file, test), cols)


def taxonomy_table(taxonomy_csv_file: str, test: bool = False) -> pd.DataFrame:
    """
    Load and process NPPES taxonomy data

    Args:
        taxonomy_csv_file (str): Path to the taxonomy CSV file
        test (bool): Load only a subset of the data for testing

    Returns:
        pd.DataFrame: Processed NPPES taxonomy data
    """
    return process_taxonomy_data(load_csv_to_df(taxonomy_csv_file))


def nppes_pipeline(npi_csv_file: str,
                   taxonomy_csv_file: str,
                   db_file:str,
                   test: bool = False) -> None:
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
    db.insert_main_data(nppes_table(npi_csv_file, COLS_TO_KEEP, test))
    db.insert_taxonomy_data(taxonomy_table(taxonomy_csv_file, test))
    db.close_connection()


def files_by_year(year: int) -> tuple:
    """
    Get the NPI and taxonomy CSV files for a given year

    Args:
        year (int): Year to get the files for

    Returns:
        tuple: NPI and taxonomy CSV file paths
    """
    return (f"../input/npi_{year}.csv", f"../input/nucc_taxonomy_{year}.csv")


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
    year = 2024
    db_file = "../output/nppes.db"

    # Check if the db file exists and run queries to check the data
    if os.path.exists(db_file):
        db = NppesDatabase(db_file)
        print("Querying nppes table:")
        db.run_query("SELECT * FROM nppes LIMIT 5")
        print("\nQuerying taxonomy table:")
        db.run_query("SELECT * FROM taxonomy LIMIT 5")
        db.close_connection()

    # Run the pipeline
    else:
        main(year, db_file)
