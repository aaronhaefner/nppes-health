from database import NppesDatabase, COLUMNS_TO_KEEP
from utils import load_csv_to_dataframe, process_main_data, process_taxonomy_data, process_medicare_data, download_file

def process_and_store_nppes_data(npi_csv_file, taxonomy_csv_file, db_file):
    db = NppesDatabase(db_file)
    
    # Load NPI data
    npi_df = load_csv_to_dataframe(npi_csv_file)
    
    # Load taxonomy data
    taxonomy_df = load_csv_to_dataframe(taxonomy_csv_file)
    
    # Process and insert main data into the database
    main_df = process_main_data(npi_df, COLUMNS_TO_KEEP)
    db.insert_main_data(main_df)

    # Process and insert taxonomy data into the database
    taxonomy_data_df = process_taxonomy_data(npi_df, taxonomy_df)
    db.insert_taxonomy_data(taxonomy_data_df)

    db.close_connection()

def process_and_store_medicare_data(medicare_csv_file, db_file):
    db = NppesDatabase(db_file)
    
    # Load Medicare data
    medicare_df = load_csv_to_dataframe(medicare_csv_file)

    # Process and insert Medicare data into the database
    medicare_data_df = process_medicare_data(medicare_df)
    db.insert_medicare_data(medicare_data_df)

    db.close_connection()

def main():
    db_file = "nppes.db"
    npi_csv_file = "../nppes_data/2007/npi_2007.csv"
    taxonomy_csv_file = "../nppes_data/taxonomy/nucc_taxonomy_2007.csv"
    # medicare_csv_file = "../nppes_data/medicare/MUP_PHY_R19_P04_V10_D13_Prov.csv"

    # Process and store NPPES data
    process_and_store_nppes_data(npi_csv_file, taxonomy_csv_file, db_file)

    # Process and store Medicare data
    # process_and_store_medicare_data(medicare_csv_file, db_file)

if __name__ == "__main__":
    main()
