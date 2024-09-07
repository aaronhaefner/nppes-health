"""Utility functions for data loading and processing of NPPES using PySpark."""
from pyspark.sql import SparkSession
from global_variables import MAIN_TABLE_COLS_MAPPING
from utils import (
    load_csv_to_df,
    process_inst_data,
    process_indiv_data,
    process_taxonomy_data,
    process_medicare_data,
    load_and_process_deactivations_data,
    save_as_parquet,
)

# Initialize Spark session
spark = (
    SparkSession.builder.appName("DataProcessing")
    .master("local[*]")
    .getOrCreate()
)


# Main processing pipeline
def process_and_save_data(input_csv: str, output_folder: str) -> None:
    """Process NPPES data and save as parquet files.

    Args:
        input_csv (str): Path to the input CSV file
        output_folder (str): Path to the output folder to save the parquet files

    Returns: None
    """
    df = load_csv_to_df(spark, input_csv, test=True)

    save_as_parquet(
        process_inst_data(df), output_folder, output_dir="institutions"
    )
    save_as_parquet(
        process_indiv_data(df, list(MAIN_TABLE_COLS_MAPPING.keys())),
        output_folder,
        output_dir="individuals",
    )
    save_as_parquet(
        process_taxonomy_data(
            load_csv_to_df(spark, "../../input/nucc_taxonomy_2024.csv")
        ),
        output_folder,
        output_dir="taxonomy",
    )
    save_as_parquet(
        process_medicare_data(
            load_csv_to_df(
                spark, "../../input/MUP_PHY_R21_P04_V10_D19_Prov.csv"
            )
        ),
        output_folder,
        output_dir="medicare",
    )
    save_as_parquet(
        load_and_process_deactivations_data(
            spark, "../../input/NPPES Deactivated NPI Report 20240610.xlsx"
        ),
        output_folder,
        output_dir="deactivations",
    )


if __name__ == "__main__":
    input_csv = "../../input/npi_2024.csv"
    output_folder = "../../output/"
    process_and_save_data(input_csv, output_folder)
