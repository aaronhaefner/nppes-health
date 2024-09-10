"""Utility functions for data loading and processing of NPPES using PySpark."""
from pyspark.sql import SparkSession
from global_variables import MAIN_TABLE_COLS_MAPPING
from utils import (
    load_csv_to_df,
    load_parquet,
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
def process_and_save_data(
    year: int, output_folder: str, test: bool = False
) -> None:
    """Process NPPES data and save as parquet files.

    Args:
        year (int): Year of the data
        output_folder (str): Path to the output folder to save the parquet files
        test (bool): Whether to load a sample of the data (default: False)

    Returns: None
    """
    input_csv = f"../../input/npi_{year}.csv"
    df = load_csv_to_df(spark, input_csv, test=test)

    save_as_parquet(
        process_indiv_data(df, list(MAIN_TABLE_COLS_MAPPING.keys())),
        output_folder,
        output_dir=f"individuals_{year}",
    )
    save_as_parquet(
        process_taxonomy_data(
            load_csv_to_df(spark, f"../../input/nucc_taxonomy_{year}.csv")
        ),
        output_folder,
        output_dir="taxonomy",
    )
    save_as_parquet(
        process_inst_data(df), output_folder, output_dir="institutions"
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


def merge_deactivations(
    year: int, input_folder: str, test: bool = False
) -> None:
    """Load individuals and deactivations data and merge them by year on NPI.

    Args:
        year (int): Year of the data
        input_folder (str): Path to the input folder containing parquet files
        test (bool): Whether to load a sample of the data

    Returns: None
    """
    individuals_df = load_parquet(
        spark, f"{input_folder}/individuals_{year}", test=test
    ).select("npi", "lastupdate")
    deactivations_df = load_parquet(spark, f"{input_folder}/deactivations")
    combined_df = deactivations_df.join(individuals_df, "npi", "left")
    return combined_df


if __name__ == "__main__":
    year = 2013
    output_folder = "../../output/"
    # process_and_save_data(input_csv, output_folder)
