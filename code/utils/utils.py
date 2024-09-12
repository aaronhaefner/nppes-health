"""Utility functions for data loading and processing of NPPES using PySpark."""
import os
import requests
import pandas as pd
from lxml import html
from global_variables import MAIN_TABLE_COLS_MAPPING
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, udf, to_date
from pyspark.sql.types import StringType
from typing import List
import warnings


def download_file(url: str, dest_folder: str) -> str:
    """Download a file from the given URL to the destination folder.

    Args:
        url (str): URL to download the file from
        dest_folder (str): Destination folder to save the file

    Returns:
        str: Path to the downloaded file
    """
    local_filename = os.path.join(dest_folder, url.split("/")[-1])
    with requests.get(url, stream=True, timeout=10) as r:
        r.raise_for_status()
        with open(local_filename, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    return local_filename


def download_latest_nppes_data(url: str, dest_folder: str) -> str:
    """Download the latest NPPES data from the given URL.

    Args:
        url (str): URL to download the data from
        dest_folder (str): Destination folder to save the file

    Returns:
        str: Path to the downloaded file
    """
    if not os.path.exists(dest_folder):
        os.makedirs(dest_folder)

    response = requests.get(url, timeout=10)
    response.raise_for_status()

    tree = html.fromstring(response.content)
    link_tag = tree.xpath(
        "//a[contains(@href, 'NPPES_Data_Dissemination')]/@href"
    )

    if not link_tag:
        raise ValueError("Couldn't find the download link on the page")

    print(f"Downloading {link_tag[0]}")
    download_url = requests.compat.urljoin(url, link_tag[0])

    return download_file(download_url, dest_folder)


def load_csv_to_df(spark, csv_file: str, test: bool = False) -> DataFrame:
    """Load NPPES CSV file to a Spark DataFrame.

    Args:
        spark (SparkSession): Spark session
        csv_file (str): Path to the CSV file
        test (bool): Whether to load a sample of the data

    Returns:
        DataFrame: Spark DataFrame containing the loaded data
    """
    df = spark.read.csv(csv_file, header=True, inferSchema=True)
    if test:
        df = df.sample(fraction=0.01, seed=42)

    print(f"Loaded {df.count()} rows from {csv_file}")
    return df


def load_excel_to_spark(
    spark: SparkSession, filename: str, sheet_name: str = None
) -> DataFrame:
    """Load Excel file to a Spark DataFrame.

    Args:
        spark (SparkSession): Spark session
        filename (str): Path to the Excel file
        sheet_name (str, optional): Name of the sheet to load.
        If None, loads the first sheet.

    Returns:
        DataFrame: Raw Spark DataFrame containing the loaded data
    """
    warnings.filterwarnings("ignore", category=UserWarning)
    try:
        df = spark.createDataFrame(
            pd.read_excel(filename, sheet_name=sheet_name)
        )
        print(f"Loaded {df.count()} rows from {filename}")
        return df
    except Exception as e:
        print(f"Error loading Excel file: {str(e)}")
        raise
    finally:
        warnings.resetwarnings()


def load_parquet(
    spark: SparkSession, parquet_path: str, test: bool = False
) -> DataFrame:
    """Load a parquet file into a Spark DataFrame.

    Args:
        spark (SparkSession): Spark session
        parquet_path (str): Path to the parquet file or directory
        test (bool): Whether to load a sample of the data (default: False)

    Returns:
        DataFrame: Spark DataFrame containing the loaded data
    """
    df = spark.read.parquet(parquet_path)
    if test:
        df = df.sample(fraction=0.01, seed=42)
    print(f"Loaded {df.count()} rows from {parquet_path}, test={test}")
    return df


def process_deactivations_data(df: DataFrame) -> DataFrame:
    """Process deactivations data for individual providers.

    Args:
        df (DataFrame): Raw Spark DataFrame containing the deactivations data

    Returns:
        DataFrame: Processed DataFrame with standardized columns
    """
    # Identify the 'npi' column by partial name match
    npi_col = [
        col_name
        for col_name in df.columns
        if col_name.startswith("NPPES Deactivated Records")
    ][0]

    # Identify the 'deactivationdate' column by its original name or position
    deactivation_col = "Unnamed: 1"
    if deactivation_col not in df.columns:
        # Find the next column after 'npi_col' if 'Unnamed: 1' is not present
        npi_col_index = df.columns.index(npi_col)
        if npi_col_index + 1 < len(df.columns):
            deactivation_col = df.columns[npi_col_index + 1]
        else:
            raise ValueError("Could not find the 'deactivationdate' column.")

    df = df.withColumnRenamed(npi_col, "npi").withColumnRenamed(
        deactivation_col, "deactivationdate"
    )
    df = df.filter(col("npi") != "NPI")
    df = df.withColumn(
        "deactivationdate", to_date(col("deactivationdate"), "MM/dd/yyyy")
    )

    return df


def load_and_process_deactivations_data(
    spark: SparkSession, filename: str
) -> DataFrame:
    """Load and process deactivations data for individual providers.

    Args:
        spark (SparkSession): Spark session
        filename (str): Path to the deactivations data file

    Returns:
        DataFrame: Processed DataFrame containing the deactivations data
    """
    processed_df = process_deactivations_data(
        load_excel_to_spark(spark, filename, sheet_name="DeactivatedNPIs")
    )

    print("Deactivations data processed successfully.")
    processed_df.show(5)

    return processed_df


def process_inst_data(df: DataFrame, cols: List[str] = None) -> DataFrame:
    """Process NPPES data for organizations, not individual providers.

    Args:
        df (DataFrame): Input DataFrame containing NPPES data
        cols (List[str]): List of columns to select from the DataFrame

    Returns:
        DataFrame: Processed DataFrame containing the selected columns
    """
    df = df.filter(col("Entity Type Code") == 2)
    if cols is not None:
        df = df.select(
            [col(c).alias(MAIN_TABLE_COLS_MAPPING.get(c, c)) for c in cols]
        )
    df = df.dropDuplicates(subset=["npi"])
    return df


def process_indiv_data(df: DataFrame, cols: List[str]) -> DataFrame:
    """Process NPPES data for individual providers.

    Args:
        df (DataFrame): Input DataFrame containing NPPES data
        cols (List[str]): List of columns to select from the DataFrame

    Returns:
        DataFrame: Processed Spark DataFrame containing the selected cols
    """
    initial_count = df.count()
    df = df.filter(col("Entity Type Code") == 1)
    filtered_count = df.count()
    print(
        f"Dropped {initial_count - filtered_count} \
        rows after filtering by Entity Type Code"
    )

    df = df.select(
        [col(c).alias(MAIN_TABLE_COLS_MAPPING.get(c, c)) for c in cols]
    )
    selected_count = df.count()
    df = df.dropDuplicates(subset=["npi"])
    final_count = df.count()
    print(
        f"Dropped {selected_count - final_count} rows after dropping duplicates"
    )

    return df


def assign_np_type(df: DataFrame) -> DataFrame:
    """Assign nurse practitioner types based on taxonomy codes.

    Args:
        df (DataFrame): Input DataFrame containing the taxonomy codes


    Returns:
        DataFrame: DataFrame with nurse practitioner types assigned
    """
    np_type_mapping = {
        "363L00000X": "Nursing Practice",
        "363LA2100X": "Acute Care",
        "363LF0000X": "Family Practice",
        "363LG0600X": "Geriatric",
        "363LP2300X": "Primary Care",
        "363LP0808X": "Psychiatric",
    }
    np_type_udf = udf(lambda x: np_type_mapping.get(x), StringType())
    df = df.withColumn("np_type", np_type_udf(col("ptaxcode")))

    pediatric_codes = ["363LP0200X", "363LS0200X"]
    maternal_neonatal_codes = ["363LX0001X", "363LN0000X", "363LP1700X"]
    adult_health_codes = [
        "363LA2200X",
        "363LC1500X",
        "363LX0106X",
        "363LW0102X",
    ]
    critical_care_codes = ["363LC0200X", "363LN0005X", "363LP0222X"]

    df = df.withColumn(
        "np_type",
        when(col("ptaxcode").isin(pediatric_codes), "Pediatric")
        .when(
            col("ptaxcode").isin(maternal_neonatal_codes), "Maternal/Neonatal"
        )
        .when(col("ptaxcode").isin(adult_health_codes), "Adult Health")
        .when(col("ptaxcode").isin(critical_care_codes), "Critical Care")
        .otherwise(col("np_type")),
    )

    df = df.withColumn("np", when(col("np_type").isNotNull(), 1).otherwise(0))

    return df


def process_taxonomy_data(taxonomy_df: DataFrame, year: int) -> DataFrame:
    """Process occupational taxonomy data for a given year.

    Args:
        taxonomy_df (DataFrame): Input DataFrame containing the taxonomy data
        year (int): Year of the taxonomy data

    Returns:
        DataFrame: Processed DataFrame containing the taxonomy data
    """
    if year < 2016:
        grouping_var = "Type"
    else:
        grouping_var = "Grouping"

    df = taxonomy_df.select("Code", grouping_var)
    df = df.filter(col("Code") != "Code")  # Remove header row

    df = df.withColumn(
        "physician",
        when(
            col(grouping_var).contains("Allopathic & Osteopathic Physicians"), 1
        ).otherwise(0),
    )
    df = df.withColumn(
        "student", when(col("Code") == "390200000X", 1).otherwise(0)
    )

    df = df.withColumnRenamed("Code", "ptaxcode")
    df = assign_np_type(df)

    return df


def process_medicare_data(df: DataFrame) -> DataFrame:
    """Process Medicare data for individual providers.

    Args:
        df (DataFrame): Input DataFrame containing the Medicare data

    Returns:
        DataFrame: Processed DataFrame containing the Medicare data
    """
    df = df.select([col(c).alias(c.lower()) for c in df.columns])

    df = df.filter(col("rndrng_prvdr_ent_cd") == "I")
    df = df.withColumn(
        "mdcr_provider",
        when(col("rndrng_prvdr_mdcr_prtcptg_ind") == "Y", 1).otherwise(0),
    )

    df = df.dropDuplicates(subset=["rndrng_npi"])
    df = df.select(col("rndrng_npi").alias("npi"), "mdcr_provider")

    return df


def save_as_parquet(df: DataFrame, output_folder: str, output_dir: str) -> None:
    """Save DataFrame as parquet file.

    Args:
        df (DataFrame): Input DataFrame to save
        output_folder (str): Path to the output folder
        output_dir (str): Path to save the DataFrame as parquet file

    Returns: None
    """
    df.write.mode("overwrite").parquet(os.path.join(output_folder, output_dir))
