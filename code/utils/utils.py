# Description: Utility functions for data loading and processing of NPPES data
import os
import requests
import pandas as pd
from lxml import html
from utils.global_variables import MAIN_TABLE_COLS_MAPPING


def download_file(url: str, dest_folder: str) -> str:
    """
    Download a file from the given URL to the destination folder

    Args:
        url (str): URL to download the file from
        dest_folder (str): Destination folder to save the file

    Returns:
        str: Path to the downloaded file
    """
    local_filename = os.path.join(dest_folder, url.split('/')[-1])
    with requests.get(url, stream=True, timeout=10) as r:
        r.raise_for_status()
        with open(local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    return local_filename


def download_latest_nppes_data(url: str, dest_folder: str) -> str:
    """
    Download the latest NPPES data from the given URL
    
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
    link_tag = tree.xpath("//a[contains(@href, 'NPPES_Data_Dissemination')]/@href")

    if not link_tag:
        raise ValueError("Couldn't find the download link on the page")
    
    print(f"Downloading {link_tag[0]}")
    download_url = requests.compat.urljoin(url, link_tag[0])

    return download_file(download_url, dest_folder)


def load_csv_to_df(csv_file: str, encoding: str='utf-8', test: bool=False ) -> pd.DataFrame:
    """
    Load NPPES CSV file to a pandas DataFrame

    Args:
        csv_file (str): Path to the CSV file
        encoding (str): Encoding of the CSV file
        test (bool): Load only a subset of the data for testing
    
    Returns:
        pd.DataFrame: Loaded DataFrame
    """
    encodings = [encoding, 'latin1', 'cp1252']
    for encoding in encodings:
        try:
            if test:
                nrows = 100000
            else:
                nrows = 1e10
            df = pd.read_csv(
                csv_file,
                nrows=nrows,
                encoding=encoding,
                low_memory=False)
            print(f"Loaded {len(df)} rows from {csv_file}")
            return df
        except UnicodeDecodeError:
            print(f"Failed to load {csv_file} with encoding {encoding}")
    raise UnicodeDecodeError("All encodings failed to decode the CSV file.")


def process_inst_data(df: pd.DataFrame, cols: list=None) -> pd.DataFrame:
    """
    Process NPPES data for organizations/institutions, not individual providers

    Args:
        df (pd.DataFrame): NPPES DataFrame
        cols (list): Columns to keep in the processed DataFrame

    Returns:
        pd.DataFrame: Processed DataFrame
    """
    # Filter out individuals
    df = df[df['Entity Type Code'] == 2]
    if cols is not None:
        df = df.rename(columns=MAIN_TABLE_COLS_MAPPING)
        df = df[cols]
    df.drop_duplicates(subset=['npi'], inplace=True)
    return df


def process_indiv_data(df: pd.DataFrame, cols: list) -> pd.DataFrame:
    """
    Process NPPES data for individual providers, not organizations/institutions

    Args:
        df (pd.DataFrame): NPPES DataFrame
        cols (list): Columns to keep in the processed DataFrame

    Returns:
        pd.DataFrame: Processed DataFrame
    """
    # Filter out non-individuals
    df = df[df['Entity Type Code'] == 1]
    df = df.rename(columns=MAIN_TABLE_COLS_MAPPING)
    df = df[cols]
    df.drop_duplicates(subset=['npi'], inplace=True)
    return df


def assign_np_type(df: pd.DataFrame) -> pd.DataFrame:
    """
    Assign nurse practitioner types based on taxonomy codes
    
    Args:
        df (pd.DataFrame): DataFrame with 'ptaxcode' column
        
    Returns:
        pd.DataFrame: Updated DataFrame with 'np_type' and 'np' columns
    """
    # Define mapping for tax codes to np_type
    np_type_mapping = {
        "363L00000X": "Nursing Practice",
        "363LA2100X": "Acute Care",
        "363LF0000X": "Family Practice",
        "363LG0600X": "Geriatric",
        "363LP2300X": "Primary Care",
        "363LP0808X": "Psychiatric"
    }

    # Apply the mapping
    df['np_type'] = df['ptaxcode'].map(np_type_mapping)

    # Define additional categories using .isin()
    pediatric_codes = ["363LP0200X", "363LS0200X"]
    maternal_neonatal_codes = ["363LX0001X", "363LN0000X", "363LP1700X"]
    adult_health_codes = ["363LA2200X",
                          "363LC1500X",
                          "363LX0106X",
                          "363LW0102X"]
    critical_care_codes = ["363LC0200X", "363LN0005X", "363LP0222X"]

    # Update np_type based on these lists
    df.loc[df['ptaxcode'].isin(pediatric_codes), 'np_type'] = "Pediatric"
    df.loc[df['ptaxcode'].isin(
        maternal_neonatal_codes), 'np_type'] = "Maternal/Neonatal"
    df.loc[df['ptaxcode'].isin(
        adult_health_codes), 'np_type'] = "Adult Health"
    df.loc[df['ptaxcode'].isin(
        critical_care_codes), 'np_type'] = "Critical Care"
    df['np'] = df['np_type'].notnull().astype(int)

    return df


def process_taxonomy_data(taxonomy_df: pd.DataFrame, year: int=2024) -> pd.DataFrame:
    """
    Process occupational taxonomy data for a given year

    Args:
        taxonomy_df (pd.DataFrame): DataFrame with taxonomy data
        year (int): Year of the taxonomy data

    Returns:
        pd.DataFrame: Processed DataFrame with new taxonomy columns
    """
    if year != 2024:
        raise ValueError("Only 2024 taxonomy data is supported.")
    grouping_var = 'Grouping'
    df = taxonomy_df[['Code', grouping_var]].copy()
    df = df.drop(0) # copyright

    # Add physician and student indicators
    df['physician'] = df[grouping_var].apply(
        lambda x: 1 if 'Allopathic & Osteopathic Physicians' in str(x) else 0)
    df['student'] = df['Code'].apply(lambda x: 1 if x == "390200000X" else 0)
    df.rename(columns={'Code': 'ptaxcode'}, inplace=True)

    # Assign taxonomy codes to nurse practitioner types
    df = assign_np_type(df)

    return df


def process_medicare_data(df: pd.DataFrame ) -> pd.DataFrame:
    """
    Pass for now
    """
    df.columns = df.columns.str.lower()

    # Individuals only
    df = df[df['rndrng_prvdr_ent_cd'] == "I"].copy()
    df['mdcr_provider'] = (
        df['rndrng_prvdr_mdcr_prtcptg_ind'] == "Y").astype(int)

    df.drop_duplicates(subset=['rndrng_npi'], inplace=True)
    df = df[['rndrng_npi', 'mdcr_provider']].copy()
    df.rename(columns={'rndrng_npi': 'npi'}, inplace=True)

    return df
