from global_variables import MAIN_TABLE_COLS_MAPPING
import pandas as pd
import requests
import os

def download_file(url, dest_folder):
    if not os.path.exists(dest_folder):
        os.makedirs(dest_folder)
    local_filename = os.path.join(dest_folder, url.split('/')[-1])
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    return local_filename


def load_csv_to_dataframe(csv_file, encoding='utf-8', test=False):
    encodings = [encoding, 'latin1', 'cp1252']
    for encoding in encodings:
        try:
            if test:
                nrows = 100000
            else:
                nrows = 1e10
            df = pd.read_csv(csv_file, nrows=nrows, encoding=encoding, low_memory=False)
            print(f"Loaded {len(df)} rows from {csv_file} with encoding {encoding}")
            return df
        except UnicodeDecodeError:
            print(f"Failed to load {csv_file} with encoding {encoding}")
    raise UnicodeDecodeError("All encodings failed to decode the CSV file.")


def process_main_data(df, columns_to_keep):
    df = df[df['Entity Type Code'] == 1]
    # map columns with MAIN_TABLE_COLS_MAPPING
    df = df.rename(columns=MAIN_TABLE_COLS_MAPPING)
    df = df[columns_to_keep]
    df.drop_duplicates(subset=['npi'], inplace=True)
    return df


def assign_np_type(df):
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
    adult_health_codes = ["363LA2200X", "363LC1500X", "363LX0106X", "363LW0102X"]
    critical_care_codes = ["363LC0200X", "363LN0005X", "363LP0222X"]

    # Update np_type based on these lists
    df.loc[df['ptaxcode'].isin(pediatric_codes), 'np_type'] = "Pediatric"
    df.loc[df['ptaxcode'].isin(maternal_neonatal_codes), 'np_type'] = "Maternal/Neonatal"
    df.loc[df['ptaxcode'].isin(adult_health_codes), 'np_type'] = "Adult Health"
    df.loc[df['ptaxcode'].isin(critical_care_codes), 'np_type'] = "Critical Care"
    df['np'] = df['np_type'].notnull().astype(int)

    return df


def process_taxonomy_data(taxonomy_df):
    df = taxonomy_df[['Code', 'Type']].copy()
    df = df.drop(0) # copyright

    # Add physician and student indicators
    df['physician'] = df['Type'].apply(lambda x: 1 if 'Allopathic & Osteopathic Physicians' in str(x) else 0)
    df['student'] = df['Code'].apply(lambda x: 1 if x == "390200000X" else 0)
    df.rename(columns={'Code': 'ptaxcode'}, inplace=True)

    # Assign taxonomy codes to nurse practitioner types
    df = assign_np_type(df)

    return df


def process_medicare_data(df):
    df.columns = df.columns.str.lower()
    
    # Individuals only
    df = df[df['rndrng_prvdr_ent_cd'] == "I"].copy()
    df['mdcr_provider'] = (df['rndrng_prvdr_mdcr_prtcptg_ind'] == "Y").astype(int)
    
    df.drop_duplicates(subset=['rndrng_npi'], inplace=True)
    df = df[['rndrng_npi', 'mdcr_provider']].copy()
    df.rename(columns={'rndrng_npi': 'npi'}, inplace=True)
    
    return df
