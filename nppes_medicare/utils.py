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


def load_csv_to_dataframe(csv_file, encoding='utf-8'):
    encodings = [encoding, 'latin1', 'cp1252']
    for encoding in encodings:
        try:
            df = pd.read_csv(csv_file, encoding=encoding)
            print(f"Loaded {len(df)} rows from {csv_file} with encoding {encoding}")
            return df
        except UnicodeDecodeError:
            print(f"Failed to load {csv_file} with encoding {encoding}")
    raise UnicodeDecodeError("All encodings failed to decode the CSV file.")

def process_main_data(df, columns_to_keep):
    df = df[df['entity'] == 1]
    df = df[columns_to_keep]
    df.drop_duplicates(subset=['npi'], inplace=True)
    return df

def process_taxonomy_data(df, taxonomy_df):
    if 'ptaxcode1' in df.columns:
        temp_df = df[['npi', 'ptaxcode1']].dropna()
        temp_df.rename(columns={'ptaxcode1': 'ptaxcode'}, inplace=True)
        temp_df = temp_df.merge(taxonomy_df[['Code', 'Type']], left_on='ptaxcode', right_on='Code', how='left')
        temp_df['physician'] = temp_df['Type'].apply(lambda x: 1 if 'Allopathic & Osteopathic Physicians' in str(x) else 0)
        temp_df.drop(columns=['Code', 'Type'], inplace=True)
        return temp_df.drop_duplicates()
    return pd.DataFrame(columns=['npi', 'ptaxcode', 'physician'])

def process_medicare_data(df):
    df = df[['rndrng_npi', 'rndrng_prvdr_ent_cd', 'tot_benes', 'rndrng_prvdr_mdcr_prtcptg_ind']]
    df = df[df['rndrng_prvdr_ent_cd'] == "I"]
    df['mdcr_provider'] = df['rndrng_prvdr_mdcr_prtcptg_ind'] == "Y"
    df['mdcr_provider'] = df['mdcr_provider'].astype(int)
    df.drop_duplicates(subset=['rndrng_npi'], inplace=True)
    return df
