# global_variables.py

# Define the columns to keep for the main table
MAIN_TABLE_COLS_MAPPING = {
    "NPI": "npi",
    "Entity Type Code": "entity",
    "Replacement NPI": "replacement_npi",
    "Employer Identification Number (EIN)": "ein",
    "Provider Organization Name (Legal Business Name)": "porgname",
    "Provider Credential Text": "pcredential",
    "Provider Business Practice Location Address State Name": "plocstatename",
    "Provider Business Practice Location Address Postal Code": "ploczip",
    "Provider Enumeration Date": "penumdate",
    "Last Update Date": "lastupdate",
    "NPI Deactivation Reason Code": "npideactreason",
    "NPI Deactivation Date": "npideactdate",
    "NPI Reactivation Date": "npireactdate",
    "Provider Gender Code": "pgender",
    "Healthcare Provider Taxonomy Code_1": "ptaxcode"
}

COLS_TO_KEEP = [
    'npi', 'entity', 'replacement_npi', 'ein', 'porgname', 'pcredential',
    'plocstatename', 'ploczip',
    'penumdate', 'lastupdate', 'npideactreason', 'npideactdate', 
    'npireactdate', 'pgender', 'ptaxcode'
]

