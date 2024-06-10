# nppes-nber

## Overview
The `nppes-nber` package provides an interface for working with the National Plan and Provider Enumeration System (NPPES) historical monthly files available from the National Bureau of Economic Research (NBER).
This package allows users to query the data and store it in a local SQLite database. The data downloading functionality is encapsulated to ensure efficient use and to prevent speed limitations imposed by the NBER website.

## Features
- **Download NPPES Data**: Private methods for downloading NPPES data files from the NBER website.
- **Database Creation**: Methods for creating and managing a local SQLite database.
- **Data Insertion**: Functions for inserting CSV data into the database.
- **Querying Data**: Methods for querying the database to explore and analyze data.

## Installation
To install the required dependencies, run:

```sh
poetry install
```

## Usage

### Setup
The package is designed such that the downloading methods are not accessible programmatically by end-users due to speed limitations.
The public-facing code focuses on database operations.
Developers can use the provided scripts to download and store data.

### Downloading and Storing Data
Developers should use the `main.py` script to download and store data.
This script is not intended for end-users.

#### Example
```sh
python main.py
```

### Querying Data
Users can interact with the local SQLite database to query and explore the data.

#### Example in IPython
```ipython
import pandas as pd
from database import NppesDatabase

# Specify the database file
db_file = "nppes.db"
db = NppesDatabase(db_file)

# Create a connection to the database
conn = db._create_connection()

# Query and view the results
df = pd.read_sql_query("SELECT * FROM nppes LIMIT 10", conn)
print(df)
db.close_connection()
```

## Project Structure
```plaintext
nppes-nber/
│
├── utils.py                # Contains utility functions
├── database.py             # Contains database operations
├── main.py                 # Entry point for script
├── README.md               # Project documentation
├── pyproject.toml          # Poetry configuration file
└── nppes_data/             # Directory for downloaded CSV files
```

## Contributing
To contribute to this project, please fork the repository, create a new branch, and submit a pull request.
For major changes, please open an issue first to discuss what you would like to change.

## License
This project is licensed under the MIT License.