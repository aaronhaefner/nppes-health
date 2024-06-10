# nppes-medicare

## Overview
The `nppes-medicare` package assembles the National Plan and Provider Enumeration System (NPPES) historical monthly files available from the National Bureau of Economic Research (NBER) into a database of active Medicare providers in the U.S. across time.
I greatly appreciate that the NBER hosts the NPPES data files and makes them available for researchers or otherwise.

The development of this package is ongoing and should not be assumed usable in its current state until noted here.
The main utility of this package is that its goal is to provide a more nuanced view of active Medicare providers in the U.S. based on multiple reliable sources.
Other packages maintain superior data integrity, such as by keeping institutions in the data, but the focus of this package is to provide a database with a transparent construction process from multiple reliably connected data sources to generate an account of healthcare providers (individuals only).

## Features
- **Download NPPES Data**: Private methods for downloading NPPES data files from the NBER website.
- **Database Creation**: Methods for creating and managing a local SQLite database.
- **Data Insertion**: Functions for inserting CSV data into the database.
- **Querying Data**: Methods for querying the database to explore and analyze data.
- **Auxiliary Table Creation**: Supports the creation of an auxiliary taxonomy table and the identification of physicians based on taxonomy codes.

## Installation
To install the required dependencies, run:

```sh
poetry install
```

## Usage

### Setup
The package is designed such that the downloading methods are not accessible programmatically by end-users due to speed limitations. The public-facing code focuses on database operations. Developers can use the provided scripts to download and store data.

### Downloading and Storing Data
Developers should use the `main.py` script to download and store data. This script is not intended for end-users.

#### Example
```sh
python main.py
```

### Loading and Processing Data
Users can interact with the local SQLite database to load and process the data. The `database.py` script provides the necessary functionality.

### Querying Data
Users can interact with the local SQLite database to query and explore the data using methods provided in the `database.py` script.

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
    └── 2007/               # Subfolder for 2007
    └── taxonomy/           # Subfolder for taxonomy data
```

## Contributing
To contribute to this project, please fork the repository, create a new branch, and submit a pull request. For major changes, please open an issue first to discuss what you would like to change.

## License
This project is licensed under the MIT License.
