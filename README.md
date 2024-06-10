# nppes-nber

## Overview
The `nppes-nber` package provides an interface for working with the National Plan and Provider Enumeration System (NPPES) historical monthly files available from the National Bureau of Economic Research (NBER). This package allows users to query the data and store it in a local SQLite database. The data downloading functionality is encapsulated to ensure efficient use and to prevent speed limitations imposed by the NBER website.

## Features
- **Download NPPES Data**: Private methods for downloading NPPES data files from the NBER website.
- **Database Creation**: Methods for creating and managing a local SQLite database.
- **Data Insertion**: Functions for inserting CSV data into the database.
- **Querying Data**: Methods for querying the database to explore and analyze data.

## Installation
To install the required dependencies, run:

```sh
poetry install

