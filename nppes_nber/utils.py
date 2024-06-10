# utils.py

import requests
import os

def _download_file(url, dest_folder):
    year = url.split('/')[-2]
    year_folder = os.path.join(dest_folder, year)
    
    if not os.path.exists(year_folder):
        os.makedirs(year_folder)
    
    filename = os.path.join(year_folder, url.split('/')[-1])
    
    response = requests.get(url)
    response.raise_for_status()
    
    with open(filename, 'wb') as file:
        file.write(response.content)
    
    print(f"Downloaded {filename}")
    return filename
