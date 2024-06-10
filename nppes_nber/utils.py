# utils.py

import requests
import os

def _download_file(url, dest_folder):
    if not os.path.exists(dest_folder):
        os.makedirs(dest_folder)
    
    filename = os.path.join(dest_folder, url.split('/')[-1])
    
    response = requests.get(url)
    response.raise_for_status()
    
    with open(filename, 'wb') as file:
        file.write(response.content)
    
    print(f"Downloaded {filename}")
    return filename
