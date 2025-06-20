import os
import requests
import gzip
import shutil

# Base URL for IMDb datasets
BASE_URL = 'https://datasets.imdbws.com/'

# List of dataset filenames
DATA_FILES = [
    'name.basics.tsv.gz',
    'title.akas.tsv.gz',
    'title.basics.tsv.gz',
    'title.crew.tsv.gz',
    'title.episode.tsv.gz',
    'title.principals.tsv.gz',
    'title.ratings.tsv.gz'
]


# Function to download a file
def download_file(url, dest_path):
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        with open(dest_path, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
        print(f"Downloaded: {dest_path}")
        return True
    except requests.exceptions.RequestException as e:
        print(f"Error downloading {url}: {e}")
        return False


# Function to unzip the files retrieved from API
def unzip_file(file_path, dest_path):
    try:
        with gzip.open(file_path, 'rb') as f_in:
            with open(dest_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        os.remove(file_path)  # Remove zip file
        return True
    except Exception as e:
        print(f'Error unzip: {e}')
        return False


# Function to ingest the data in the Temporal folder with the prefix of API source
def imbd_ingestion(temporal_folder_path):
    # Download each dataset
    for file_name in DATA_FILES:
        file_url = BASE_URL + file_name
        file_path = os.path.join(temporal_folder_path, file_name)
        success_download = download_file(file_url, file_path)
        if not success_download: 
            return False
        success_unzip = unzip_file(file_path, os.path.join(temporal_folder_path, 'imbd_'+file_name.removesuffix('.gz')))
        if not success_unzip:
            return False
    print('All data ingested in the Temporal Folder')
    return True
