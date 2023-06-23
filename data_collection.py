import os
import requests
import zipfile
from tqdm import tqdm

def get_candidates(year, force=False):
    """
    Downloads and extracts the candidates file for the given year.
    """
    
    # Check if the folder already exists
    if not force:
        if os.path.exists(f"./data/{year}"):
            print(f"{year} candidates file already exists. Skipping... ⏩", flush=True)
            return

    # Define the URLs and paths
    url = f"https://cdn.tse.jus.br/estatistica/sead/odsele/consulta_cand/consulta_cand_{year}.zip"
    zip_folder = f"./data/zips/consulta_cand_{year}.zip"
    extract_folder = f"./data/{year}"

    # Create the 'data' folder if it doesn't exist
    os.makedirs("data/zips", exist_ok=True)

    # Download the file
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        total_size = int(response.headers.get('content-length', 0))
        block_size = 1024  # 1 KB
        progress_bar = tqdm(
            total=total_size,
            unit='B',
            unit_scale=True,
            unit_divisor=1024,
            ncols=80,
            bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt}'
        )
        with open(zip_folder, 'wb') as file:
            for data in response.iter_content(block_size):
                progress_bar.update(len(data))
                file.write(data)
        progress_bar.close()
        print(f"{year} candidates file downloaded successfully. ✅", flush=True)
    else:
        print(f"{year} candidates file download failed. ❌", flush=True)

    # Extract the contents of the zip file
    try:
        with zipfile.ZipFile(zip_folder, 'r') as zip_ref:
            file_list = zip_ref.namelist()
            progress_bar = tqdm(file_list, unit='file', ncols=80)
            for file in progress_bar:
                zip_ref.extract(file, extract_folder)
        progress_bar.close()
        print(f"{year} candidates file extracted successfully. ✅", flush=True)
    except zipfile.BadZipFile:
        print(f"Failed to extract the {year} candidates file. It might be a corrupt zip file. ❌", flush=True)
