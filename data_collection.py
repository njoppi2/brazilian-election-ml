import os
import requests
import zipfile
from tqdm import tqdm


def download_file(url, zip_folder, show_progress=True):
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        total_size = int(response.headers.get('content-length', 0))
        block_size = 1024  # 1 KB

        if show_progress:
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
                if show_progress:
                    progress_bar.update(len(data))
                file.write(data)

        if show_progress:
            progress_bar.close()

        return True
    else:
        print("File download failed. ❌", flush=True)
        return False


def extract_zip(zip_path, extract_folder, show_progress=True):
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            file_list = zip_ref.namelist()

            if show_progress:
                progress_bar = tqdm(file_list, unit='file', ncols=80)
            else:
                progress_bar = file_list

            for file in progress_bar:
                zip_ref.extract(file, extract_folder)

            if show_progress:
                progress_bar.close()

        return True
    except zipfile.BadZipFile:
        print("Failed to extract the zip file. It might be a corrupt zip file. ❌", flush=True)
        return False




def collect_data(year, type, redownload=False):

    # Check if the folder already exists
    if not redownload:
        if os.path.exists(f"data/{type}/{year}"):
            print(f"{year} {type} files already exists. Skipping... ⏩", flush=True)
            return

    # Define the URLs and paths
    UFs = [
        'AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MT', 'MS', 'MG', 'PA',
        'PB', 'PR', 'PE', 'PI', 'RJ', 'RN', 'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO'
    ]   
    base_url = "https://cdn.tse.jus.br/estatistica/sead/odsele/consulta_cand"
    extract_folder = f"data/{type}/{year}"

    # Create a folder to store the downloaded data, if it doesn't exist
    os.makedirs(f"data/zips/{type}", exist_ok=True)

    if type == "candidates":
        print("Downloading candidates files. ⏬", flush=True)
        full_url = f"{base_url}/consulta_cand_{year}.zip"
        zip_folder = f"data/zips/{type}/{type}_{year}.zip"

        if download_file(full_url, zip_folder):
            print("Extracting candidates files. ⏬", flush=True)
            extract_zip(zip_folder, extract_folder)

    elif type == "social_media":
        print("Downloading and extracting the social media files. ⏬", flush=True)
        progress_bar = tqdm(UFs, unit='UF', ncols=80)
        for UF in progress_bar:
            full_url = f"{base_url}/rede_social_candidato_{year}_{UF}.zip"
            zip_folder = f"data/zips/{type}/{type}_{year}_{UF}.zip"
            if download_file(full_url, zip_folder, show_progress=False):
                extract_zip(zip_folder, extract_folder, show_progress=False)
        progress_bar.close()

    else:
        print("Invalid data type. Please provide a valid data type.")
        return

def get_candidates(year, redownload=False):
    """
    Downloads and extracts the candidates file for the given year.
    """
    collect_data(year, type="candidates", redownload=redownload)

def get_social_media(year, redownload=False):
    """
    Downloads and extracts the social media file for the given year.
    """
    collect_data(year, type="social_media", redownload=redownload)