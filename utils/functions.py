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
    
def get_downloaded_content(folder_path):
    folder_tuples = set()

    for root, dirs, files in os.walk(folder_path):
        # Skip the top-level folder
        if root != folder_path:
            # Extract sub-folder and sub-sub-folder
            if len(dirs) > 0:
                sub_folder = os.path.basename(root)
                if sub_folder != "zips":
                    for sub_dir in dirs:
                        folder_tuples.add((sub_folder, sub_dir))
                        
    return folder_tuples
