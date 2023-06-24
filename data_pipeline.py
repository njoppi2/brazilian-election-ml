import pandas as pd
from utils.functions import download_file, extract_zip, get_downloaded_content
import os
from tqdm import tqdm


class DataPipeline:
    def __init__(self):
        self.downloaded_data = get_downloaded_content("data")
        self.UFs = [
            'AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MT', 'MS', 'MG', 'PA',
            'PB', 'PR', 'PE', 'PI', 'RJ', 'RN', 'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO'
        ]
        self.base_url = "https://cdn.tse.jus.br/estatistica/sead/odsele/consulta_cand"
        self.candidates_string = "consulta_cand"
        self.social_media_string = "rede_social_candidato"


    def collect_data(self, year, data_type, redownload=False):
        """
        Downloads and extracts the data from the TSE website.
        """

        download_data = (data_type, str(year))
        # Check if the folder already exists
        if not redownload:
            if download_data in self.downloaded_data:
                print(f"{year} {data_type} files already downloaded. Skipping... ⏩", flush=True)
                return
        
        self.downloaded_data.add((year, data_type))
        extract_folder = f"data/{data_type}/{year}"

        # Create a folder to store the downloaded data, if it doesn't exist
        os.makedirs(f"data/zips/{data_type}", exist_ok=True)

        # Download and extract the data
        if data_type == "candidates":
            print("Downloading candidates files. ⏬", flush=True)
            full_url = f"{self.base_url}/{self.candidates_string}_{year}.zip"
            zip_folder = f"data/zips/{data_type}/{data_type}_{year}.zip"

            if download_file(full_url, zip_folder):
                print("Extracting candidates files. ⏬", flush=True)
                extract_zip(zip_folder, extract_folder)

        elif data_type == "social_media":
            print("Downloading and extracting the social media files. ⏬", flush=True)
            progress_bar = tqdm(self.UFs, unit='UF', ncols=80)
            for UF in progress_bar:
                full_url = f"{self.base_url}/{self.social_media_string}_{year}_{UF}.zip"
                zip_folder = f"data/zips/{data_type}/{data_type}_{year}_{UF}.zip"
                if download_file(full_url, zip_folder, show_progress=False):
                    extract_zip(zip_folder, extract_folder, show_progress=False)
            progress_bar.close()

        else:
            print("Invalid data type. Please provide a valid data type.")
            return


    def transform_data(self):
        """
        Transforms the csv data into a format that is easier to work with.
        """
        
        # Read the CSV file into a Pandas DataFrame
        data = pd.read_csv('data/candidates/2022/consulta_cand_2022_AC.csv', sep=';', encoding='latin1')

        # Print the first few rows of the dataset
        print(data.shape)
        print(data.head())