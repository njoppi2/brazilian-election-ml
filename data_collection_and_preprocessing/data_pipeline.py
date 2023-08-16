import pandas as pd
from utils.functions import download_file, extract_zip, get_downloaded_content
import os
from tqdm import tqdm
from sqlalchemy import create_engine
from dotenv import dotenv_values
import io


class DataPipeline:
    def __init__(self):
        self.data_folder = "../data"
        self.downloaded_data = get_downloaded_content(self.data_folder)
        self.UFs = [
            'AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MT', 'MS', 'MG', 'PA',
            'PB', 'PR', 'PE', 'PI', 'RJ', 'RN', 'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO'
        ]
        self.base_url = "https://cdn.tse.jus.br/estatistica/sead/odsele"
        self.mid_url = {
            "candidates": "consulta_cand",
            "social_media": "consulta_cand",
            "voting_section": "votacao_secao"
        }
        self.string = {
            "candidates": "consulta_cand",
            "social_media": "rede_social_candidato",
            "voting_section": "votacao_secao"
        }
    

    def collect_data(self, year, data_type, redownload=False):
        """
        Downloads and extracts the data from the TSE website.
        """

        extract_folder = f"{self.data_folder}/{data_type}/{year}"

        def download_and_extract(UF = None, show_progress = False):
            parsed_UF = UF and f"_{UF}" or ""
            full_url = f"{self.base_url}/{self.mid_url[data_type]}/{self.string[data_type]}_{year}{parsed_UF}.zip"
            zip_folder = f"{self.data_folder}/zips/{data_type}/{data_type}_{year}{parsed_UF}.zip"
            if download_file(full_url, zip_folder, show_progress):
                extract_zip(zip_folder, extract_folder)

        download_data = (data_type, str(year))
        # Check if the folder already exists
        if not redownload:
            if download_data in self.downloaded_data:
                print(f"{year} {data_type} files already downloaded. Skipping... ⏩", flush=True)
                return
        
        # Create a folder to store the downloaded data, if it doesn't exist
        os.makedirs(f"{self.data_folder}/zips/{data_type}", exist_ok=True)

        # Download and extract the data
        print(f"Downloading and extracting the {data_type} files. ⏬", flush=True)

        if data_type == "candidates":
            download_and_extract(show_progress=True)

        elif data_type == "social_media" or data_type == "voting_section":
            progress_bar = tqdm(self.UFs, unit='UF', ncols=80)
            for UF in progress_bar:
                download_and_extract(UF)
            progress_bar.close()

        else:
            print("Invalid data type. Please provide a valid data type.")
            return
        
        self.downloaded_data.add((year, data_type))
        

    def transform_to_sql_tables(self, directories=None, table_prefix="", done=False):
        if done:
            print("Data transformation already done. ⏩", flush=True)
            return
        # Get the directory of the current Python script
        current_directory = os.path.dirname(os.path.abspath(__file__))

        # Construct the path to the .env file
        env_file_path = os.path.join(current_directory, '.env')

        # Load environment variables from .env file
        config = dotenv_values(env_file_path)

        # PostgreSQL connection information
        db_host = config['DB_HOST']
        db_port = config['DB_PORT']
        db_name = config['DB_NAME']
        db_username = config['DB_USER']
        # db_password = config['DB_PASSWORD']

        # Combine all parts of the DB URL
        # db_url = f"postgresql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}"
        db_url = f"postgresql://{db_username}@{db_host}:{db_port}/{db_name}"
        engine = create_engine(db_url)
        
        if directories is None:
            directories = [f"{self.data_folder}/{item[0]}/{item[1]}" for item in self.downloaded_data]

        for csv_directory in directories:
            print(f"Creating tables for .csv's inside {csv_directory}. ⏬", flush=True)
            csv_files = [file_name for file_name in os.listdir(csv_directory) if file_name.endswith(".csv")]
            progress_bar = tqdm(csv_files, desc=f"Processing {csv_directory}", unit="file")

            chunk_size = 10000
            for file_name in progress_bar:
                table_name = table_prefix + os.path.splitext(file_name)[0]
                csv_file_path = os.path.join(csv_directory, file_name)

                if "ibge" in csv_file_path:
                    encoding = "utf-8"
                    delimiter = ","
                    skiprows = 1
                else:
                    encoding = "iso-8859-1"
                    delimiter = ";"
                    skiprows = 0

                # Read CSV file into a pandas DataFrame
                # print("creating dataframe")
                df = pd.read_csv(csv_file_path, encoding=encoding, delimiter=delimiter, skiprows=skiprows)

                # print("creating sql table")
                # df.to_sql(table_name, engine, index=False, if_exists="replace")#, method='multi', chunksize=100)

                # Drop old table and create new empty table
                df.head(0).to_sql(table_name, engine, if_exists='replace',index=False)

                chunk_size = 1000
                for chunk in pd.read_csv(csv_file_path, encoding=encoding, delimiter=delimiter, skiprows=skiprows, chunksize=chunk_size):
                    conn = engine.raw_connection()
                    cur = conn.cursor()
                    output = io.StringIO()
                    chunk.to_csv(output, sep='\t', header=False, index=False)
                    output.seek(0)
                    contents = output.getvalue()
                    cur.copy_from(output, table_name, null="") # null values become ''
                    conn.commit()
                    cur.close()
                    conn.close()
                progress_bar.set_postfix({'Current file': file_name})  # Update progress bar description