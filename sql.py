import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import dotenv_values
from tqdm import tqdm  # Import tqdm

def create_sql_tables_from_csv(directories, db_url, table_prefix=""):
    engine = create_engine(db_url)
    
    for csv_directory in directories:
        print(f"Creating tables for .csv's indide {csv_directory}. ⏬", flush=True)
        csv_files = [file_name for file_name in os.listdir(csv_directory) if file_name.endswith(".csv")]
        progress_bar = tqdm(csv_files, desc=f"Processing {csv_directory}", unit="file")

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
            df = pd.read_csv(csv_file_path, encoding=encoding, delimiter=delimiter, skiprows=skiprows)

            df.to_sql(table_name, engine, index=False, if_exists="replace")
            progress_bar.set_postfix({'Current file': file_name})  # Update progress bar description

# Load the environment variables from the .env file
config = dotenv_values('.env')

# PostgreSQL connection information
db_host = config['DB_HOST']
db_port = config['DB_PORT']
db_name = config['DB_NAME']
db_username = config['DB_USER']
# db_password = config['DB_PASSWORD']

# Combine all parts of the DB URL
# db_url = f"postgresql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}"
db_url = f"postgresql://{db_username}@{db_host}:{db_port}/{db_name}"

# List of directories containing .csv files
# csv_directories = ["data/candidates/2022", "data/social_media/2022"]
csv_directories = ["data/ibge"]

# Call the function to create SQL tables
create_sql_tables_from_csv(csv_directories, db_url)