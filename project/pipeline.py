import pandas as pd
import sqlite3
import os
import requests
import zipfile
import io

def download_and_extract_zip(url, extract_to='.'):
    """
    Downloads a ZIP file from the given URL and extracts its contents to the specified directory.
    """
    response = requests.get(url)
    if response.status_code == 200:
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            z.extractall(extract_to)
            extracted_files = z.namelist()
        print(f"ZIP file downloaded and extracted successfully. Files: {extracted_files}")
        return extracted_files
    else:
        print(f"Failed to download ZIP file. Status code: {response.status_code}")
        return None

def download_with_token(url, token, output_path):
    """
    Downloads a file from the given URL using an API token and saves it to the specified output path.
    """
    headers = {
        'Authorization': f'Token {token}'
    }
    response = requests.get(url, headers=headers, verify=False)
    if response.status_code == 200:
        with open(output_path, 'wb') as file:
            file.write(response.content)
        print(f"CSV file downloaded successfully and saved as {output_path}.")
        return True
    else:
        print(f"Failed to download CSV file from source2. Status code: {response.status_code}")
        return False

def etl1(input_filepath):
    """
    Extracts and transforms GDP data from the given CSV file.
    """
    print(f"Reading GDP data from {input_filepath}...")
    if "Metadata" in input_filepath:
        print("Skipping metadata file.")
        return None

    try:
        df = pd.read_csv(input_filepath, sep=',', skiprows=4, engine='c', low_memory=False)
        if df.empty:
            raise ValueError(f"No columns to parse from file {input_filepath}. File may be improperly formatted.")
    except Exception as e:
        raise ValueError(f"Error reading the file {input_filepath}: {e}")

    year_range = range(2002, 2023)
    years = [str(i) for i in year_range]
    cat_cols = ['Country Name']
    cols_to_keep = cat_cols + years

    # Ensure columns exist before filtering
    missing_cols = [col for col in cols_to_keep if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing expected columns: {missing_cols}")

    df = df[cols_to_keep]

    countries_to_study = ['Germany', 'Ireland', 'Poland', 'Greece', 'Italy']
    df = df[df['Country Name'].isin(countries_to_study)]

    return df


def etl2(input_filepath):
    """
    Extracts and transforms temperature data from the given CSV file.
    """
    print(f"Reading temperature data from {input_filepath}...")
    try:
        df = pd.read_csv(input_filepath)
        if df.empty:
            raise ValueError(f"No data found in file {input_filepath}.")
    except pd.errors.ParserError as pe:
        print(f"Error tokenizing data in file {input_filepath}: {pe}")
        return None
    except Exception as e:
        raise ValueError(f"Error reading the file {input_filepath}: {e}")

    year_range = range(2002, 2023)
    years = [str(i) for i in year_range]
    cat_cols = ['Country', 'Level']
    cols_to_keep = cat_cols + years

    # Ensure columns exist before filtering
    missing_cols = [col for col in cols_to_keep if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing expected columns: {missing_cols}")

    df.dropna(inplace=True)
    df = df[cols_to_keep]
    countries_to_study = ['Germany', 'Ireland', 'Poland', 'Greece', 'Italy']
    df = df[df['Country'].isin(countries_to_study) & (df['Level'] == "National")].drop(columns=['Level'])

    return df

def sql_load(df, path, table):
    """
    Loads the given DataFrame into an SQLite database.
    """
    with sqlite3.connect(path) as conn:
        df.to_sql(table, conn, if_exists='replace', index=False)
        print(f"Data loaded into table '{table}' in database '{path}'.")

def main():
    url1 = "http://api.worldbank.org/v2/en/indicator/NY.GDP.MKTP.KD.ZG?downloadformat=csv"
    api_data_url = "https://globaldatalab.org/geos/download/surfacetempyear/"

    # API token for source 2
    api_token = "SdCAagFKCD7PPp5J1-bOIamgQ_j6H3Wze6Tff--yDmY"

    # Directory and filenames for downloaded data
    temp_dir = './temp'
    source2_csv = os.path.join(temp_dir, "GDL-Yearly-Average-Surface-Temperature-(ºC)-data.csv")

    # Ensure temp directory exists
    if not os.path.exists(temp_dir):
        os.makedirs(temp_dir)

    # Download and extract ZIP file for source1
    extracted_files = download_and_extract_zip(url1, extract_to=temp_dir)
    if not extracted_files:
        print("Error: Failed to extract ZIP file for source1.")
        return

    # Identify the correct CSV file in the extracted files
    source1_csv = None
    for file in extracted_files:
        if file.lower().endswith('.csv') and 'API_NY.GDP.MKTP.KD.ZG' in file:
            source1_csv = os.path.join(temp_dir, file)
            break

    if not source1_csv:
        print("Error: No appropriate CSV file found in the ZIP archive.")
        return

    # Download source2 CSV file using API access token
    if not download_with_token(api_data_url, api_token, source2_csv):
        return

    # ETL process
    try:
        gdp_data = etl1(source1_csv)
        temp_data = etl2(source2_csv)
    except Exception as e:
        print(f"Error during ETL process: {e}")
        return

    # Create processed data directory if it doesn't exist
    processed_data = './data/processed'
    if not os.path.exists(processed_data):
        os.makedirs(processed_data)

    # Load data into SQLite database
    sql_load(gdp_data, os.path.join(processed_data, 'processed_gdp_data.sqlite'), 'GDP_data')
    sql_load(temp_data, os.path.join(processed_data, 'processed_temp_data.sqlite'), 'temperature_data')

if __name__ == "__main__":
    main()
