import os
import pandas as pd
import kaggle
import sqlite3



def setup_kaggle():
    """
    Set up Kaggle API credentials from environment variables.
    """
    os.environ['KAGGLE_USERNAME'] = os.getenv('KAGGLE_USERNAME')
    os.environ['KAGGLE_KEY'] = os.getenv('KAGGLE_KEY')

def download_and_extract_dataset(dataset_id, data_dir):
    """
    download from Kaggle and extract to destination.
    """
    try:
        kaggle.api.dataset_download_files(dataset_id, path=data_dir, unzip=True)
        print(f'Dataset {dataset_id} downloaded and extracted to {data_dir}')
    except Exception as e:
        print(f"Error downloading and extracting dataset {dataset_id}: {e}")
        return False
    return True

def etl1(data_dir):
    """
    GDP data processing.
    """
    try:
        csv_filename = os.path.join(data_dir, 'GDP.csv')
        df = pd.read_csv(csv_filename)
        year_range = range(1961, 2023)
        years = [str(i) for i in year_range]
        cat_cols = ['Country']
        cols_to_keep = cat_cols + years

        # Ensure columns exist before filtering
        missing_cols = [col for col in cols_to_keep if col not in df.columns]
        if missing_cols:
            raise ValueError(f"Missing expected columns: {missing_cols}")
        
        df = df[cols_to_keep]
        countries_to_study = ['Germany', 'Ireland', 'France', 'Greece', 'Italy']
        df = df[df['Country'].isin(countries_to_study)]
        return df

    except Exception as e:
        print(f"Error processing GDP data: {e}")
        return None

def rename_columns(df):
    if df is None:
        raise ValueError("DataFrame is None, cannot rename columns.")
    new_column_names = {col: int(col[1:]) for col in df.columns if col.startswith('F')}
    df.rename(columns=new_column_names, inplace=True)


def etl2(data_dir):
    file_path = os.path.join(data_dir, 'climate_change_indicators.csv')
    
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")
    
    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        raise ValueError(f"Error reading CSV file {file_path}: {e}")

    if df is None or df.empty:
        raise ValueError("DataFrame is empty or None after reading the CSV file.")
    
    print("Columns in DataFrame before renaming:", df.columns.tolist())
    rename_columns(df)
    print("Columns in DataFrame after renaming:", df.columns.tolist())

    cat_cols = ['Country']
    years = [year for year in range(1961, 2023) if year in df.columns]
    cols_to_keep = cat_cols + years
    missing_cols = [col for col in range(1961, 2023) if col not in df.columns]
    
    if missing_cols:
        raise KeyError(f"Columns {missing_cols} not found in DataFrame")
    df = df[cols_to_keep]

    countries_to_study = ['Germany', 'Ireland', 'France', 'Greece', 'Italy']
    df = df[df['Country'].isin(countries_to_study)]
    return df

def sql_load(df, path, table):
    """
    Loads the given DataFrame into an SQLite database.
    """
    with sqlite3.connect(path) as conn:
        df.to_sql(table, conn, if_exists='replace', index=False)
        print(f"Data loaded into table '{table}' in database '{path}'.")

def main():
    setup_kaggle()

    data_dir = './data'
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)

    dataset1_id = 'annafabris/world-gdp-by-country-1960-2022'
    dataset2_id = 'tarunrm09/climate-change-indicators'

    if download_and_extract_dataset(dataset1_id, data_dir):
        df1 = etl1(data_dir)
        if df1 is not None:
            print("GDP data Processed")

    if download_and_extract_dataset(dataset2_id, data_dir):
        df2 = etl2(data_dir)
        if df2 is not None:
            print("Temperature data processed")
            
    processed_data = './data/processed'
    if not os.path.exists(processed_data):
        os.makedirs(processed_data)

    sql_load(df1, os.path.join(processed_data, 'processed_gdp_data.sqlite'), 'GDP_data')
    sql_load(df2, os.path.join(processed_data, 'processed_temp_data.sqlite'), 'temperature_data')


if __name__ == "__main__":
    main()
