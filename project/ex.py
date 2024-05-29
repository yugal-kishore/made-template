import pandas as pd
import sqlite3
import os


def etl1(input_url):
    try:
        df = pd.read_csv(
            input_url,
            sep=',',
            skiprows=4,
            engine='c',
            low_memory=False
        )

        year_range = range(2002, 2023)
        years = [str(i) for i in year_range]
        cat_cols = ['Country Name']
        cols_to_keep = cat_cols + years
        df = df[cols_to_keep]

        countries_to_study = ['Germany', 'Ireland', 'Poland', 'Greece', 'Italy']
        df = df[df['Country Name'].isin(countries_to_study)]

        return df
    except pd.errors.ParserError as e:
        print("Error parsing CSV:", e)
        return None


def etl2(input_url):
    try:
        df = pd.read_csv(input_url)
        df.dropna(inplace=True)
        year_range = range(2002, 2023)
        years = [str(i) for i in year_range]
        cat_cols = ['Country', 'Level']
        cols_to_keep = cat_cols + years
        df = df[cols_to_keep]
        countries_to_study = ['Germany', 'Ireland', 'Poland', 'Greece', 'Italy']
        df = df[df['Country'].isin(countries_to_study) & (df['Level'] == "National")].drop(columns=['Level'])

        return df
    except pd.errors.ParserError as e:
        print("Error parsing CSV:", e)
        return None


def sql_load(df, path, table):
    if df is not None:
        with sqlite3.connect(path) as conn:
            df.to_sql(table, conn, if_exists='replace', index=False)


def main():
    source1 = "./gdp.csv"
    source2 = "./GDL-Yearly-Average-Surface-Temperature-(ºC)-data (1).csv"

    if not os.path.isfile(source1):
        print(f"Error: The file {source1} does not exist.")
        return
    if not os.path.isfile(source2):
        print(f"Error: The file {source2} does not exist.")
        return

    gdp_data = etl1(source1)
    temp_data = etl2(source2)
    processed_data = './data/processed'
    if not os.path.exists(processed_data):
        os.makedirs(processed_data)
    sql_load(gdp_data, os.path.join(processed_data, 'processed_gdp_data.sqlite'), 'GDP_data')
    sql_load(temp_data, os.path.join(processed_data, 'processed_temp_data.sqlite'), 'Temperature_data')


if __name__ == "__main__":
    main()
