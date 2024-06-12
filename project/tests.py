import pandas as pd
import sqlite3

def test_etl1():
    conn_gdp = sqlite3.connect('./data/processed/processed_gdp_data.sqlite')
    processed_gdp_data = pd.read_sql_query("SELECT * FROM GDP_data", conn_gdp)
    conn_gdp.close()
    
    print(f"Number of records in processed_gdp_data: {len(processed_gdp_data)}")
    
    # Assuming 62 years of data (1961-2022) + 1 country col
    assert processed_gdp_data.shape[1] == 63  
    
    # Check if 'Country' column exists
    assert 'Country' in processed_gdp_data.columns
    # Check if the years start from 1961 
    assert '1961' in processed_gdp_data.columns
    
    # Checking data types
    assert pd.api.types.is_string_dtype(processed_gdp_data['Country'])
    assert pd.api.types.is_numeric_dtype(processed_gdp_data['1961'])
    
    print("All tests passed for processed_gdp_data")

def test_etl2():
    conn_temp = sqlite3.connect('./data/processed/processed_temp_data.sqlite')
    processed_temp_data = pd.read_sql_query("SELECT * FROM temperature_data", conn_temp)
    conn_temp.close()
    
    print(f"Number of records in processed_temp_data: {len(processed_temp_data)}")
    
    # Assuming 62 years of data (1961-2022) + 1 country col
    assert processed_temp_data.shape[1] == 63
    # Check if 'Country' column exists
    assert 'Country' in processed_temp_data.columns
    # Check if the years start from 1961
    assert '1961' in processed_temp_data.columns
    
    # Checking data types
    assert pd.api.types.is_string_dtype(processed_temp_data['Country'])
    assert pd.api.types.is_numeric_dtype(processed_temp_data['1961'])
    
    print("All tests passed for processed_temp_data")

def main():
    test_etl1()
    test_etl2()

if __name__ == "__main__":
    print("Test cases running")
    main()
