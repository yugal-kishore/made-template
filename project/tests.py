import pytest
import pandas as pd
import sqlite3

conn =sqlite3.connect(r'./data/processed.sqlite')



def test_etl1():
    processed_gdp_data = pd.read_sql_query("select * from processed_gdp_data",conn)

    assert len(processed_gdp_data.values)[1] == 63              # checking if data for all the years are present
    assert len(processed_gdp_data.values)[1] == 5               # checking if all the countries we plan to study are present

    assert processed_gdp_data.values[1] == 'Country'
    assert processed_gdp_data.values[2] == 1961
    assert pd.api.types.is_string_dtype(processed_gdp_data['Country'])
    assert pd.api.types.is_numeric_dtype(processed_gdp_data['1961'])

    print("All tests passed for processed_gdp_data")



def test_etl2():
    processed_temp_data = pd.read_sql_query("select * from processed_temp_data",conn)

    assert len(processed_temp_data.values)[1] == 63             # checking if data for all the years are present
    assert len(processed_temp_data.values)[1] == 5              # checking if all the countries we plan to study are present
    assert processed_temp_data.values[1] == 'Country'
    assert processed_temp_data.values[2] == 1961
    assert pd.api.types.is_string_dtype(processed_temp_data['Country'])
    assert pd.api.types.is_numeric_dtype(processed_temp_data['1961'])


    print("All tests passed for processed_temp_data")


def main():
    test_etl1()
    test_etl2()

if __name__ == "__main__":
    print("test cases running ")
    #main()