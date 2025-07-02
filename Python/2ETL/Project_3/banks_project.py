# Code for ETL operations on World's Largest Banks data

# Importing the required libraries
import requests
import sqlite3
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime

# Define log file path at the beginning
log_file = "code_log.txt"

# Suppress generated warnings
def warn(*args, **kwargs):
    pass
import warnings
warnings.warn = warn
warnings.filterwarnings('ignore')

# Task 1: Logging function
def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    timestamp_format = "%Y-%h-%d-%H:%M:%S" # Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now() # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open(log_file,"a") as f:
        f.write(timestamp + " : " + message + "\n")

# Task 2: Extraction of data
def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
    log_progress("Starting data extraction")
    
    page = requests.get(url).text
    soup = BeautifulSoup(page, "html.parser")

    df = pd.DataFrame(columns=table_attribs)

    tables = soup.find_all("tbody")
    rows = tables[0].find_all("tr")

    for row in rows:
        col = row.find_all("td")
        if len(col) != 0:
            data_dict = {"Name": col[1].find_all("a")[1]["title"],
                         "MC_USD_Billion": float(col[2].contents[0][:-1])}
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df, df1], ignore_index=True)

    log_progress("Data extraction complete")
    return df

# Define the URL and table attributes we're looking for
url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ['Name', 'MC_USD_Billion']

# Call the extract function and display the result
df_extracted = extract(url, table_attribs)
print(df_extracted)

# Task 3: Transformation of data
def transform(df, csv_path):
    '''Transforms Market Cap to GBP, EUR, and INR billions using exchange rates.'''
    try:
        log_progress("Starting data transformation")
        
        # Read and process exchange rates
        exchange_rate = pd.read_csv(csv_path).set_index("Currency")["Rate"]
        
        # Calculate all conversions
        currencies = ['GBP', 'EUR', 'INR']
        for currency in currencies:
            df[f"MC_{currency}_Billion"] = (df["MC_USD_Billion"] * exchange_rate[currency]).round(2)
        
        log_progress("Data transformation complete")
        return df
        
    except Exception as e:
        log_progress(f"Error during transformation: {str(e)}")
        raise

# Apply transformation
try:
    df_transformed = transform(df_extracted, 'exchange_rate.csv')
    print(df_transformed)
    print("\n5th bank EUR value:", df_transformed['MC_EUR_Billion'].iloc[4])
except Exception as e:
    print(f"Error during transformation: {e}")

# Task 4: Loading to CSV
def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    
    try:
        log_progress("Saving data to CSV file")
        df.to_csv(output_path, index=False)
        log_progress(f"Data saved to CSV file: {output_path}")
    except Exception as e:
        log_progress(f"Error while saving to CSV: {str(e)}")
        raise

# Define the output path for the CSV file
output_csv_path = 'Largest_banks_data.csv'

# Call the function to save the transformed data
load_to_csv(df_transformed, output_csv_path)

# Task 5: Loading to Database
def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database table.
    Args:
        df: Transformed DataFrame
        sql_connection: SQLite connection object
        table_name: Name of the table to create
    Returns nothing.
    '''
    try:
        log_progress(f"Starting to load data to {table_name} table")
        
        # Load data to database
        df.to_sql(name=table_name, 
                 con=sql_connection, 
                 if_exists='replace', 
                 index=False,
                 dtype={
                     'Name': 'TEXT',
                     'MC_USD_Billion': 'REAL',
                     'MC_GBP_Billion': 'REAL',
                     'MC_EUR_Billion': 'REAL',
                     'MC_INR_Billion': 'REAL'
                 })
        
        log_progress(f"Data successfully loaded to {table_name} table")
        
        # Verify the data was loaded
        query = f"SELECT COUNT(*) AS row_count FROM {table_name}"
        result = pd.read_sql(query, sql_connection)
        log_progress(f"Verified {result.iloc[0,0]} rows loaded to {table_name}")
        
    except Exception as e:
        log_progress(f"Error loading data to database: {str(e)}")
        raise

# Main execution block for database loading
try:
    log_progress("Initiating SQLite database connection")
    
    # Connect to SQLite database (creates if doesn't exist)
    sql_connection = sqlite3.connect('Banks.db')
    log_progress("Successfully connected to Banks.db")
    
    # Load the transformed data
    load_to_db(df_transformed, sql_connection, 'Largest_banks')
    
    # Sample query to verify data
    sample_query = "SELECT * FROM Largest_banks LIMIT 5"
    sample_data = pd.read_sql(sample_query, sql_connection)
    print("\nSample data from database:")
    print(sample_data)
    
    log_progress("Database operations completed successfully")
    
except Exception as e:
    log_progress(f"Database operation failed: {str(e)}")
    print(f"Error: {str(e)}")
    
finally:
    # Ensure connection is closed
    if 'sql_connection' in locals():
        sql_connection.close()
        log_progress("Database connection closed")
        print("Database connection closed")


# Task 6: Function to Run queries on Database
def run_queries(query, sql_connection):
    '''Executes SQL queries and prints results
    Args:
        query: SQL query string
        sql_connection: SQLite database connection object
    '''
    try:
        log_progress(f"Executing query: {query}")
        
        # Execute query and fetch results
        result = pd.read_sql(query, sql_connection)
        
        # Print query and results
        print(f"\nQuery: {query}")
        print("Result:")
        print(result)
        
        log_progress("Query executed successfully")
        return result
        
    except Exception as e:
        log_progress(f"Query failed: {str(e)}")
        print(f"Error executing query: {e}")
        raise

# Re-establish database connection for queries
try:
    log_progress("Reconnecting to database for queries")
    sql_connection = sqlite3.connect('Banks.db')
    
    # Query 1: Print entire table
    query1 = "SELECT * FROM Largest_banks"
    run_queries(query1, sql_connection)
    
    # Query 2: Average market cap in GBP
    query2 = "SELECT AVG(MC_GBP_Billion) AS Avg_MC_GBP_Billion FROM Largest_banks"
    run_queries(query2, sql_connection)
    
    # Query 3: Names of top 5 banks
    query3 = "SELECT Name FROM Largest_banks LIMIT 5"
    run_queries(query3, sql_connection)
    
    log_progress("All queries executed")
    
except Exception as e:
    log_progress(f"Database query error: {str(e)}")
    
finally:
    if 'sql_connection' in locals():
        sql_connection.close()
        log_progress("Database connection closed")

# Task 7: Verify log entries
with open(log_file, "r") as log:
    LogContent = log.read()
    print(LogContent)