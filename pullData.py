import pandas as pd
from datetime import datetime, timedelta
import time
import comtradeapicall
import sqlite3
import os
import numpy as np
# CREATE A NEW FUNCTION THAT WILL BE CREATE DB, THEN WE JUST CHECK IF THE DB EXISTS OR NOT SO WE CAN SKIP THE CREATE TABLE QUERY EVERYTIME
# THEN WE CAN JUST DO THE INSERT OR REPLACE INTO TABLE QUERY

# the goal of this project is to learn/create an end-to-end analytics project: pull data from api, load into db, clean and transform data, then do visualization and analysis

# code below is used to query the data from the API and load it into a CSV. Since I am using free data the max I can query is 500 rows per thing. Otherwise gotta pay $$$$$

def infer_azure_sqlserver_type(dtype):
    """Map pandas data types to Azure SQL Server data types."""
    if pd.api.types.is_integer_dtype(dtype):
        return "INT"  # Standard integer type in SQL Server
    elif pd.api.types.is_float_dtype(dtype):
        return "FLOAT"  # Double precision floating point
    elif pd.api.types.is_bool_dtype(dtype):
        return "BIT"  # SQL Server's boolean type
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return "DATETIME2"  # Modern datetime type in SQL Server
    elif pd.api.types.is_string_dtype(dtype):
        return "NVARCHAR(MAX)"  # Unicode variable-length string with large capacity
    else:
        return "NVARCHAR(MAX)"  # Default to Unicode text for unrecognized types


def load_data_from_api(table_name, conn):
    '''
    Params: None
    Purpose: Pull Canada's export data from https://comtradeplus.un.org/ api and load it into db, also loading into a csv. 
    '''
    apikey = os.getenv('primaryKey')
    if apikey:
        print("WE HAVE A KEY")
    start = datetime(2022, 1, 1)
    end = datetime(2024, 12, 1)
    periods = []
    current = start
    current = start
    # loop populates periods with strings from 2022-2024 in the format of yyyymm -> 2202201 
    while current <= end:
        periods.append(current.strftime('%Y%m'))  # Format as YYYYMM
        current += timedelta(days=31)  # Increment by roughly one month (31 days)
        current = current.replace(day=1)  # Ensure we reset to the first day of the month

    cmdCodes = ['27','26','25']
    # we do the below so we can extract column names in order to do insert/replace in the table
    # this will allow multiple runs of the notebook that does not mess up the DB
    mydf = comtradeapicall.getFinalData(subscription_key = apikey, typeCode='C', freqCode='M', clCode='HS', period='202201',
                                            reporterCode='124', cmdCode='27', flowCode='X', partnerCode=None,
                                            partner2Code=None,
                                            customsCode=None, motCode=None, maxRecords=1, format_output='JSON',
                                            aggregateBy=None, breakdownMode='classic', countOnly=None, includeDesc=True)
    # for each period we call the api and pull all export data, load it into a df which then appends the data to the sqlite database.
    columns = mydf.columns.tolist()
    columns.append('pk')
    # Create the dynamic SQL query for the INSERT OR REPLACE statement
    placeholders = ', '.join(['?'] * len(columns))  # Placeholder for values
    column_names = ', '.join(columns)  # Column names in the query
    column_type_map = {}
    for col in mydf.columns:
        column_type_map[col] = infer_azure_sqlserver_type(mydf[col].dtype)

    # Manually add 'pk' type so we can use it as a primary key
    column_type_map['pk'] = "VARCHAR(255)"

    columns_def = ""
    for col in columns:
        coltype = column_type_map[col]
        columns_def += f"{col} {coltype}, "
    columns_def += "PRIMARY KEY (pk)"

    escaped_columns_def = columns_def.replace("'", "''")

    table_create = f"""
    IF NOT EXISTS (
        SELECT * FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_NAME = '{table_name}'
    )
    BEGIN
        EXEC('CREATE TABLE {table_name} ({escaped_columns_def})')
    END
    """
    cursor = conn.cursor()  
    cursor.execute(table_create)  # Create the table if it doesn't exist
    conn.commit()
    # now we will populate the DB with the data we pulled from the API



    for period in periods:
        for cmdCode in cmdCodes:
            mydf = comtradeapicall.getFinalData(subscription_key = apikey,typeCode='C', freqCode='M', clCode='HS', period=period,
                                            reporterCode='124', cmdCode=cmdCode, flowCode='X', partnerCode=None,
                                            partner2Code=None,
                                            customsCode=None, motCode=None, maxRecords=100000, format_output='JSON',
                                            aggregateBy=None, breakdownMode='classic', countOnly=None, includeDesc=True)
            
            if mydf is None:
                print("No data for this period.")
                continue
            elif not mydf.empty:
                # Add a new column 'pk' to the DataFrame, this column will serve as our primary key so that we can properly insert/replace data.
                # The format is period_reporterCode_flowCode_partnerCode_cmdCode which creates a unique key for each row.
                # This is important because we are using the INSERT OR REPLACE statement in SQLite.  
                mydf['pk'] = (
                    mydf['period'].astype(str) + "_" +
                    mydf['reporterCode'].astype(str) + "_" +
                    mydf['flowCode'].astype(str) + "_" +
                    mydf['partnerCode'].astype(str) + "_" +
                    mydf['cmdCode'].astype(str)
                )
                # we run this query to insert the pulled batch data from mydf into the sqlite db which we defined above
                print('Fetched: ', len(mydf), ' records for period: ', period, ' cmdCode: ', cmdCode)

                update_query = f"""
                UPDATE {table_name}
                SET {', '.join([f"{col} = ?" for col in columns[1:]])}  -- skip pk in update
                WHERE pk = ?
                """

                insert_query = f"""
                INSERT INTO {table_name} ({', '.join(columns)})
                VALUES ({', '.join(['?' for _ in range(len(columns))])})
                """
                for row in mydf.itertuples(index=False):
                    # Convert row values to the correct types (you can adjust the casting as needed)
                    row_values = tuple(
                        None if value == '' or (isinstance(value, float) and np.isnan(value)) else value
                        for value in row
                    )  # Replace empty string with None for SQL NULL handling
                    
                    try:
                        # Attempt to update the row
                        cursor.execute(update_query, (*row_values[1:], row_values[0]))  # Skip pk in update
                        if cursor.rowcount == 0:  # If no rows were updated (i.e., row didn't exist), insert a new record
                            cursor.execute(insert_query, row_values)
                    except Exception as e:
                        # Log the error and the problematic row
                        print(f"Error occurred while processing row: {row_values}")
                        print(f"Error: {e}")
                # Commit the changes
                conn.commit()
                # Append the DataFrame to the database file and append to list to add to csv, warning, reruning this code will duplicate the data                print(f"Retrieved {len(mydf)} records.")
            else:
                print("No data for this commodity.")
            
            time.sleep(1) # do not want to get blocked by API on accident
            
