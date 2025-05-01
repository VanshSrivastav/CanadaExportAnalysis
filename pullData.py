import pandas as pd
from datetime import datetime, timedelta
import time
import comtradeapicall
import sqlite3

# CREATE A NEW FUNCTION THAT WILL BE CREATE DB, THEN WE JUST CHECK IF THE DB EXISTS OR NOT SO WE CAN SKIP THE CREATE TABLE QUERY EVERYTIME
# THEN WE CAN JUST DO THE INSERT OR REPLACE INTO TABLE QUERY

# the goal of this project is to learn/create an end-to-end analytics project: pull data from api, load into db, clean and transform data, then do visualization and analysis

# code below is used to query the data from the API and load it into a CSV. Since I am using free data the max I can query is 500 rows per thing. Otherwise gotta pay $$$$$

def infer_sqlite_type(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return "INTEGER"
    elif pd.api.types.is_float_dtype(dtype):
        return "REAL"
    elif pd.api.types.is_bool_dtype(dtype):
        return "INTEGER"  # SQLite uses 0/1 for booleans
    else:
        return "TEXT"

def load_data_from_api(table_name, conn):
    '''
    Params: None
    Purpose: Pull Canada's export data from https://comtradeplus.un.org/ api and load it into db, also loading into a csv. 
    '''
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
    mydf = comtradeapicall.previewFinalData(typeCode='C', freqCode='M', clCode='HS', period='202201',
                                            reporterCode='124', cmdCode='27', flowCode='X', partnerCode=None,
                                            partner2Code=None,
                                            customsCode=None, motCode=None, maxRecords=500, format_output='JSON',
                                            aggregateBy=None, breakdownMode='classic', countOnly=None, includeDesc=True)
    # for each period we call the api and pull all export data, load it into a df which then appends the data to the sqlite database.
    columns = mydf.columns.tolist()
    columns.append('pk')
    # Create the dynamic SQL query for the INSERT OR REPLACE statement
    placeholders = ', '.join(['?'] * len(columns))  # Placeholder for values
    column_names = ', '.join(columns)  # Column names in the query
    column_type_map = {}
    for col in mydf.columns:
        column_type_map[col] = infer_sqlite_type(mydf[col].dtype)

    # Manually add 'pk' type so we can use it as a primary key
    column_type_map['pk'] = "TEXT"

    table_create = f"CREATE TABLE IF NOT EXISTS {table_name} ("
    for col in columns:
        coltype = column_type_map[col]
        table_create += f"{col} {coltype}, "

    table_create += "PRIMARY KEY (pk));"    
    conn.execute(table_create)  # Create the table if it doesn't exist
    conn.commit()
    # now we will populate the DB with the data we pulled from the API
    for period in periods:
        for cmdCode in cmdCodes:
            mydf = comtradeapicall.previewFinalData(typeCode='C', freqCode='M', clCode='HS', period=period,
                                            reporterCode='124', cmdCode=cmdCode, flowCode='X', partnerCode=None,
                                            partner2Code=None,
                                            customsCode=None, motCode=None, maxRecords=500, format_output='JSON',
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
                query = f"INSERT OR REPLACE INTO {table_name} ({column_names}) VALUES ({placeholders})"
                data = [tuple(row) for row in mydf.values]
                conn.executemany(query, data)
                conn.commit()
                # Append the DataFrame to the database file and append to list to add to csv, warning, reruning this code will duplicate the data                print(f"Retrieved {len(mydf)} records.")
            else:
                print("No data for this commodity.")
            
            time.sleep(1) # do not want to get blocked by API on accident
