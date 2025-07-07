import pandas as pd
from datetime import datetime, timedelta
import time
import comtradeapicall
import os
import numpy as np
from dateutil.relativedelta import relativedelta
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
    start = datetime(2024, 2, 2)
    end = datetime(2024, 12, 1)
    periods = []
    current = start
    joined_period = ''
    count = 0

    while current <= end:
        if count <= 11:
            joined_period += current.strftime('%Y%m') + ','
            count += 1
        else:
            count = 0
            joined_period = joined_period.rstrip(',')
            periods.append(joined_period)
            joined_period = ''

        current += relativedelta(months=1)  # Reliable month increment

    if joined_period:
        joined_period = joined_period.rstrip(',')
        periods.append(joined_period)

    # missing periods for 33, 66, 99 = 201601, 201702, 201703, 201704, 201705, 201706, 201707, 201708, 201709, 201710, 201711, 201712, 202101, 202202, 202401, 202412 

    cmdCodes = [
    "01",  # Live animals
    "02",  # Meat and edible meat offal
    "03",  # Fish and crustaceans, molluscs and other aquatic invertebrates
    "04",  # Dairy produce; birds' eggs; natural honey; edible products of animal origin
    "05",  # Products of animal origin, not elsewhere specified or included
    "06",  # Live trees and other plants; bulbs, roots and the like; cut flowers and ornamental foliage
    "07",  # Edible vegetables and certain roots and tubers
    "08",  # Edible fruit and nuts; peel of citrus fruit or melons
    "09",  # Coffee, tea, maté and spices
    "10",  # Cereals
    "11",  # Products of the milling industry; malt; starches; inulin; wheat gluten
    "12",  # Oil seeds and oleaginous fruits; miscellaneous grains, seeds and fruit; industrial or medicinal plants; straw and fodder
    "13",  # Lac; gums, resins and other vegetable saps and extracts
    "14",  # Vegetable plaiting materials; vegetable products not elsewhere specified or included
    "15",  # Animal or vegetable fats and oils and their cleavage products; prepared edible fats; animal or vegetable waxes
    "16",  # Preparations of meat, fish or crustaceans, molluscs or other aquatic invertebrates
    "17",  # Sugars and sugar confectionery
    "18",  # Cocoa and cocoa preparations
    "19",  # Preparations of cereals, flour, starch or milk; pastrycooks' products
    "20",  # Preparations of vegetables, fruit, nuts or other parts of plants
    "21",  # Miscellaneous edible preparations
    "22",  # Beverages, spirits and vinegar
    "23",  # Residues and waste from the food industries; prepared animal fodder
    "24",  # Tobacco and manufactured tobacco substitutes
    "25",  # Salt; sulphur; earths and stone; plastering materials, lime and cement
    "26",  # Ores, slag and ash
    "27",  # Mineral fuels, mineral oils and products of their distillation; bituminous substances; mineral waxes
    "28",  # Inorganic chemicals; organic or inorganic compounds of precious metals, of rare-earth metals, of radioactive elements or of isotopes
    "29",  # Organic chemicals
    "30",  # Pharmaceutical products
    "31",  # Fertilisers
    "32",  # Tanning or dyeing extracts; tannins and their derivatives; dyes, pigments and other colouring matter; paints and varnishes; putty and other mastics; inks
    "33",  # Essential oils and resinoids; perfumery, cosmetic or toilet preparations
    "34",  # Soap, organic surface-active agents, washing preparations, lubricating preparations, artificial waxes, prepared waxes, polishing or scouring preparations, candles and similar articles, modelling pastes, "dental waxes" and dental preparations with a basis of plaster
    "35",  # Albuminoidal substances; modified starches; glues; enzymes
    "36",  # Explosives; pyrotechnic products; matches; pyrophoric alloys; certain combustible preparations
    "37",  # Photographic or cinematographic goods
    "38",  # Chemical products n.e.s.
    "39",  # Plastics and articles thereof
    "40",  # Rubber and articles thereof
    "41",  # Raw hides and skins (other than furskins) and leather
    "42",  # Articles of leather; saddlery and harness; travel goods, handbags and similar containers; articles of animal gut (other than silk-worm gut)
    "43",  # Furskins and artificial fur; manufactures thereof
    "44",  # Wood and articles of wood; wood charcoal
    "45",  # Cork and articles of cork
    "46",  # Manufactures of straw, of esparto or of other plaiting materials; basketware and wickerwork
    "47",  # Pulp of wood or of other fibrous cellulosic material; recovered (waste and scrap) paper or paperboard
    "48",  # Paper and paperboard; articles of paper pulp, of paper or of paperboard
    "49",  # Printed books, newspapers, pictures and other products of the printing industry; manuscripts, typescripts and plans
    "50",  # Silk
    "51",  # Wool, fine or coarse animal hair; horsehair yarn and woven fabric
    "52",  # Cotton
    "53",  # Other vegetable textile fibres; paper yarn and woven fabrics of paper yarn
    "54",  # Man-made filaments; strip and the like of man-made textile materials
    "55",  # Man-made staple fibres
    "56",  # Wadding, felt and nonwovens; special yarns; twine, cordage, ropes and cables and articles thereof
    "57",  # Carpets and other textile floor coverings
    "58",  # Special woven fabrics; tufted textile fabrics; lace; tapestries; trimmings; embroidery
    "59",  # Impregnated, coated, covered or laminated textile fabrics; textile articles of a kind suitable for industrial use
    "60",  # Knitted or crocheted fabrics
    "61",  # Articles of apparel and clothing accessories, knitted or crocheted
    "62",  # Articles of apparel and clothing accessories, not knitted or crocheted
    "63",  # Other made up textile articles; sets; worn clothing and worn textile articles; rags
    "64",  # Footwear, gaiters and the like; parts of such articles
    "65",  # Headgear and parts thereof
    "66",  # Umbrellas, sun umbrellas, walking-sticks, seat-sticks, whips, riding-crops and parts thereof
    "67",  # Prepared feathers and down and articles made of feathers or of down; artificial flowers; articles of human hair
    "68",  # Articles of stone, plaster, cement, asbestos, mica or similar materials
    "69",  # Ceramic products
    "70",  # Glass and glassware
    "71",  # Natural or cultured pearls, precious or semi-precious stones, precious metals, metals clad with precious metal, and articles thereof; imitation jewellery; coin
    "72",  # Iron and steel
    "73",  # Articles of iron or steel
    "74",  # Copper and articles thereof
    "75",  # Nickel and articles thereof
    "76",  # Aluminium and articles thereof
    "78",  # Lead and articles thereof
    "79",  # Zinc and articles thereof
    "80",  # Tin and articles thereof
    "81",  # Other base metals; cermets; articles thereof
    "82",  # Tools, implements, cutlery, spoons and forks, of base metal; parts thereof of base metal
    "83",  # Miscellaneous articles of base metal
    "84",  # Nuclear reactors, boilers, machinery and mechanical appliances; parts thereof
    "85",  # Electrical machinery and equipment and parts thereof; sound recorders and reproducers, television image and sound recorders and reproducers, and parts
    "86",  # Railway or tramway locomotives, rolling-stock and parts thereof; railway or tramway track fixtures and fittings and parts thereof; mechanical (including electro-mechanical) traffic signalling equipment of all kinds
    "87",  # Vehicles other than railway or tramway rolling-stock, and parts and accessories thereof
    "88",  # Aircraft, spacecraft, and parts thereof
    "89",  # Ships, boats and floating structures
    "90",  # Optical, photographic, cinematographic, measuring, checking, precision, medical or surgical instruments and apparatus; parts and accessories thereof
    "91",  # Clocks and watches and parts thereof
    "92",  # Musical instruments; parts and accessories of such articles
    "93",  # Arms and ammunition; parts and accessories thereof
    "94",  # Furniture; bedding, mattresses, mattress supports, cushions and similar stuffed furnishings; lamps and lighting fittings, not elsewhere specified or included; illuminated signs, illuminated name-plates and the like; prefabricated buildings
    "95",  # Toys, games and sports requisites; parts and accessories thereof
    "96",  # Miscellaneous manufactured articles
    "97",  # Works of art, collectors' pieces and antiques
    "98",  # Special classification provisions (generally for temporary use, etc.)
    "99",  # Temporary use or other special provisions
    ]

    concatenatedCodes = []
    for i in range(0, len(cmdCodes), 33):
        chunk = cmdCodes[i:i+33]  # get up to 20 codes
        cmdCodeString = ",".join(chunk)
        concatenatedCodes.append(cmdCodeString.rstrip(','))

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
        for cmdCodeStrings in concatenatedCodes:
            cursor = conn.cursor()  # apparently re iniiting it makes the connection not expire 
            mydf = comtradeapicall.getFinalData(subscription_key = apikey,typeCode='C', freqCode='M', clCode='HS', period=period,
                                                reporterCode='124', cmdCode=cmdCodeStrings, flowCode='X', partnerCode=None, partner2Code=None, 
                                                customsCode=None, motCode=None, maxRecords=100000, format_output='JSON',
                                                aggregateBy=None, breakdownMode='classic', countOnly=None, includeDesc=True)
        
            if mydf is None or len(mydf) == 0:
                print("Last batch of data for this period.")

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
                mydf = mydf.replace({np.nan: None})     

                # we run this query to insert the pulled batch data from mydf into the sqlite db which we defined above
                print('Fetched: ', len(mydf), ' records for period: ', period, ' cmdCode: ', cmdCodeStrings)

                # Create the queries
                update_query = f"""
                UPDATE {table_name}
                SET {', '.join([f"{col} = ?" for col in columns[1:]])}  -- skip pk in update
                WHERE pk = ?
                """

                insert_query = f"""
                INSERT INTO {table_name} ({', '.join(columns)})
                VALUES ({', '.join(['?' for _ in range(len(columns))])})
                """
                
                # For MSSQL MERGE (upsert) - much more efficient than UPDATE/INSERT pattern
                merge_query = f"""
                MERGE {table_name} AS target
                USING (VALUES ({', '.join(['?' for _ in range(len(columns))])})) AS source ({', '.join(columns)})
                ON target.pk = source.pk
                WHEN MATCHED THEN
                    UPDATE SET {', '.join([f"{col} = source.{col}" for col in columns[1:]])}
                WHEN NOT MATCHED THEN
                    INSERT ({', '.join(columns)}) VALUES ({', '.join([f"source.{col}" for col in columns])});
                """
                
                try:
                    # Clean the data first
                    mydf_clean = mydf.replace('', None)  # Replace empty strings with None
                    mydf_clean = mydf_clean.where(pd.notnull(mydf_clean), None)  # Replace NaN with None
                    
                    # Convert to list of tuples for bulk insert
                    rows_data = [tuple(row) for row in mydf_clean.itertuples(index=False)]
                    
                    # Process in batches of 1000 records
                    batch_size = 1000
                    total_records = len(rows_data)
                    
                    for i in range(0, total_records, batch_size):
                        batch = rows_data[i:i + batch_size]
                        batch_num = (i // batch_size) + 1
                        
                        try:
                            # Use MERGE for efficient upsert in MSSQL (faster than update/insert pattern)
                            cursor.executemany(merge_query, batch)
                            conn.commit()
                            
                            print(f"Batch {batch_num}: Merged {len(batch)} records for period: {period}, cmdCodes: {cmdCodeStrings}")
                            
                        except Exception as batch_error:
                            print(f"Batch {batch_num} merge failed: {batch_error}")
                            cursor.close()
                except Exception as e:
                    print(f"Error processing data for period {period} and cmdCode {cmdCodeStrings}: {e}")
                finally:
                    cursor.close()

                    print(f"Total processed: {total_records} records in {(total_records + batch_size - 1) // batch_size} batches")
            else:
                print("No data for this commodity.")
            
            time.sleep(1) # do not want to get blocked by API on accident