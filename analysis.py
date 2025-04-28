import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import sqlite3
from IPython.display import display # allows display of pandas df as a table

plt.style.use('fivethirtyeight') # style of plots
pd.set_option('display.max_columns', 500) # sets max viewable cols to 500 in a df if the df is really wide

conn = sqlite3.connect('canada_exports_2022_2024.db')

query = 'SELECT * FROM canada_export_data'

df = pd.read_sql_query(query, conn)

display(df)
