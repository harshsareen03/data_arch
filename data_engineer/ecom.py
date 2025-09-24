import pandas as pd

import requests

import sqlite3
# from sqlalchemy import create_engine

csv_data=pd.read_csv('store_sales.csv')

api_data=pd.DataFrame(requests.get('https://jsonplaceholder.typicode.com/users').json())

combined_data=pd.concat([csv_data,api_data])

daily_sales=combined_data.groupby(['date'])['sales_amount'].sum().reset_index()

enigne=sqlite3.connect('sqlite:///ecom.db')
cursor =enigne.cursor()


cursor.commit()
cursor.close()

csv_data.to_sql('store_sales',enigne,if_exists='replace',index=False)