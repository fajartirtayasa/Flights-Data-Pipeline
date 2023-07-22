import requests
import json
import pandas as pd
from datetime import datetime, timedelta
import pyarrow as pa
import pyarrow.parquet as pq

# Set current time variabel
current_date = datetime.today()

# Calculate the date 2 days before the current date
begin_date = current_date - timedelta(days=2)
begin_timestamp = int(begin_date.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())

# Calculate the date 1 day before the current date
end_date = current_date - timedelta(days=1)
end_timestamp = int(end_date.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())

airport = 'KATL' # Hartsfield-Jackson Atlanta International Airport

# Define the url to get data
url1 = f'https://opensky-network.org/api/flights/departure?airport={airport}&begin={begin_timestamp}&end={end_timestamp}'
url2 = f'https://opensky-network.org/api/flights/arrival?airport={airport}&begin={begin_timestamp}&end={end_timestamp}'

print('==> Start to get data from the API <==')

response1 = requests.get(url1, auth=('fajartirtayasa', 'OpenSky2023'))
response2 = requests.get(url2, auth=('fajartirtayasa', 'OpenSky2023'))

try:
    print(f'Status Code to get dep data: {response1.status_code}')
    print(f'Status Code to get arr data: {response2.status_code}')
    data_unclean1 = json.loads(response1.content)
    data_unclean2 = json.loads(response2.content)
    print(f'Number of rows in dep data: {len(data_unclean1)}')
    print(f'Number of rows in arr data: {len(data_unclean2)}')
    
    df1 = pd.DataFrame(data_unclean1)
    df2 = pd.DataFrame(data_unclean2)
    
    # Define the schema for the Parquet file
    schema1 = pa.Schema.from_pandas(df1)
    schema2 = pa.Schema.from_pandas(df2)
    
    # Create a PyArrow table from the DataFrame
    table1 = pa.Table.from_pandas(df1, schema=schema1)
    table2 = pa.Table.from_pandas(df2, schema=schema2)

    # Convert the Unix timestamp to a datetime object
    date = datetime.fromtimestamp(begin_timestamp)

    # Format the datetime object as a string
    begin_date_string = date.strftime("%d_%B_%Y")
    
    # Define the Parquet file path
    parquet_file_path1 = f'/home/fatir/datalake-flight/flight_dep_data/flight_dep_data-{begin_date_string}.parquet'
    parquet_file_path2 = f'/home/fatir/datalake-flight/flight_arr_data/flight_arr_data-{begin_date_string}.parquet'
    
    # Write the table to a Parquet file
    pq.write_table(table1, parquet_file_path1)
    pq.write_table(table2, parquet_file_path2)
    
    print(f"Dep Data saved as Parquet file: {parquet_file_path1}")
    print(f"Arr Data saved as Parquet file: {parquet_file_path2}")
    
except:
    print('Something went wrong!')
    raise
