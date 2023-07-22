import pandas as pd

def create_time_dimension(start_date, end_date):
    # Buat rentang tanggal dengan Pandas
    date_range = pd.date_range(start=start_date, end=end_date, freq='H')

    # Buat DataFrame untuk dimensi waktu
    time_dimension = pd.DataFrame(date_range, columns=['Datetime'])

    # Ekstrak informasi waktu
    time_dimension['hour'] = time_dimension['Datetime'].dt.hour
    time_dimension['am_pm'] = time_dimension['Datetime'].dt.strftime('%p')
    time_dimension['is_rushhour'] = time_dimension['Datetime'].dt.hour.apply(lambda x: 'Yes' if (x >= 7 and x < 10) or (x >= 17 and x < 19) else 'No')
    
    return time_dimension[['hour', 'am_pm', 'is_rushhour']]

start_date = '2023-07-01 00:00:00'
end_date = '2023-07-01 23:59:59'
df_time = create_time_dimension(start_date, end_date)
# print(time_dimension)

# Save the DataFrame as a Parquet file
df_time.to_parquet('/home/fatir/final-project/dim_time.parquet', index=False)