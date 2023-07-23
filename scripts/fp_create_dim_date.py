import pandas as pd
import holidays
from datetime import datetime

# Generate the time dimension data
start_date = datetime(2020, 1, 1)
end_date = datetime(2039, 12, 31)
date_range = pd.date_range(start_date, end_date)

# Initialize empty lists for each column
dates = []
years = []
months = []
days = []
surrogate_keys = []
quarters = []
week_numbers = []
is_weekends = []
is_public_holidays = []
holiday_descriptions = []
week_numbers_by_month = []

# Get the list of public holidays (US in this example, replace with appropriate country code)
holiday_list = holidays.CountryHoliday('US')

# Loop through each date in the range
for date in date_range:
    surrogate_key = int(date.strftime('%Y%m%d'))
    surrogate_keys.append(surrogate_key)
    dates.append(date.strftime('%Y-%m-%d'))
    years.append(date.year)
    months.append(date.month)
    days.append(date.day)
    quarters.append(date.quarter)
    week_numbers.append(date.isocalendar()[1])
    is_weekends.append(date.weekday() >= 5)
    is_public_holiday = date in holiday_list
    is_public_holidays.append(is_public_holiday)
    holiday_description = holiday_list.get(date) if is_public_holiday else None
    holiday_descriptions.append(holiday_description)

# Create a DataFrame from the lists
df = pd.DataFrame({
    'sk_date': surrogate_keys,
    'date_th': dates,
    'year_th': years,
    'month_th': months,
    'day_th': days,
    'quarter_th': quarters,
    'week_number': week_numbers,
    'is_weekend': is_weekends,
    'is_public_holiday': is_public_holidays,
    'holiday_description': holiday_descriptions
})

# Save the DataFrame as a Parquet file
df.to_parquet('/home/fatir/final-project/dim_date.parquet', index=False)
