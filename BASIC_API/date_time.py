from datetime import datetime, time, timedelta
from turtle import pd

# Get the current date and time
current_datetime = datetime.now()

# Define the morning and cutoff times
morning_start_time = time(0, 0)   # 12:00 AM
morning_end_time = time(9, 0)     # 9:00 AM
evening_start_time = time(10, 0)  # 10:00 AM
evening_end_time = time(23, 0)    # 11:00 PM

# Check the current time
current_time = current_datetime.time()

if morning_start_time <= current_time <= morning_end_time:
    # If it's between 12 AM to 9 AM, use sysdate-1
    formatted_date = "SYSDATE-1"
elif evening_start_time <= current_time <= evening_end_time:
    # If it's between 10 AM to 11 PM, use sysdate
    formatted_date = "SYSDATE"
else:
    # For any other time, handle accordingly (e.g., set a default date)
    formatted_date = "SYSDATE" 


print(formatted_date)

#time_in_header=pd.Timestamp.now().strftime('%Y-%m-%d')
#print(time_in_header)
