from datetime import datetime, timedelta

class time_formats():

    def eia_formated_time(self):
        # Get the current datetime
        current_datetime = datetime.now()
        print(current_datetime)
        # Calculate the time zone offset (assuming it's +02:00)
        time_zone_offset = timedelta(hours=2)

        # Apply the time zone offset to the current datetime
        adjusted_datetime = current_datetime - time_zone_offset

        # Format the adjusted datetime object as a string
        formatted_datetime = adjusted_datetime.strftime("%Y-%m-%dT%H:%M:%S.%f+02:00")

        # Print the formatted datetime string
        return formatted_datetime
    
