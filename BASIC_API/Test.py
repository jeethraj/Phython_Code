data = [('611000112',), ('822000024',), ('611000112',)]

# Initialize an empty list to store mobile numbers
mobile_numbers = []


# Iterate through the tuples and extract the mobile numbers
for item in data:
    mobile_numbers.append(item[0])

print(mobile_numbers)


from datetime import datetime

# Get the current date and time
current_datetime = datetime.now()

# Print the current date and time
print(current_datetime)
