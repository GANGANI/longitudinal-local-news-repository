import json
from collections import Counter

# Load the JSON file
file_path = 'updated_data_v2.json'  # Replace with your JSON file path
with open(file_path, 'r') as file:
    data = json.load(file)

# Flatten the list of status codes
status_codes = []

for state, content in data.items():
    if 'newspaper' in content:
        for entry in content['newspaper']:
            if 'website_status' in entry:
                status_codes.append(entry['website_status'])

# Count occurrences of each status code
status_count = Counter(status_codes)

# Save the counts to a text file
output_file_path = 'status_counts.txt'
with open(output_file_path, 'w') as file:
    for status, count in status_count.items():
        file.write(f"Status Code {status}: {count}\n")

print(f"Status counts saved to {output_file_path}")
