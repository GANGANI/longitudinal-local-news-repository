import json
import requests

def get_website_status(url):
    """Fetch the HTTP status code of a given URL."""
    try:
        response = requests.get(url, timeout=5)  # Set a timeout for the request
        return response.status_code
    except requests.exceptions.RequestException as e:
        print(f"Error accessing {url}: {e}")
        return None

# Load the JSON file
file_path = 'updated_data.json'  # Replace with your JSON file path
with open(file_path, 'r') as file:
    data = json.load(file)

# Traverse the nested JSON structure
for state, content in data.items():
    if 'newspaper' in content:  # Check if "newspaper" key exists
        for entry in content['newspaper']:
            if 'website' in entry:  # Check if "website" key exists
                website = entry['website']
                entry['website_status'] = get_website_status(website)

# Save the updated data back to a JSON file
output_file_path = 'updated_data_v2.json'  # Replace with the desired output file path
with open(output_file_path, 'w') as file:
    json.dump(data, file, indent=4)

print(f"Updated JSON data saved to {output_file_path}")
