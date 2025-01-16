import requests
import json

# Function to get the status code of a URL
def get_status_code(url):
    try:
        response = requests.get(url, timeout=10, allow_redirects=True)
        return response.status_code
    except requests.exceptions.RequestException as e:
        print(f"Error fetching {url}: {e}")
        return None  # In case of error, return None

# Read JSON data from the file
input_file = 'data.json'  # Replace with the actual file name
output_file = 'updated_data.json'  # File where updated data will be saved

with open(input_file, 'r') as f:
    data = json.load(f)

# Loop through the data to update the website_status
for state, media_types in data.items():
    for media_type, media_list in media_types.items():
        for media in media_list:
            website_url = media.get('website')
            if website_url:
                # Get the status code for the website
                status_code = get_status_code(website_url)
                # Add the status code to the object
                media['website_status'] = status_code

# Save the updated data back to the file
with open(output_file, 'w') as f:
    json.dump(data, f, indent=4)

print(f"Updated data has been saved to {output_file}")
