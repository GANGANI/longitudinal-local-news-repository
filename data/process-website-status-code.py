import json
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

def get_website_status(url):
    """Fetch the HTTP status code of a given URL."""
    try:
        response = requests.get(url, timeout=10)  # Set a timeout for the request
        return response.status_code
    except requests.exceptions.RequestException as e:
        print(f"Error accessing {url}: {e}")
        return None

# Load the JSON file
file_path = 'usa_2016_2024_pu5e.json'  # Replace with your JSON file path
with open(file_path, 'r') as file:
    data = json.load(file)

media = ['newspaper', 'tv', 'radio', 'broadcast']

# Collect all website entries to process in parallel
websites_to_check = []
print("Collecting websites to check...")
for state, content in data.items():
    for news_media in media:
        for entry in content[news_media]:
            if 'website' in entry:
                website_status = entry.get('website_status')  # Safely get the website_status value
                if not website_status or website_status != 200:
                    websites_to_check.append((entry, entry['website']))
print(f"Added website to check: {entry['website']}")

# Function to update the status of a website entry
def update_website_status(entry, website):
    entry['website_status'] = get_website_status(website)
    return entry

# Use ThreadPoolExecutor to process websites in parallel
with ThreadPoolExecutor(max_workers=10) as executor:
    print(f"Starting parallel processing with {len(websites_to_check)} websites...")
    future_to_entry = {executor.submit(update_website_status, entry, website): entry for entry, website in websites_to_check}

    for idx, future in enumerate(as_completed(future_to_entry), start=1):
        print(f"Processing {idx}/{len(future_to_entry)}...")
        entry = future_to_entry[future]
        try:
            future.result()  # Ensure any exceptions are raised here
        except Exception as e:
            print(f"Error updating entry: {e}")

# Save the updated data back to a JSON file
output_file_path = 'website_with_status_code_V3.json'  # Replace with the desired output file path
with open(output_file_path, 'w') as file:
    json.dump(data, file, indent=4)

print(f"All websites processed. Updated JSON data saved to {output_file_path}")
