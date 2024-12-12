import json
import requests

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

n = 0
media = ['newspaper', 'tv', 'radio', 'broadcast']
# Traverse the nested JSON structure
for state, content in data.items():
    for news_media in media:
        for entry in content[news_media]:
            n += 1
            # if 'website' in entry:
            #     website_status = entry.get('website_status')  # Safely get the website_status value
            #     if not website_status or website_status != 200: 
            #         print(f"\nStatus code: {website_status}")
            #         website = entry['website']
            #         entry['website_status'] = get_website_status(website)
            #         print(f"Updated status code: {entry['website_status']}\n")

print(f"Number of website: {n}")
# Save the updated data back to a JSON file
output_file_path = 'website_with_status_code_V2.json'  # Replace with the desired output file path
with open(output_file_path, 'w') as file:
    json.dump(data, file, indent=4)

print(f"Updated JSON data saved to {output_file_path}")
