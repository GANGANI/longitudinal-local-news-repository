import json
import subprocess

# Define the function to run the archivenow command and get the archived URL
def get_archived_url(website_url):
    try:
        # Execute the 'archivenow' command and capture the output
        result = subprocess.run(['archivenow', '--ia', website_url], capture_output=True, text=True, check=True)
        print (result)
        # Return the archived URL from the output
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        print(f"Error archiving {website_url}: {e}")
        return None

# Load the JSON data from a file
def process_websites(input_file, output_file):
    with open(input_file, 'r') as file:
        data = json.load(file)

    # Iterate over the objects in the JSON data
    for state, newspapers in data.items():
        for newspaper in newspapers.get('newspaper', []):
            if newspaper.get('website_status') == 200:
                website_url = newspaper.get('website')
                if website_url:
                    print(f"Processing website: {website_url}")
                    archived_url = get_archived_url(website_url)
                    if archived_url:
                        newspaper['archived_url'] = archived_url

    # Write the updated data back to the file
    with open(output_file, 'w') as file:
        json.dump(data, file, indent=4)

# Example usage
input_file = 'website_with_status_code_V3.json'  # Input file with the data
output_file = 'websites_with_archives.json'  # Output file where updated data will be stored
process_websites(input_file, output_file)
