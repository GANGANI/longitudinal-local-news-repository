import gzip
import json
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

def find_feed_url(url):
    try:
        response = requests.get(url, timeout=5)
        response.raise_for_status()  # Check if request was successful
        soup = BeautifulSoup(response.content, "html.parser")
        
        # List of possible MIME types for feeds
        feed_types = ["application/rss+xml", "application/atom+xml", "text/xml", "application/xml"]
        
        # Check for link tags with RSS or Atom feed types
        for feed_type in feed_types:
            link = soup.find("link", type=feed_type)
            if link and link.get("href"):
                feed_link = link["href"]
                print(f"\n************\nfeed_type: {feed_type} & link:{feed_link}\n***********\n")
                return feed_link
    except requests.exceptions.ConnectionError:
        print(f"Connection error for URL: {url}")
    except requests.exceptions.Timeout:
        print(f"Request timed out for URL: {url}")
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
    
    return None  # Return None if no feed URL found or if an error occurs


# Load the gzipped JSON file
file_path = "updated_usa_2016_2024_v2.json.gz"
with gzip.open(file_path, 'rt', encoding='utf-8') as f:
    json_data = json.load(f)

# Prepare a list of tasks for parallel processing
tasks = []

# Create a function to process each media object and retrieve the RSS feed URL
def process_media_object(media_object):
    website = media_object.get("website")
    if website:
        if "rss" not in media_object:
            rss_feed_url = find_feed_url(website)
            media_object["rss"] = []
            if rss_feed_url:
                media_object["rss"].append(rss_feed_url)
        elif len(media_object["rss"]) == 0:
            rss_feed_url = find_feed_url(website)
            if rss_feed_url:
                media_object["rss"].append(rss_feed_url)
    return media_object

# Use ThreadPoolExecutor to process media objects in parallel
with ThreadPoolExecutor(max_workers=10) as executor:
    futures = []
    for state, media_types in json_data.items():
        for media_type, media_objects in media_types.items():
            for media_object in media_objects:
                # Submit each media object for processing
                futures.append(executor.submit(process_media_object, media_object))
                
    # Collect results with tqdm progress tracking
    for future in tqdm(as_completed(futures), total=len(futures), desc="Processing media objects"):
        future.result()  # Ensure all futures complete

# Save the updated JSON data
with open('updated_usa_2016_2024_v4.json', 'w', encoding='utf-8') as outfile:
    json.dump(json_data, outfile, ensure_ascii=False, indent=4)

