import gzip
import json
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin

def get_robots_txt_url(url):
    """Get the robots.txt URL based on the website URL."""
    return urljoin(url, "robots.txt")

def get_sitemap_from_robots(url):
    """Retrieve sitemap URLs from robots.txt, if available."""
    robots_url = get_robots_txt_url(url)
    try:
        response = requests.get(robots_url, timeout=5)
        response.raise_for_status()
        
        # Look for Sitemap directives in robots.txt
        sitemap_urls = []
        for line in response.text.splitlines():
            if line.lower().startswith("sitemap:"):
                sitemap_url = line.split(":", 1)[1].strip()
                sitemap_urls.append(sitemap_url)
        return sitemap_urls
    except requests.RequestException:
        return None

def get_sitemap_url(url):
    """Attempt to find a sitemap URL based on the website URL."""
    # Common sitemap URL patterns
    sitemap_urls = [urljoin(url, "sitemap.xml"), urljoin(url, "sitemap_index.xml")]
    for sitemap_url in sitemap_urls:
        try:
            response = requests.get(sitemap_url, timeout=5)
            if response.status_code == 200:
                return sitemap_url
        except requests.RequestException:
            continue
    return None

def get_rss_from_sitemap(sitemap_url):
    """Extract RSS feed links from the sitemap."""
    try:
        response = requests.get(sitemap_url, timeout=5)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, "xml")  # Parse as XML for sitemaps
        rss_links = []
        
        # Look for any link that could point to an RSS feed in the sitemap
        for loc in soup.find_all("loc"):
            href = loc.text
            if "rss" in href or "feed" in href:
                rss_links.append(href)
                
        return rss_links if rss_links else None
    except requests.RequestException:
        return None
    
def get_rss_feed_url(url):
    """Fetch the RSS feed URL from the given website URL."""
    try:
        response = requests.get(url, timeout=5)
        response.raise_for_status()  # Check for HTTP errors
        soup = BeautifulSoup(response.content, "html.parser")
        # Try to find the direct RSS link
        rss_link = soup.find("link", type="application/rss+xml")
        if rss_link and rss_link.get("href"):
            return rss_link["href"]  # Return the direct RSS feed URL
    except requests.RequestException:
        pass  # Handle exceptions and continue
    return None

# Load the gzipped JSON file
file_path = "updated_usa_2016_2024_v4.json.gz"
with gzip.open(file_path, 'rt', encoding='utf-8') as f:
    json_data = json.load(f)

# Prepare a list of tasks for parallel processing
tasks = []

# Create a function to process each media object and retrieve the RSS feed URL
def process_media_object(media_object):
    website = media_object.get("website")
    rss_feed_url = None
    if website:
        if "rss" not in media_object:
            sitemap_url = get_sitemap_url(website)
            print(f"\n******\n{sitemap_url}\n***********")
            if not sitemap_url:
                sitemap_url = get_sitemap_from_robots(website)
                print(f"Sitemap from robot: {sitemap_url}")
            if sitemap_url:
                rss_feed_url = get_rss_from_sitemap(sitemap_url)
                print(f"########\n{rss_feed_url}\n########\n")
            if rss_feed_url:
                media_object["rss"] = []
                media_object["rss"].append(rss_feed_url)
        elif len(media_object["rss"]) == 0:
            sitemap_url = get_sitemap_url(website)
            print(f"\n******\n{sitemap_url}\n***********")
            if not sitemap_url:
                sitemap_url = get_sitemap_from_robots(website)
                print(f"Sitemap from robot: {sitemap_url}")
            if sitemap_url:
                rss_feed_url = get_rss_from_sitemap(sitemap_url)
                print(f"########\n{rss_feed_url}\n########\n")
            if rss_feed_url:
                media_object["rss"].extend(rss_feed_url)
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
with open('updated_usa_2016_2024_with_sitemap.json', 'w', encoding='utf-8') as outfile:
    json.dump(json_data, outfile, ensure_ascii=False, indent=4)

