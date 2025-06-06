import logging
import json
import os
import hashlib
import gzip
import feedparser
import datetime
import requests
import subprocess
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, unquote, urlsplit
from NwalaTextUtils.textutils import derefURI, cleanHtml
import time
import subprocess

# Configure logging
logging.basicConfig(
    level=logging.INFO,  # Set the logging level
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("news_scraper.log"),  # Log to a file
        logging.StreamHandler()  # Also log to the console
    ]
)

logging.info("Starting the script...")

# Load the JSON file
with open("output.json", "r") as f:
    data = json.load(f)

# Define headers for HTTP requests
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36'
}


# Utility Functions
def is_valid_url(url):
    """Check if the URL is valid."""
    try:
        result = urlsplit(url)
        return all([result.scheme, result.netloc])
    except ValueError:
        logging.error(f"Invalid URL: {url}")
        return False


def extract_domain(url):
    """Extract and clean the domain from a URL."""
    parsed_url = urlparse(unquote(url))
    domain = parsed_url.netloc.split('&')[0].split('?')[0]
    return domain[4:] if domain.startswith("www.") else domain


def get_expanded_url(short_url):
    """Resolve short URLs to their final destination."""
    try:
        response = requests.head(short_url, allow_redirects=True, timeout=5)
        return response.url
    except requests.RequestException as e:
        logging.error(f"Error resolving URL: {short_url}: {e}")
        return short_url


def extract_article_urls_from_html(html_content, base_url):
    """Extract all article URLs from the given HTML content."""
    soup = BeautifulSoup(html_content, 'html.parser')
    resolved_base = get_expanded_url(base_url)
    return {
        urljoin(resolved_base, link['href'])
        for link in soup.find_all("a", href=True)
    }


def get_publication_date(entry):
    """Extract publication date from RSS entry."""
    published_time = entry.get("published_parsed")
    return datetime.datetime(*published_time[:6]) if published_time else datetime.datetime.now()


def is_news_article(link):
    is_news_article = False
    link = get_expanded_url(link)

    if not is_valid_url(link):
        logging.info(f"Invalid URL: {link} for is_news_article")
        return is_news_article

    parsed_url = urlparse(link)
    path_segments = [segment for segment in parsed_url.path.split('/') if segment]
    if not path_segments:
        return is_news_article
    else:
        depth = len(path_segments)
        if depth >= 3:
            is_news_article = True
        else:
            logging.info(f"Path depth({depth}) is less than 3 for {link}\n")
            return is_news_article

    try:
        html = derefURI(link)
        plaintext = cleanHtml(html)
        count = len(plaintext)
        if count > 20:
            is_news_article = True
        else:
            logging.info(f"Word count is less for {link}\n {plaintext}\n")
            is_news_article = False
    except Exception as e:
        # If an exception occurs, write the error message to the log file
        logging.error(f"Error processing link: {link} because of {e}\n")
        is_news_article = False

    return is_news_article


def get_archived_path(link, directory):
    """Archive a URL using Browsertrix and return the archived file path."""
    try:
        os.makedirs(directory, exist_ok=True)
        link = get_expanded_url(link)
        url_hash = hashlib.md5(link.encode()).hexdigest()
        command = f"docker run -v $PWD/{directory}:/crawls/ -it webrecorder/browsertrix-crawler crawl --url {link} --generateWACZ --collection {url_hash} --timeLimit 300"
        
        logging.info(f"Starting subprocess: {command} with live logging...")

        # Open subprocess with real-time logging
        process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

        # Read output line by line
        for line in process.stdout:
            print(line, end="")  

        for line in process.stderr:
            print(line, end="")  

        process.wait()  # Wait for process to finish

        # Check if the archive file was created
        archive_path = os.path.join(directory, "collection", url_hash)
        if os.path.exists(archive_path):
            logging.info(f"Archived URL {link} at {archive_path}")
            return archive_path
        else:
            logging.warning(f"Archive process completed but WACZ file not found for {link}")
            return None

    except subprocess.SubprocessError as e:
        logging.error(f"Exception during archiving {link}: {e}")
        return None


def save_to_file(filepath, data, mode='wb'):
    """Save JSON objects line by line to a file with optional gzip compression."""
    if not data:
        logging.warning(f"No data to save in {filepath}")
        return  # Exit early if there's no data

    try:
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with gzip.open(filepath, mode, compresslevel=5) as f:
            for item in (data if isinstance(data, list) else [data]):
                f.write(json.dumps(item).encode('utf-8') + b'\n')
        logging.info(f"Data saved to {filepath}")
    except Exception as e:
        logging.error(f"Error saving data to {filepath}: {e}")


# Main Processing
def process_publication(state, publication, year, month, day):
    """Process a single publication and save its articles."""

    website_url = publication.get("website")
    logging.info(f"Processing publication: {website_url}")

    rss_feeds = publication.get("rss", [])
    website_hash = hashlib.md5(website_url.encode()).hexdigest()
    directory = os.path.join("news", state, str(year), str(month), str(day), website_hash)

    metadata_file_path = os.path.join(directory, f"{website_hash}_metadata.jsonl.gz")

    # Check if the file already exists
    if not os.path.exists(metadata_file_path):
        archived_website_path = get_archived_path(website_url, directory)
        if archived_website_path:
            website_json = {
                'website_link': website_url,
                'publication_metadata': publication,
                'archived_time': datetime.datetime.now().isoformat(),
                'archived_path': archived_website_path
            }
            # Save to file (append if the file doesn't exist yet)
            save_to_file(metadata_file_path, website_json, 'ab')
            logging.info(f"Metadata of the website: {website_url} is successfully updated in the location: {metadata_file_path}")
    else:
        logging.info(f"File {metadata_file_path} already exists, skipping save.")

    article_json_objs = []
    nlinks = 0

    # Process RSS Feeds
    for rss_feed_url in rss_feeds:
        logging.info(f"Processing RSS feed: {rss_feed_url}")
        feed = feedparser.parse(rss_feed_url)
        for entry in feed.entries:
            article_url = entry.link
            if article_url and is_news_article(article_url):
                logging.info(f"Found article: {article_url}")
                archived_path = get_archived_path(article_url, directory)
                if archived_path:
                    article_json_objs.append({
                        'link': article_url,
                        'publication_date': get_publication_date(entry).isoformat(),
                        'archived_time': datetime.datetime.now(datetime.timezone.utc).isoformat(),
                        'archived_path': archived_path
                    })
                    nlinks += 1
                    if nlinks >= 5:
                        break
                time.sleep(5)
        if nlinks >= 5:
            break

    # Scrape Website if RSS Links Are Insufficient
    if nlinks < 5:
        try:
            logging.info(f"Scraping website: {website_url}")
            response = requests.get(website_url, headers=HEADERS, timeout=10)
            response.raise_for_status()
            for article_url in extract_article_urls_from_html(response.text, website_url):
                if article_url and is_news_article(article_url):
                    logging.info(f"Found article: {article_url}")
                    archived_path_excess = get_archived_path(article_url, directory)
                    if archived_path_excess:
                        article_json_objs.append({
                            'link': article_url,
                            'publication_date': datetime.datetime.now(datetime.timezone.utc).isoformat(),
                            'archived_time': datetime.datetime.now(datetime.timezone.utc).isoformat(),
                            'archived_path': archived_path_excess
                        })
                        nlinks += 1
                        if nlinks >= 5:
                            break
                    time.sleep(5)
        except requests.RequestException as e:
            logging.error(f"Error scraping {website_url}: {e}")

    website_article_location = os.path.join(directory, f"{website_hash}_articles.jsonl.gz")
    logging.info(f"Articles of the website: {website_url} is successfully updated in the location: {website_article_location}")
    save_to_file(website_article_location, article_json_objs, 'ab')
    

# Function to get the status code of a URL
def get_status_code(url):
    try:
        response = requests.get(url, timeout=10, allow_redirects=True)
        return response.status_code
    except requests.exceptions.RequestException as e:
        logging.info(f"Error fetching {url}: {e}")
        return None  # In case of error, return None

# Run the Script
while True:
    for state, publications in data.items():
        logging.info(f"Processing state: {state}")
        for news_media in ['newspaper', 'tv', 'radio', 'broadcast']:
            for publication in publications.get(news_media, []):
                website_url = publication.get("website")
                response_status = get_status_code(website_url)
                logging.info(f"The response status of {website_url} is: {response_status}")
                if response_status and (200 <= response_status < 300):
                    timestamp = datetime.datetime.now(datetime.timezone.utc)
                    logging.info(f"The response status of {website_url} is: {response_status}")
                    process_publication(state, publication, timestamp.year, timestamp.month, timestamp.day)
    time.sleep(1)  # Prevent overwhelming the server
