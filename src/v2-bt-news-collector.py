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


def has_special_characters(path_segment):
    """Check for special characters in path segments."""
    return any(char in path_segment for char in "-_.")


def is_news_article(link):
    is_news_article = False
    link = get_expanded_url(link)

    if not is_valid_url(link):
        print(f"Invalid URL: {link}")
        return is_news_article

    parsed_url = urlparse(link)
    path_segments = [segment for segment in parsed_url.path.split('/') if segment]
    if not path_segments:
        return is_news_article
    else:
        depth = len(path_segments)
        if depth >= 3:
            is_news_article = True
        elif depth <= 2 and any(has_special_characters(segment) for segment in path_segments[:2]):
            is_news_article = True
        else:
            return is_news_article

    try:
        html = derefURI(link)
        plaintext = cleanHtml(html)
        count = len(plaintext)
        if count > 20:
            is_news_article = True
        else:
            print(f"Word count is less for {link}\n {plaintext}\n")
            is_news_article = False
    except Exception as e:
        # If an exception occurs, write the error message to the log file
        print(f"Error processing link: {link} because of {e}\n")
        is_news_article = False

    return is_news_article


def get_archived_path(link, directory, website_hash):
    """Archive a URL using Browsertrix and return the archived file path."""
    try:
        os.makedirs(directory, exist_ok=True)
        
        command = f"docker run -v $PWD/{directory}:/crawls/ -it webrecorder/browsertrix-crawler crawl --url {link} --generateWACZ --collection {website_hash}"
        
        print(f"Starting subprocess: {command} with live logging...")

        # Open subprocess with real-time logging
        process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

        # Read output line by line
        for line in process.stdout:
            print(line, end="")  # Print logs in real time

        for line in process.stderr:
            print(line, end="")  # Capture and print errors as well

        process.wait()  # Wait for process to finish

        # Check if the archive file was created
        archive_path = os.path.join(directory, f"{website_hash}.wacz")
        if os.path.exists(archive_path):
            logging.info(f"Archived URL {link} at {archive_path}")
            return archive_path
        else:
            logging.warning(f"Archive process completed but WACZ file not found for {link}")
            return None

    except subprocess.SubprocessError as e:
        logging.error(f"Exception during archiving {link}: {e}")
        return None



def save_to_file(filepath, data, mode='at'):
    """Save JSON objects line by line to a file with optional gzip compression."""
    try:
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with gzip.open(filepath, mode, compresslevel=5) as f:
            if isinstance(data, list):
                # Write each item in the list as a separate JSON line
                for item in data:
                    f.write((json.dumps(item) + '\n').encode('utf-8'))
            elif isinstance(data, dict):
                # Write single JSON object as a line
                f.write((json.dumps(data) + '\n').encode('utf-8'))
            else:
                raise ValueError("Data must be a list of JSON objects or a single JSON object.")
        logging.info(f"Data saved to {filepath}")
    except Exception as e:
        logging.error(f"Error saving data to {filepath}: {e}")


def read_cached_urls(filepath):
    """Read cached URLs from a gzip file."""
    if os.path.exists(filepath):
        try:
            with gzip.open(filepath, "rt", encoding="utf-8") as f:
                return {line.strip() for line in f}
        except Exception as e:
            logging.error(f"Error reading cache file {filepath}: {e}")
    return set()


def save_publication(state, year, month, date, website_url, publication):
    website_hash = hashlib.md5(website_url.encode()).hexdigest()
    directory_path = os.path.join("news", state, str(year), str(month), str(date), str(website_hash))
    os.makedirs(directory_path, exist_ok=True)

    wesite_file_path = os.path.join(directory_path, f"{website_hash}.jsonl.gz")
    if not os.path.exists(wesite_file_path):
        logging.info(f"Website: {website_url} has been saved")
        archived_url = get_archived_path(website_url)
        if archived_url:
            publication['archived_link'] = archived_url
            with gzip.open(wesite_file_path, "at") as f:
                f.write(json.dumps(publication))


# Main Processing
def process_publication(state, publication, year, month, day):
    """Process a single publication and save its articles."""
    website_url = publication.get("website")
    logging.info(f"Processing publication: {website_url}")
    rss_feeds = publication.get("rss", [])
    website_hash = hashlib.md5(website_url.encode()).hexdigest()
    directory = os.path.join("news", state, str(year), str(month), str(day), website_hash)
    cache_filepath = os.path.join(directory, f"{website_hash}-cache.txt.gz")
    cached_urls = read_cached_urls(cache_filepath)
    website_json = None
    archived_website_path = get_archived_path(website_url, directory, website_hash)
    if archived_website_path:
        website_json = {
            'website_link': website_url,
            'publication_metadata': publication,
            'archived_time': datetime.datetime.now().isoformat(),
            'archived_path': archived_website_path
        }

    article_json_objs = []
    nlinks = 0

    # Process RSS Feeds
    for rss_feed_url in rss_feeds:
        logging.info(f"Processing RSS feed: {rss_feed_url}")
        feed = feedparser.parse(rss_feed_url)
        for entry in feed.entries:
            article_url = entry.link
            if article_url not in cached_urls and is_news_article(article_url):
                logging.info(f"Found article: {article_url}")
                archived_path = get_archived_path(article_url, directory, website_hash)
                if archived_path:
                    article_json_objs.append({
                        'link': article_url,
                        'publication_date': get_publication_date(entry).isoformat(),
                        'archived_time': datetime.datetime.now().isoformat(),
                        'archived_path': archived_path
                    })
                    cached_urls.add(article_url)
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
                if article_url not in cached_urls and is_news_article(article_url):
                    logging.info(f"Found article: {article_url}")
                    archived_path = get_archived_path(article_url, directory, website_hash)
                    if archived_path:
                        article_json_objs.append({
                            'link': article_url,
                            'publication_date': datetime.datetime.now().isoformat(),
                            'archived_time': datetime.datetime.now().isoformat(),
                            'archived_path': archived_path
                        })
                        cached_urls.add(article_url)
                        nlinks += 1
                        if nlinks >= 5:
                            break
                    time.sleep(5)
        except requests.RequestException as e:
            logging.error(f"Error scraping {website_url}: {e}")

    # Save Results
    save_to_file(os.path.join(directory, f"{website_hash}.jsonl.gz"), article_json_objs, 'at')
    save_to_file(os.path.join(directory, f"{website_hash}_metadata.jsonl.gz"), website_json, 'at')
    save_to_file(cache_filepath, '\n'.join(cached_urls), 'wt')

# Function to get the status code of a URL
def get_status_code(url):
    try:
        response = requests.get(url, timeout=10, allow_redirects=True)
        return response.status_code
    except requests.exceptions.RequestException as e:
        print(f"Error fetching {url}: {e}")
        return None  # In case of error, return None

# Run the Script
while True:
    for state, publications in data.items():
        logging.info(f"Processing state: {state}")
        for news_media in ['newspaper', 'tv', 'radio', 'broadcast']:
            for publication in publications.get(news_media, []):
                website_url = publication.get("website")
                response_status = get_status_code(website_url)
                logging.info(response_status)
                if response_status and (200 <= response_status < 300):
                    timestamp = datetime.datetime.now()
                    process_publication(state, publication, timestamp.year, timestamp.month, timestamp.day)
                    save_publication(state, timestamp.year, timestamp.month, timestamp.day, website_url, publication)
    time.sleep(1)  # Prevent overwhelming the server
