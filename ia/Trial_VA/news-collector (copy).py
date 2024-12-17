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
with open("preprocessed_updated_news_media_rss_and_status_code.json", "r") as f:
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

def is_news_article(link, website_url):
    """Determine if a URL is a news article."""
    link = get_expanded_url(link)
    if not is_valid_url(link) or urlparse(website_url).path == urlparse(link).path:
        return False
    path_segments = [segment for segment in urlparse(link).path.split('/') if segment]
    if len(path_segments) >= 3 or any(has_special_characters(segment) for segment in path_segments[:2]):
        try:
            html = derefURI(link)
            plaintext = cleanHtml(html)
            return len(plaintext.strip()) > 20
        except Exception as e:
            logging.error(f"Error validating news article {link}: {e}")
            return False
    return False

def get_archived_url(link):
    """Archive a URL using Internet Archive and return the archived URL."""
    try:
        result = subprocess.run(['archivenow', '--ia', link], capture_output=True, text=True, check=True)
        output = result.stdout.strip()
        if "Error" in output:
            logging.error(f"Archive error for {link}: {output}")
            if "Server Error" in output:
                logging.warning(f"Sleeping for 1 second due to server error")
                time.sleep(1)
            return None
        return output
    except subprocess.CalledProcessError as e:
        logging.error(f"Exception during archiving {link}: {e}")
        return None

# File Operations
def save_to_file(filepath, data, mode='at'):
    """Save data to a file with optional gzip compression."""
    try:
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with gzip.open(filepath, mode) as f:
            f.write((data + '\n') if isinstance(data, str) else json.dumps(data) + '\n')
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

    article_json_objs = []
    nlinks = 0

    # Process RSS Feeds
    for rss_feed_url in rss_feeds:
        logging.info(f"Processing RSS feed: {rss_feed_url}")
        feed = feedparser.parse(rss_feed_url)
        for entry in feed.entries:
            article_url = entry.link
            if article_url not in cached_urls and is_news_article(article_url, website_url):
                logging.info(f"Found article: {article_url}")
                archived_url = get_archived_url(article_url)
                if archived_url:
                    article_json_objs.append({
                        'link': article_url,
                        'publication_date': get_publication_date(entry).isoformat(),
                        'archived_time': datetime.datetime.now().isoformat(),
                        'archived_link': archived_url
                    })
                    cached_urls.add(article_url)
                    nlinks += 1
                    if nlinks >= 5:
                        break
                time.sleep(0.5)
        if nlinks >= 5:
            break

    # Scrape Website if RSS Links Are Insufficient
    if nlinks < 5:
        try:
            logging.info(f"Scraping website: {website_url}")
            response = requests.get(website_url, headers=HEADERS, timeout=10)
            response.raise_for_status()
            for article_url in extract_article_urls_from_html(response.text, website_url):
                if article_url not in cached_urls and is_news_article(article_url, website_url):
                    logging.info(f"Found article: {article_url}")
                    archived_url = get_archived_url(article_url)
                    if archived_url:
                        article_json_objs.append({
                            'link': article_url,
                            'publication_date': datetime.datetime.now().isoformat(),
                            'archived_time': datetime.datetime.now().isoformat(),
                            'archived_link': archived_url
                        })
                        cached_urls.add(article_url)
                        nlinks += 1
                        if nlinks >= 5:
                            break
                    time.sleep(0.5)
        except requests.RequestException as e:
            logging.error(f"Error scraping {website_url}: {e}")

    # Save Results
    save_to_file(os.path.join(directory, f"{website_hash}.jsonl.gz"), article_json_objs, 'at')
    save_to_file(cache_filepath, '\n'.join(cached_urls), 'wt')

# Run the Script
while True:
    for state, publications in data.items():
        logging.info(f"Processing state: {state}")
        for news_media in ['newspaper', 'tv', 'radio', 'broadcast']:
            for publication in publications.get(news_media, []):
                response_status = publication.get('website_status')
                if response_status and (200 <= response_status < 300):
                    timestamp = datetime.datetime.now()
                    process_publication(state, publication, timestamp.year, timestamp.month, timestamp.day)
    time.sleep(1)  # Prevent overwhelming the server
