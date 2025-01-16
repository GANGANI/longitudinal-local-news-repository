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
        last_segment = path_segments[-1]
        if last_segment.isdigit() or has_special_characters(last_segment):
            depth = len(path_segments)
            if depth >= 3 :
                is_news_article = True
            elif depth <= 2 and any(has_special_characters(segment) or segment.isdigit() for segment in path_segments[:2]):
                is_news_article = True
            else:
                return is_news_article
        else:
            return is_news_article
    try:
        html = derefURI(link)
        plaintext = cleanHtml(html)
        count = len(plaintext)
        if count > 20:
            is_news_article = True
        else:
            print (f"Word count is less for {link}\n {plaintext}\n")
            is_news_article = False
    except Exception as e:
        # If an exception occurs, write the error message to the log file
        print(f"Error processing link: {link} ecause of {e}\n")
        is_news_article = False

    return is_news_article

def save_article_html(directory, article_url, html_content):
    """
    Save the HTML content of an article to a file.
    """
    try:
        article_hash = hashlib.md5(article_url.encode()).hexdigest()
        filepath = os.path.join(directory, f"{article_hash}.html.gz")
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with gzip.open(filepath, 'wt', encoding='utf-8') as html_file:
            html_file.write(html_content)
        logging.info(f"Article HTML saved to {filepath}")
        return filepath
    except Exception as e:
        logging.error(f"Error saving article HTML for {article_url}: {e}")
        return None
    
def get_archived_url(link):
    """Archive a URL using Internet Archive and return the archived URL."""
    try:
        result = subprocess.run(['archivenow', '--ia', link], capture_output=True, text=True, check=True)
        output = result.stdout.strip()
        if "Error" in output:
            logging.error(f"Archive error for {link}: {output}")
            if "Server Error" in output:
                logging.warning(f"Sleeping for 2 second due to server error")
                time.sleep(2)
            return None
        return output
    except subprocess.CalledProcessError as e:
        logging.error(f"Exception during archiving {link}: {e}")
        return None

def save_to_file(filepath, data, mode='at'):
    """Save JSON objects line by line to a file with optional gzip compression."""
    try:
        os.makedirs(os.path.dirname(filepath), exist_ok=True)

        # Handle binary or text modes appropriately
        if mode == 'wt':
            with gzip.open(filepath, mode, encoding='utf-8') as f:
                if isinstance(data, list):
                    for item in data:
                        f.write(json.dumps(item) + '\n')
                elif isinstance(data, dict):
                    f.write(json.dumps(data) + '\n')
                else:
                    raise ValueError("Data must be a list of JSON objects or a single JSON object.")
        elif mode == 'at':  # Append in binary mode
            with gzip.open(filepath, 'ab') as f:
                if isinstance(data, list):
                    for item in data:
                        f.write((json.dumps(item) + '\n').encode('utf-8'))
                elif isinstance(data, dict):
                    f.write((json.dumps(data) + '\n').encode('utf-8'))
                else:
                    raise ValueError("Data must be a list of JSON objects or a single JSON object.")
        else:
            raise ValueError("Unsupported mode for saving files.")
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
    """
    Save the HTML content of a publication's website to a gzipped file.
    """
    try:
        # Generate a unique hash for the website URL
        website_hash = hashlib.md5(website_url.encode()).hexdigest()
        directory_path = os.path.join("news", state, str(year), str(month), str(date), website_hash)
        os.makedirs(directory_path, exist_ok=True)

        # Define the filepath for the gzipped HTML content
        website_file_path = os.path.join(directory_path, f"{website_hash}.html.gz")

        # Check if the file already exists
        if not os.path.exists(website_file_path):
            # Fetch the website's HTML content
            response = requests.get(website_url, headers=HEADERS, timeout=10)
            response.raise_for_status()

            # Save the HTML content to a gzipped JSON file
            with gzip.open(website_file_path, 'wt', encoding='utf-8') as html_file:
                html_file.write(response.text)

            # Update the publication with the file path
            publication['archived_link'] = website_file_path
            logging.info(f"Website content saved to {website_file_path}")
        else:
            logging.info(f"Website content already exists at {website_file_path}")

    except requests.RequestException as e:
        logging.error(f"Error fetching website {website_url}: {e}")
    except Exception as e:
        logging.error(f"Error saving publication for {website_url}: {e}")

        
# Main Processing
def process_publication(state, publication, year, month, day, timestamp):
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
            if article_url not in cached_urls and is_news_article(article_url):
                logging.info(f"Found article: {article_url}")
                try:
                    response = requests.get(article_url, headers=HEADERS, timeout=10)
                    response.raise_for_status()
                    html_filepath = save_article_html(os.path.join(directory, f"{website_hash}-{timestamp}"), article_url, response.text)
                    if html_filepath:
                        article_json_objs.append({
                            'link': article_url,
                            'publication_date': get_publication_date(entry).isoformat(),
                            'saved_time': datetime.datetime.now().isoformat(),
                            'html_file_path': html_filepath
                        })
                        cached_urls.add(article_url)
                        nlinks += 1
                        if nlinks >= 5:
                            break
                except requests.RequestException as e:
                    logging.error(f"Error fetching article {article_url}: {e}")
                time.sleep(1)
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
                    try:
                        article_response = requests.get(article_url, headers=HEADERS, timeout=10)
                        article_response.raise_for_status()
                        html_filepath = save_article_html(os.path.join(directory, f"{website_hash}-{timestamp}"), article_url, article_response.text)
                        if html_filepath:
                            article_json_objs.append({
                                'link': article_url,
                                'publication_date': datetime.datetime.now().isoformat(),
                                'saved_time': datetime.datetime.now().isoformat(),
                                'html_file_path': html_filepath
                            })
                            cached_urls.add(article_url)
                            nlinks += 1
                            if nlinks >= 5:
                                break
                    except requests.RequestException as e:
                        logging.error(f"Error fetching article {article_url}: {e}")
                    time.sleep(1)
        except requests.RequestException as e:
            logging.error(f"Error scraping {website_url}: {e}")

    # Save Results
    save_to_file(os.path.join(directory, f"{website_hash}.jsonl.gz"), article_json_objs, 'at')
    save_to_file(cache_filepath, list(cached_urls), 'wt')

# Run the Script
while True:
    for state, publications in data.items():
        logging.info(f"Processing state: {state}")
        for news_media in ['newspaper', 'tv', 'radio', 'broadcast']:
            for publication in publications.get(news_media, []):
                response_status = publication.get('website_status')
                if response_status and (200 <= response_status < 300):
                    timestamp = datetime.datetime.now()
                    website_url = publication.get("website")
                    process_publication(state, publication, timestamp.year, timestamp.month, timestamp.day, timestamp)
                    save_publication(state, timestamp.year, timestamp.month, timestamp.day, website_url, publication)
    time.sleep(1)  # Prevent overwhelming the server
