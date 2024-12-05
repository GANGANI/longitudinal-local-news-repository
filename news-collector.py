import json
import os
import hashlib
import gzip
import feedparser
import datetime
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, unquote, urlsplit
from NwalaTextUtils.textutils import derefURI
from NwalaTextUtils.textutils import cleanHtml

# Load the JSON file
with open("subset_updated_usa_2016_2024_with_sitemap.json", "r") as f:
    data = json.load(f)

# Define a function to create directory structure and save the article URL
def save_article_url(state, year, month, website_url, new_json_obj):
    # Hash the website URL to create a unique filename
    website_hash = hashlib.md5(website_url.encode()).hexdigest()

    print(f"Saved to {website_hash}\n")
    
    # Define the directory structure based on year and month
    directory_path = os.path.join("news", state, str(year), str(month))
    os.makedirs(directory_path, exist_ok=True)
    
    # Create a gzip file to store the URL
    filename = os.path.join(directory_path, f"{website_hash}.jsonl.gz")
    with gzip.open(filename, "at") as f:  
        f.write(json.dumps(new_json_obj) + '\n')

def extract_article_urls_from_html(html_content, base_url):
    soup = BeautifulSoup(html_content, 'html.parser')
    article_urls = set()
    
    # Normalize the base URL (e.g., remove trailing slash)
    parsed_base_url = urlparse(base_url)
    
    normalized_base_url = parsed_base_url.netloc
    
    for link in soup.find_all("a", href=True):
        # Resolve relative URLs
        url = urljoin(base_url, link['href'])
        
        # Normalize the URL for comparison
        parsed_url = urlparse(url)
        normalized_url = parsed_url.netloc
        print(f"---------------\n normalized_base_url: {normalized_base_url}, normalized_url: {normalized_url}")
        if normalized_url.startswith(normalized_base_url):
            print(f"{url}---------------\n")
            # Optional: Filter for article-like paths
            article_urls.add(url)
    
    return article_urls

def get_publication_date(entry):
    published_time = entry.get("published_parsed")
    
    # If no timestamp available, use the current date
    if published_time:
        timestamp = datetime.datetime(*published_time[:6])
    else:
        timestamp = datetime.datetime.now()
    return timestamp

# Define a browser-like User-Agent
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36'
}

def is_valid_url(url):
   # Validate if the URL follows the correct structure
   try:
       result = urlsplit(url)
       return all([result.scheme, result.netloc])
   except ValueError:
       return False
   
def extract_domain(url):
    # Decode any URL-encoded characters
    decoded_url = unquote(url)
    parsed_url = urlparse(decoded_url)
    
    # Get the netloc (domain) part
    domain = parsed_url.netloc
    
    # If the domain is empty, return None
    if not domain:
        return None
    
    # Clean the domain by removing any unwanted characters
    # Remove query parameters by splitting on '&' or '?'
    clean_domain = domain.split('&')[0]  # Remove query string starting with '&'
    clean_domain = clean_domain.split('?')[0]  # Remove query string starting with '?'

    # Remove 'www.' prefix if it exists
    if clean_domain.startswith("www."):
        clean_domain = clean_domain[4:]  # Strip 'www.'

    return clean_domain

def get_expanded_url(short_url):
    try:
        response = requests.head(short_url, allow_redirects=True)
        return response.url
    except requests.RequestException as e:
        print(f"An error occurred: {e}")
        return None
    
def has_special_characters(path_segment):
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
            print (f"Word count is less for {link}\n {plaintext}\n")
            is_news_article = False
    except Exception as e:
        # If an exception occurs, write the error message to the log file
        print(f"Error processing link: {link} ecause of {e}\n")
        is_news_article = False

    return is_news_article

# Iterate over each state and its publications in the JSON data
for state, publications in data.items():
    print(f"Processing state: {state}")
    
    # Iterate over the publications for the current state
    for publication in publications.get("newspaper", []):
        rss_feeds = publication.get("rss", [])
        website_url = publication.get("website")
        
        media_json_obj = {
            'media': publication
        }
        
        article_json_objs = []
        # Hash the website URL for the folder name
        website_hash = hashlib.md5(website_url.encode()).hexdigest()
        
        # If RSS feeds are available, parse them; otherwise, scrape from the website
        if rss_feeds:
            for rss_feed_url in rss_feeds:
                try:
                    # Fetch and parse the RSS feed
                    print(f"Fetching RSS feed from: {rss_feed_url}")
                    feed = feedparser.parse(rss_feed_url)
                    
                    # Iterate over each entry in the RSS feed
                    for entry in feed.entries:
                        article_url = entry.link
                        
                        if is_news_article(article_url):
                            published_time = get_publication_date(entry)
                        
                            # Extract the year and month for directory structure
                            year = timestamp.year
                            month = timestamp.month
                            # Save the article URL in the specified structure
                            article_json_obj = {
                                'link': article_url,
                                'publication_date': published_time.isoformat()  
                            }
                            article_json_objs.append(article_json_obj)
                                            
                except Exception as e:
                    print(f"Error processing RSS feed {rss_feed_url} for {publication.get('name')}: {e}")
        else:
            # If no RSS feed, try scraping the homepage for article URLs
            print(f"No RSS feed available for {publication.get('name')} ({website_url}), scraping HTML for article links.")
            try:
                response = requests.get(website_url, headers=HEADERS)
                response.raise_for_status()
                # Extract article URLs from HTML content
                article_urls = extract_article_urls_from_html(response.text, website_url)
                
                # Use the current date as a fallback timestamp for scraped articles
                timestamp = datetime.datetime.now()
                year = timestamp.year
                month = timestamp.month
                
                # Save each article URL found in HTML
                for article_url in article_urls:
                    print(f"*****************\n {article_url}")
                    if is_news_article(article_url):
                        article_json_obj = {
                            'link': article_url,
                            'publication_date': timestamp.isoformat()  
                        }
                        print(f"News Article \n***************\n")
                        article_json_objs.append(article_json_obj)
                
                print(f"\nFound {len(article_urls)} article URLs on {website_url}\n")
            
            except requests.RequestException as e:
                print(f"Error fetching HTML for {publication.get('name')} ({website_url}): {e}")

        media_json_obj['articles'] = article_json_objs
        save_article_url(state, year, month, website_url, media_json_obj)
