"""
X/Twitter Scraper - Extracts engagement metrics from X posts using Selenium
"""

import re
import time
import json
import logging
from datetime import datetime
from typing import Tuple, Optional, Dict, List
from pathlib import Path

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager

logger = logging.getLogger(__name__)


class XScraper:
    """Scrapes engagement metrics from X/Twitter posts using Selenium"""

    def __init__(self, cookie_file: Optional[str] = None):
        """
        Initialize the scraper with headless Chrome driver

        Args:
            cookie_file: Path to JSON file containing X session cookies (optional)
        """
        self.driver = None
        self.cookie_file = cookie_file
        self.cookies_loaded = False
        self._init_driver()

    def _init_driver(self):
        """Initialize headless Chrome WebDriver with anti-detection settings"""
        try:
            chrome_options = Options()
            chrome_options.add_argument('--headless=new')
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--disable-blink-features=AutomationControlled')
            chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
            chrome_options.add_argument('--window-size=1280,720')

            # Memory optimization
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('--disable-software-rasterizer')
            chrome_options.add_argument('--disable-extensions')
            chrome_options.add_argument('--disable-logging')
            chrome_options.add_argument('--disable-background-networking')
            chrome_options.add_argument('--disable-default-apps')
            chrome_options.add_argument('--disable-sync')
            chrome_options.add_argument('--metrics-recording-only')
            chrome_options.add_argument('--mute-audio')
            chrome_options.add_argument('--no-first-run')
            chrome_options.add_argument('--safebrowsing-disable-auto-update')
            chrome_options.add_argument('--disable-features=TranslateUI')
            chrome_options.add_argument('--disable-hang-monitor')
            chrome_options.add_argument('--disable-ipc-flooding-protection')
            chrome_options.add_argument('--disable-renderer-backgrounding')
            chrome_options.add_argument('--disable-backgrounding-occluded-windows')
            chrome_options.add_argument('--disable-breakpad')
            chrome_options.add_argument('--single-process')  # Reduce memory

            # Disable images to save bandwidth and memory
            prefs = {
                'profile.managed_default_content_settings.images': 2,
                'disk-cache-size': 4096
            }
            chrome_options.add_experimental_option('prefs', prefs)

            # Disable automation flags
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)

            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=chrome_options)

            # Set webdriver property to undefined
            self.driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
                'source': 'Object.defineProperty(navigator, "webdriver", {get: () => undefined})'
            })

            logger.info("Chrome WebDriver initialized successfully")

            # Load cookies if provided
            if self.cookie_file:
                self._load_cookies()

        except Exception as e:
            logger.error(f"Failed to initialize Chrome WebDriver: {e}")
            raise

    def _load_cookies(self):
        """Load cookies from JSON file and add them to the driver"""
        if not self.cookie_file:
            return

        try:
            # Resolve cookie file path (may be relative to the app directory)
            cookie_path = Path(__file__).parent / self.cookie_file
            if not cookie_path.exists():
                # Try absolute path
                cookie_path = Path(self.cookie_file)

            if not cookie_path.exists():
                logger.warning(f"Cookie file not found: {self.cookie_file}")
                return

            # Load cookies from JSON file
            with open(cookie_path, 'r') as f:
                cookies = json.load(f)

            # Navigate to X.com first (required by Selenium to set cookies)
            logger.info("Navigating to x.com to set cookies...")
            self.driver.get("https://x.com")
            time.sleep(2)  # Wait for initial page load

            # Add each cookie to the driver
            for cookie in cookies:
                try:
                    # Selenium requires specific cookie format
                    cookie_dict = {
                        'name': cookie['name'],
                        'value': cookie['value'],
                        'domain': cookie.get('domain', '.x.com'),
                        'path': cookie.get('path', '/'),
                        'secure': cookie.get('secure', False),
                    }

                    # Add optional fields if present
                    if 'expirationDate' in cookie:
                        cookie_dict['expiry'] = int(cookie['expirationDate'])
                    if 'httpOnly' in cookie:
                        cookie_dict['httpOnly'] = cookie['httpOnly']

                    # Handle sameSite attribute carefully
                    if 'sameSite' in cookie:
                        same_site = cookie['sameSite']
                        # Only add sameSite if it's a valid value for Selenium
                        if same_site in ['strict', 'lax', 'none']:
                            cookie_dict['sameSite'] = same_site.capitalize()
                        elif same_site == 'no_restriction':
                            cookie_dict['sameSite'] = 'None'

                    self.driver.add_cookie(cookie_dict)
                except Exception as e:
                    logger.debug(f"Failed to add cookie {cookie.get('name', 'unknown')}: {e}")

            self.cookies_loaded = True
            logger.info(f"Successfully loaded {len(cookies)} cookies from {cookie_path}")

            # Refresh page to apply cookies
            self.driver.refresh()
            time.sleep(2)

        except Exception as e:
            logger.error(f"Error loading cookies from {self.cookie_file}: {e}")

    def close_driver(self):
        """Close the WebDriver"""
        if self.driver:
            try:
                self.driver.quit()
                logger.info("Chrome WebDriver closed")
            except Exception as e:
                logger.error(f"Error closing driver: {e}")

    def _parse_count(self, count_str: str) -> int:
        """
        Parse engagement count from string format (e.g., '1.2K', '5M', '847')

        Args:
            count_str: String representation of count

        Returns:
            Integer count value
        """
        if not count_str:
            return 0

        count_str = count_str.strip().replace(',', '')

        # Handle K (thousands) and M (millions)
        multiplier = 1
        if count_str.endswith('K'):
            multiplier = 1000
            count_str = count_str[:-1]
        elif count_str.endswith('M'):
            multiplier = 1000000
            count_str = count_str[:-1]
        elif count_str.endswith('B'):
            multiplier = 1000000000
            count_str = count_str[:-1]

        try:
            return int(float(count_str) * multiplier)
        except (ValueError, TypeError):
            return 0

    def _extract_metrics_from_aria_labels(self) -> Dict[str, int]:
        """
        Extract metrics from aria-label attributes (most reliable method)

        Returns:
            Dictionary with replies, retweets, likes counts
        """
        metrics = {'replies': 0, 'retweets': 0, 'likes': 0}

        try:
            # METHOD 1: Try to find the engagement group with combined aria-label
            # X now uses a single role="group" element with all metrics in one aria-label
            # Example: "15 replies, 52 reposts, 149 likes, 31 bookmarks, 4818 views"
            groups = self.driver.find_elements(By.CSS_SELECTOR, '[role="group"]')
            for group in groups:
                aria_label = group.get_attribute('aria-label')
                if aria_label and ('repl' in aria_label.lower() or 'repost' in aria_label.lower()):
                    # Parse all metrics from the combined string
                    reply_match = re.search(r'(\d+[,\d]*[KMB]?)\s*repl', aria_label, re.IGNORECASE)
                    if reply_match:
                        metrics['replies'] = self._parse_count(reply_match.group(1))

                    retweet_match = re.search(r'(\d+[,\d]*[KMB]?)\s*repost', aria_label, re.IGNORECASE)
                    if retweet_match:
                        metrics['retweets'] = self._parse_count(retweet_match.group(1))

                    like_match = re.search(r'(\d+[,\d]*[KMB]?)\s*like', aria_label, re.IGNORECASE)
                    if like_match:
                        metrics['likes'] = self._parse_count(like_match.group(1))

                    # If we found metrics in this group, we're done
                    if metrics['replies'] or metrics['retweets'] or metrics['likes']:
                        logger.debug(f"Extracted metrics from group aria-label: {metrics}")
                        return metrics

            # METHOD 2 (Fallback): Try individual buttons (old X layout)
            if not metrics['replies']:
                reply_buttons = self.driver.find_elements(By.CSS_SELECTOR, '[data-testid="reply"]')
                for button in reply_buttons:
                    aria_label = button.get_attribute('aria-label')
                    if aria_label:
                        match = re.search(r'(\d+[,\d]*[KMB]?)\s*repl', aria_label, re.IGNORECASE)
                        if match:
                            metrics['replies'] = self._parse_count(match.group(1))
                            break

            if not metrics['retweets']:
                retweet_buttons = self.driver.find_elements(By.CSS_SELECTOR, '[data-testid="retweet"]')
                for button in retweet_buttons:
                    aria_label = button.get_attribute('aria-label')
                    if aria_label:
                        match = re.search(r'(\d+[,\d]*[KMB]?)\s*retweet', aria_label, re.IGNORECASE)
                        if match:
                            metrics['retweets'] = self._parse_count(match.group(1))
                            break

            if not metrics['likes']:
                like_buttons = self.driver.find_elements(By.CSS_SELECTOR, '[data-testid="like"]')
                for button in like_buttons:
                    aria_label = button.get_attribute('aria-label')
                    if aria_label:
                        match = re.search(r'(\d+[,\d]*[KMB]?)\s*like', aria_label, re.IGNORECASE)
                        if match:
                            metrics['likes'] = self._parse_count(match.group(1))
                            break

            logger.debug(f"Extracted metrics from aria-labels: {metrics}")
        except Exception as e:
            logger.warning(f"Error extracting from aria-labels: {e}")

        return metrics

    def _extract_metrics_from_text(self) -> Dict[str, int]:
        """
        Fallback method: Extract metrics from visible button text

        Returns:
            Dictionary with replies, retweets, likes counts
        """
        metrics = {'replies': 0, 'retweets': 0, 'likes': 0}

        try:
            # Try to find metrics in button text (order: reply, retweet, like)
            buttons = self.driver.find_elements(By.CSS_SELECTOR, '[role="group"] button')

            for button in buttons:
                text = button.text.strip()
                if text and text[0].isdigit():
                    count = self._parse_count(text)

                    # Determine which metric based on test-id or position
                    test_id = button.get_attribute('data-testid')
                    if test_id == 'reply' or 'reply' in button.get_attribute('aria-label').lower():
                        metrics['replies'] = count
                    elif test_id == 'retweet' or 'retweet' in button.get_attribute('aria-label').lower():
                        metrics['retweets'] = count
                    elif test_id == 'like' or 'like' in button.get_attribute('aria-label').lower():
                        metrics['likes'] = count

            logger.debug(f"Extracted metrics from text: {metrics}")
        except Exception as e:
            logger.warning(f"Error extracting from text: {e}")

        return metrics

    def _extract_impressions(self) -> int:
        """
        Extract impressions/views count from the post

        Returns:
            Impressions count
        """
        impressions = 0

        try:
            # METHOD 1: Check the engagement group aria-label for views
            groups = self.driver.find_elements(By.CSS_SELECTOR, '[role="group"]')
            for group in groups:
                aria_label = group.get_attribute('aria-label')
                if aria_label and 'view' in aria_label.lower():
                    # Example: "15 replies, 52 reposts, 149 likes, 31 bookmarks, 4818 views"
                    match = re.search(r'(\d+[,\d]*[KMB]?)\s*view', aria_label, re.IGNORECASE)
                    if match:
                        impressions = self._parse_count(match.group(1))
                        if impressions > 0:
                            logger.debug(f"Extracted impressions from group aria-label: {impressions}")
                            return impressions

            # METHOD 2: Look for spans containing view counts
            # X displays views like "4,818\n Views" or "4,818 Views"
            view_elements = self.driver.find_elements(By.XPATH, "//*[contains(text(), 'Views') or contains(text(), 'views') or contains(text(), 'View')]")
            for element in view_elements:
                # Get parent to capture number + text together
                try:
                    parent_text = element.find_element(By.XPATH, "..").text
                except:
                    parent_text = element.text

                # Match patterns like "4,818 Views" or "1.2K Views"
                match = re.search(r'(\d+[,\d]*[KMB]?)\s*[Vv]iews?', parent_text)
                if match:
                    impressions = self._parse_count(match.group(1))
                    if impressions > 0:
                        break

            # METHOD 3: Look for analytics/views link (old method)
            if impressions == 0:
                analytics_links = self.driver.find_elements(By.CSS_SELECTOR, 'a[href*="/analytics"]')
                for link in analytics_links:
                    text = link.text.strip()
                    match = re.search(r'(\d+[,\d]*[KMB]?)\s*[Vv]iews?', text)
                    if match:
                        impressions = self._parse_count(match.group(1))
                        break

            logger.debug(f"Extracted impressions: {impressions}")
        except Exception as e:
            logger.warning(f"Error extracting impressions: {e}")

        return impressions

    def _extract_author_handle(self) -> Optional[str]:
        """
        Extract the author's handle from the tweet page

        Returns:
            Author handle (without @) or None
        """
        try:
            # Method 1: Look for the author link in the tweet header
            # The author's handle appears in links like href="/username"
            author_links = self.driver.find_elements(
                By.CSS_SELECTOR,
                'article[data-testid="tweet"] a[href^="/"][role="link"]'
            )

            for link in author_links:
                href = link.get_attribute('href')
                if href and '/' in href:
                    # Extract handle from href like "https://x.com/username" or "/username"
                    parts = href.rstrip('/').split('/')
                    potential_handle = parts[-1]

                    # Skip non-handle paths
                    if potential_handle in ('home', 'explore', 'notifications', 'messages',
                                           'i', 'search', 'settings', 'compose', 'intent'):
                        continue
                    if potential_handle.startswith('status'):
                        continue
                    if not potential_handle or potential_handle.startswith('?'):
                        continue

                    # Valid handle found
                    logger.debug(f"Extracted author handle: {potential_handle}")
                    return potential_handle.lower()

            # Method 2: Look for @username text in the tweet
            username_elements = self.driver.find_elements(
                By.CSS_SELECTOR,
                'article[data-testid="tweet"] [dir="ltr"] span'
            )

            for elem in username_elements:
                text = elem.text.strip()
                if text.startswith('@'):
                    handle = text[1:].lower()  # Remove @ and lowercase
                    logger.debug(f"Extracted author handle from @mention: {handle}")
                    return handle

            logger.warning("Could not extract author handle from tweet")
            return None

        except Exception as e:
            logger.warning(f"Error extracting author handle: {e}")
            return None

    def _extract_date_posted(self) -> Optional[str]:
        """
        Extract the date when the tweet was posted

        Returns:
            ISO format timestamp string or None
        """
        try:
            # Handle quoted tweets - get the main tweet's date, not quoted tweet's date
            time_elements = self.driver.find_elements(By.TAG_NAME, 'time')

            for time_elem in time_elements:
                # Skip if inside a quoted tweet container
                parent_html = time_elem.find_element(By.XPATH, '../..').get_attribute('outerHTML')
                if 'quoteTweet' in parent_html:
                    continue

                datetime_attr = time_elem.get_attribute('datetime')
                if datetime_attr:
                    logger.debug(f"Extracted date_posted: {datetime_attr}")
                    return datetime_attr

            # Fallback: just get first time element
            if time_elements:
                datetime_attr = time_elements[0].get_attribute('datetime')
                logger.debug(f"Extracted date_posted (fallback): {datetime_attr}")
                return datetime_attr

        except Exception as e:
            logger.warning(f"Error extracting date_posted: {e}")

        return None

    def scrape_tweet_metrics(self, tweet_url: str, timeout: int = 15) -> Tuple[Optional[Dict], str]:
        """
        Scrape engagement metrics from a single tweet

        Args:
            tweet_url: Full URL to the tweet
            timeout: Page load timeout in seconds

        Returns:
            Tuple of (metrics_dict, message_string)
            metrics_dict contains: impressions, likes, retweets, replies, date_posted
        """
        try:
            logger.info(f"Scraping tweet: {tweet_url}")

            # Navigate to tweet URL
            self.driver.get(tweet_url)

            # Wait for page to load - look for tweet container
            try:
                WebDriverWait(self.driver, timeout).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, 'article[data-testid="tweet"]'))
                )
                # Additional wait for dynamic content
                time.sleep(2)
            except TimeoutException:
                error_msg = f"Timeout waiting for tweet to load: {tweet_url}"
                logger.error(error_msg)
                return None, error_msg

            # Extract metrics using multiple methods (fallback strategy)
            metrics = self._extract_metrics_from_aria_labels()

            # Fallback to text extraction if aria-labels failed
            if metrics['replies'] == 0 and metrics['retweets'] == 0 and metrics['likes'] == 0:
                metrics = self._extract_metrics_from_text()

            # Extract impressions
            metrics['impressions'] = self._extract_impressions()

            # Extract date posted
            metrics['date_posted'] = self._extract_date_posted()

            # Extract author handle
            metrics['author_handle'] = self._extract_author_handle()

            success_msg = f"Successfully scraped metrics for {tweet_url}"
            logger.info(f"{success_msg}: {metrics}")

            return metrics, success_msg

        except Exception as e:
            error_msg = f"Error scraping {tweet_url}: {str(e)}"
            logger.error(error_msg)
            return None, error_msg

    def scrape_multiple_tweets(self, tweet_urls: List[str], delay: int = 5) -> List[Tuple[str, Optional[Dict], str]]:
        """
        Scrape metrics from multiple tweets with delay between requests

        Args:
            tweet_urls: List of tweet URLs to scrape
            delay: Delay in seconds between requests (randomized Â±20%)

        Returns:
            List of tuples: (url, metrics_or_none, message)
        """
        results = []

        for i, url in enumerate(tweet_urls):
            metrics, message = self.scrape_tweet_metrics(url)
            results.append((url, metrics, message))

            # Add delay between requests (except after last one)
            if i < len(tweet_urls) - 1:
                # Randomize delay Â±20%
                import random
                actual_delay = delay * random.uniform(0.8, 1.2)
                logger.debug(f"Waiting {actual_delay:.1f}s before next request")
                time.sleep(actual_delay)

        return results


if __name__ == "__main__":
    # Test the scraper
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    scraper = XScraper()

    # Example tweet URL (replace with actual URL for testing)
    test_url = "https://x.com/NolusProtocol/status/1234567890"

    try:
        metrics, message = scraper.scrape_tweet_metrics(test_url)
        print(f"\n{message}")
        if metrics:
            print(f"Metrics: {metrics}")
    finally:
        scraper.close_driver()
