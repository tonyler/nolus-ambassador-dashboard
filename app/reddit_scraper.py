"""
Reddit Scraper - Extracts engagement metrics from Reddit posts using web scraping (no API required)
"""

import re
import time
import logging
import requests
from datetime import datetime
from typing import Tuple, Optional, Dict, List
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


class RedditScraper:
    """Scrapes engagement metrics from Reddit posts using requests + BeautifulSoup"""

    def __init__(self):
        """Initialize the scraper with session"""
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        })
        logger.info("Reddit scraper initialized successfully")

    def close_driver(self):
        """Close the session (for compatibility with old code)"""
        self.session.close()
        logger.info("Reddit scraper session closed")

    def _parse_count(self, count_str: str) -> int:
        """
        Parse engagement count from string format (e.g., '1.2k', '5m', '847')

        Args:
            count_str: String representation of count

        Returns:
            Integer count value
        """
        if not count_str:
            return 0

        count_str = count_str.strip().replace(',', '').lower()

        # Handle k (thousands) and m (millions)
        multiplier = 1
        if count_str.endswith('k'):
            multiplier = 1000
            count_str = count_str[:-1]
        elif count_str.endswith('m'):
            multiplier = 1000000
            count_str = count_str[:-1]
        elif count_str.endswith('b'):
            multiplier = 1000000000
            count_str = count_str[:-1]

        try:
            return int(float(count_str) * multiplier)
        except (ValueError, TypeError):
            return 0

    def _extract_metrics_from_json(self, soup: BeautifulSoup) -> Dict:
        """
        Extract metrics from JSON data embedded in the page

        Args:
            soup: BeautifulSoup object of the page

        Returns:
            Dictionary with metrics
        """
        metrics = {
            'score': 0,
            'comments': 0,
            'views': 0,
            'date_posted': None
        }

        try:
            # Old Reddit: Look for data attributes in the main post div
            thing = soup.find('div', class_='thing')
            if thing:
                # Get score from data-score attribute
                score_attr = thing.get('data-score')
                if score_attr and score_attr != 'â¢':
                    try:
                        metrics['score'] = int(float(score_attr))
                        logger.debug(f"Extracted score from data-score: {metrics['score']}")
                    except ValueError:
                        pass

                # Get comments from data-comments-count attribute