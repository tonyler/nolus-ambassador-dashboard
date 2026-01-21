"""
Ambassador service for handle-to-ambassador resolution and metrics updates.
Extracted from local_data_service.py to maintain file size limits.
"""

import re
import logging
from datetime import datetime
from typing import Dict, List, Tuple, Optional, Any

from config_loader import get_config
from db_service import DatabaseService

logger = logging.getLogger(__name__)

# Maximum URL length to prevent ReDoS attacks
MAX_URL_LENGTH = 2048


class AmbassadorService:
    """Service for ambassador resolution and X post metrics management."""

    def __init__(self, db_service: Optional[DatabaseService] = None):
        """Initialize ambassador service.

        Args:
            db_service: Database service instance. If None, creates a new one.
        """
        self.config = get_config()
        self.db_service = db_service or DatabaseService()
        logger.info("AmbassadorService initialized")

    def resolve_ambassador_from_handle(self, handle: str, platform: str = 'x') -> Optional[str]:
        """Look up ambassador name from platform handle.

        Args:
            handle: Platform handle (X handle or Reddit username)
            platform: 'x' or 'reddit'

        Returns:
            Ambassador name or None if not found

        Raises:
            ValueError: If platform is not 'x' or 'reddit'
        """
        if not handle:
            return None

        if platform not in ('x', 'reddit'):
            raise ValueError(f"Invalid platform '{platform}'. Must be 'x' or 'reddit'")

        if platform == 'x':
            return self.config.get_ambassador_by_x_handle(handle)
        else:
            return self.config.get_ambassador_by_reddit_username(handle)

    def update_x_post_ambassador_from_handle(self, tweet_id: str, author_handle: str) -> Tuple[bool, str]:
        """Update X post's ambassador based on scraped author handle.

        Args:
            tweet_id: Tweet ID
            author_handle: Scraped author handle

        Returns:
            Tuple of (success, message)
        """
        if not author_handle:
            return False, "No author handle provided"

        ambassador = self.config.get_ambassador_by_x_handle(author_handle)
        if not ambassador:
            return False, f"No ambassador found for X handle '{author_handle}'"

        updated = self.db_service.update_x_post_ambassador(tweet_id, ambassador)
        if updated:
            return True, f"Updated ambassador to '{ambassador}' from handle '{author_handle}'"
        return False, "Post not found or not updated"

    def get_current_month_x_posts(self) -> List[Dict[str, Any]]:
        """Get X posts for current month that need scraping.

        Returns:
            List of post dictionaries with Tweet_URL and Ambassador keys
        """
        try:
            now = datetime.now()
            month_name = now.strftime('%b')
            year = now.year

            posts = self.db_service.get_x_posts(month=month_name, year=year)

            # Transform to expected format for scheduler
            result = []
            for post in posts:
                result.append({
                    'Tweet_URL': post.get('tweet_url', ''),
                    'Ambassador': post.get('ambassador', 'Unknown'),
                    'tweet_id': post.get('tweet_id', '')
                })

            return result

        except Exception as e:
            logger.error(f"Error getting current month X posts: {e}", exc_info=True)
            return []

    def update_x_post_metrics(self, tweet_url: str, metrics: Dict[str, Any]) -> Tuple[bool, str]:
        """Update X post metrics from scraper.

        Args:
            tweet_url: Tweet URL
            metrics: Dictionary with impressions, likes, retweets, replies, date_posted, author_handle

        Returns:
            Tuple of (success, message)
        """
        try:
            # Validate URL length to prevent ReDoS
            if not tweet_url or len(tweet_url) > MAX_URL_LENGTH:
                return False, "Invalid or too long URL"

            # Extract tweet ID from URL
            match = re.search(r'/status/(\d+)', tweet_url)
            if not match:
                return False, "Could not extract tweet ID from URL"

            tweet_id = match.group(1)

            # Check if author_handle was scraped and update ambassador if needed
            author_handle = metrics.get('author_handle')
            if author_handle:
                ambassador = self.config.get_ambassador_by_x_handle(author_handle)
                if ambassador:
                    self.db_service.update_x_post_ambassador(tweet_id, ambassador)
                    logger.info(f"Auto-assigned ambassador '{ambassador}' from handle '{author_handle}'")

            # Update metrics
            now = datetime.now()
            month_name = now.strftime('%b')
            year = now.year

            posts = [{
                'ambassador': metrics.get('ambassador', 'Unknown'),
                'tweet_url': tweet_url,
                'tweet_id': tweet_id,
                'impressions': metrics.get('impressions', 0),
                'likes': metrics.get('likes', 0),
                'retweets': metrics.get('retweets', 0),
                'replies': metrics.get('replies', 0),
                'date_posted': metrics.get('date_posted', now.isoformat()),
                'submitted_date': now.isoformat(),
                'month': month_name,
                'year': year
            }]

            self.db_service.upsert_x_posts(posts)

            return True, f"Updated metrics for tweet {tweet_id}"

        except Exception as e:
            logger.error(f"Error updating X post metrics: {e}", exc_info=True)
            return False, f"Error: {str(e)}"
