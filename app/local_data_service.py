"""
Local data service that provides the same interface as SheetsService
but reads from local SQLite database for faster performance.
"""

import logging
import threading
from datetime import datetime
from typing import List, Dict, Tuple, Optional, Any
import re
from functools import wraps

from db_service import DatabaseService
from config_loader import get_config

logger = logging.getLogger(__name__)


def safe_int(value: Any) -> int:
    """Convert value to int, return 0 if empty/None"""
    try:
        return int(value) if value else 0
    except (ValueError, TypeError):
        return 0


class LocalDataService:
    """Service class for local database operations, drop-in replacement for SheetsService."""

    def __init__(self, db_service: Optional[DatabaseService] = None):
        """Initialize local data service.

        Args:
            db_service: Database service instance. If None, creates a new one.
        """
        self.config = get_config()
        self.db_service = db_service or DatabaseService()

        # Initialize cache with thread lock (same as SheetsService)
        self._cache: Dict[str, Tuple[Any, float]] = {}
        self._cache_ttl = self.config.cache_ttl
        self._cache_lock = threading.Lock()

        logger.info("LocalDataService initialized")

    def _get_cache(self, key: str) -> Optional[Any]:
        """Get cached value if not expired."""
        with self._cache_lock:
            if key in self._cache:
                value, timestamp = self._cache[key]
                if (datetime.now().timestamp() - timestamp) < self._cache_ttl:
                    logger.debug(f"Cache hit for {key}")
                    return value
                else:
                    # Expired, remove from cache
                    del self._cache[key]
                    logger.debug(f"Cache expired for {key}")
            return None

    def _set_cache(self, key: str, value: Any) -> None:
        """Set cached value with current timestamp."""
        with self._cache_lock:
            self._cache[key] = (value, datetime.now().timestamp())
            logger.debug(f"Cache set for {key}")

    def _should_exclude_month(self, year: int, month: int) -> bool:
        """Check if a month should be excluded from leaderboard."""
        excluded_months = self.config.excluded_months
        month_key = f"{year}-{month:02d}"
        return month_key in excluded_months

    def get_x_leaderboard(self, year: Optional[int] = None, month: Optional[int] = None) -> Tuple[List[Dict], int]:
        """Get X/Twitter leaderboard data.

        Args:
            year: Filter by year (None for all time)
            month: Filter by month (None for all time)

        Returns:
            Tuple of (leaderboard list, total impressions)
        """
        try:
            # Check cache first
            cache_key = f"x_leaderboard_{year}_{month}"
            cached_result = self._get_cache(cache_key)
            if cached_result is not None:
                return cached_result

            # Get posts from database
            if year and month:
                # Check if this month should be excluded
                if self._should_exclude_month(year, month):
                    return ([], 0)

                # Get specific month
                month_name = datetime(year, month, 1).strftime('%b')
                posts = self.db_service.get_x_posts(month=month_name, year=year)
            else:
                # Get all posts
                posts = self.db_service.get_x_posts()

            # Filter by date_posted and exclusions
            filtered_posts = []
            for post in posts:
                date_posted = post.get('date_posted', '')
                if not date_posted:
                    continue

                try:
                    post_date = datetime.fromisoformat(date_posted).date()

                    # Skip excluded months
                    if self._should_exclude_month(post_date.year, post_date.month):
                        continue

                    # If year/month specified, filter by them
                    if year and month:
                        if post_date.year != year or post_date.month != month:
                            continue

                    filtered_posts.append(post)
                except Exception as e:
                    logger.debug(f"Error parsing date {date_posted}: {e}")
                    continue

            # Aggregate by ambassador
            ambassador_stats: Dict[str, Dict] = {}
            total_impressions_all = 0

            for post in filtered_posts:
                name = post['ambassador']
                if name not in ambassador_stats:
                    ambassador_stats[name] = {
                        'name': name,
                        'tweets': 0,
                        'total_impressions': 0,
                        'total_likes': 0,
                        'total_replies': 0,
                        'total_retweets': 0
                    }

                ambassador_stats[name]['tweets'] += 1
                ambassador_stats[name]['total_impressions'] += safe_int(post.get('impressions'))
                ambassador_stats[name]['total_likes'] += safe_int(post.get('likes'))
                ambassador_stats[name]['total_replies'] += safe_int(post.get('replies'))
                ambassador_stats[name]['total_retweets'] += safe_int(post.get('retweets'))
                total_impressions_all += safe_int(post.get('impressions'))

            # Sort by total impressions
            leaderboard = sorted(ambassador_stats.values(), key=lambda x: x['total_impressions'], reverse=True)

            # Cache the result
            result = (leaderboard, total_impressions_all)
            self._set_cache(cache_key, result)
            return result

        except Exception as e:
            logger.error(f"Error getting X leaderboard: {e}", exc_info=True)
            return [], 0

    def get_reddit_leaderboard(self, year: Optional[int] = None, month: Optional[int] = None) -> List[Dict]:
        """Get Reddit leaderboard data.

        Args:
            year: Filter by year (None for all time)
            month: Filter by month (None for all time)

        Returns:
            List of ambassador statistics
        """
        try:
            # Check cache first
            cache_key = f"reddit_leaderboard_{year}_{month}"
            cached_result = self._get_cache(cache_key)
            if cached_result is not None:
                return cached_result

            # Get posts from database
            if year and month:
                # Check if this month should be excluded
                if self._should_exclude_month(year, month):
                    return []

                # Get specific month
                month_name = datetime(year, month, 1).strftime('%b')
                posts = self.db_service.get_reddit_posts(month=month_name, year=year)
            else:
                # Get all posts
                posts = self.db_service.get_reddit_posts()

            # Filter by date_posted and exclusions
            filtered_posts = []
            for post in posts:
                date_posted = post.get('date_posted', '')
                if not date_posted:
                    continue

                try:
                    post_date = datetime.fromisoformat(date_posted).date()

                    # Skip excluded months
                    if self._should_exclude_month(post_date.year, post_date.month):
                        continue

                    # If year/month specified, filter by them
                    if year and month:
                        if post_date.year != year or post_date.month != month:
                            continue

                    filtered_posts.append(post)
                except Exception as e:
                    logger.debug(f"Error parsing date {date_posted}: {e}")
                    continue

            # Aggregate by ambassador
            ambassador_stats: Dict[str, Dict] = {}

            for post in filtered_posts:
                name = post['ambassador']
                if name not in ambassador_stats:
                    ambassador_stats[name] = {
                        'name': name,
                        'posts': 0,
                        'total_score': 0,
                        'total_comments': 0,
                        'total_views': 0
                    }

                ambassador_stats[name]['posts'] += 1
                ambassador_stats[name]['total_score'] += safe_int(post.get('score'))
                ambassador_stats[name]['total_comments'] += safe_int(post.get('comments'))
                ambassador_stats[name]['total_views'] += safe_int(post.get('views'))

            # Sort by total score
            leaderboard = sorted(ambassador_stats.values(), key=lambda x: x['total_score'], reverse=True)

            # Cache the result
            self._set_cache(cache_key, leaderboard)
            return leaderboard

        except Exception as e:
            logger.error(f"Error getting Reddit leaderboard: {e}", exc_info=True)
            return []

    def get_total_leaderboard(self, year: Optional[int] = None, month: Optional[int] = None) -> List[Dict]:
        """Get combined leaderboard from both X and Reddit.

        Args:
            year: Filter by year (None for all time)
            month: Filter by month (None for all time)

        Returns:
            List of combined ambassador statistics
        """
        try:
            # Check cache first
            cache_key = f"total_leaderboard_{year}_{month}"
            cached_result = self._get_cache(cache_key)
            if cached_result is not None:
                return cached_result

            # Get both leaderboards
            x_leaderboard, _ = self.get_x_leaderboard(year, month)
            reddit_leaderboard = self.get_reddit_leaderboard(year, month)

            # Create dictionaries for quick lookup
            x_stats = {item['name']: item for item in x_leaderboard}
            reddit_stats = {item['name']: item for item in reddit_leaderboard}

            # Get all unique ambassadors
            all_ambassadors = set(x_stats.keys()) | set(reddit_stats.keys())

            # Combine stats
            combined_stats = []
            for name in all_ambassadors:
                x_data = x_stats.get(name, {})
                reddit_data = reddit_stats.get(name, {})

                combined_stats.append({
                    'name': name,
                    'x_tweets': x_data.get('tweets', 0),
                    'x_impressions': x_data.get('total_impressions', 0),
                    'x_likes': x_data.get('total_likes', 0),
                    'x_replies': x_data.get('total_replies', 0),
                    'x_retweets': x_data.get('total_retweets', 0),
                    'reddit_posts': reddit_data.get('posts', 0),
                    'reddit_score': reddit_data.get('total_score', 0),
                    'reddit_comments': reddit_data.get('total_comments', 0),
                    'reddit_views': reddit_data.get('total_views', 0),
                    'total_posts': x_data.get('tweets', 0) + reddit_data.get('posts', 0),
                    'combined_score': (
                        x_data.get('total_impressions', 0) * 0.001 +  # Weight impressions
                        x_data.get('total_likes', 0) +
                        reddit_data.get('total_score', 0) * 10  # Weight Reddit score higher
                    )
                })

            # Sort by combined score
            leaderboard = sorted(combined_stats, key=lambda x: x['combined_score'], reverse=True)

            # Cache the result
            self._set_cache(cache_key, leaderboard)
            return leaderboard

        except Exception as e:
            logger.error(f"Error getting total leaderboard: {e}", exc_info=True)
            return []

    def get_snapshots(self, month: Optional[str] = None, year: Optional[int] = None) -> List[Dict]:
        """Get snapshot data for graphs.

        Args:
            month: Month name (e.g., 'Dec')
            year: Year (e.g., 2025)

        Returns:
            List of snapshot dictionaries
        """
        try:
            # Check cache first
            cache_key = f"snapshots_{month}_{year}"
            cached_result = self._get_cache(cache_key)
            if cached_result is not None:
                return cached_result

            # Get from database
            if month and year:
                snapshots = self.db_service.get_snapshots(month=month, year=year)
            else:
                # Default to current month
                now = datetime.now()
                month = now.strftime('%b')
                year = now.year
                snapshots = self.db_service.get_snapshots(month=month, year=year)

            # Transform to match expected format
            result = []
            for snapshot in snapshots:
                result.append({
                    'Date': snapshot.get('date', ''),
                    'X_Impressions': snapshot.get('x_impressions', 0),
                    'X_Likes': snapshot.get('x_likes', 0),
                    'X_Retweets': snapshot.get('x_retweets', 0),
                    'X_Replies': snapshot.get('x_replies', 0),
                    'X_Posts': snapshot.get('x_posts', 0),
                    'Reddit_Score': snapshot.get('reddit_score', 0),
                    'Reddit_Comments': snapshot.get('reddit_comments', 0),
                    'Reddit_Views': snapshot.get('reddit_views', 0),
                    'Reddit_Posts': snapshot.get('reddit_posts', 0),
                })

            # Cache the result
            self._set_cache(cache_key, result)
            return result

        except Exception as e:
            logger.error(f"Error getting snapshots: {e}", exc_info=True)
            return []

    def clear_cache(self) -> None:
        """Clear all cached data."""
        with self._cache_lock:
            self._cache.clear()
            logger.info("Cache cleared")

    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics.

        Returns:
            Dictionary with cache stats
        """
        with self._cache_lock:
            return {
                'cache_size': len(self._cache),
                'cache_ttl': self._cache_ttl,
                'cached_keys': list(self._cache.keys())
            }

    def get_available_months(self) -> List[Tuple[int, int]]:
        """Get list of available months with data.

        Returns:
            List of (year, month) tuples, most recent first
        """
        try:
            cache_key = "available_months"
            cached_result = self._get_cache(cache_key)
            if cached_result is not None:
                return cached_result

            conn = self.db_service._get_connection()
            try:
                cursor = conn.cursor()

                # Get distinct year/month from x_posts
                cursor.execute('''
                    SELECT DISTINCT year, month FROM x_posts
                    UNION
                    SELECT DISTINCT year, month FROM reddit_posts
                    ORDER BY year DESC, month DESC
                ''')

                rows = cursor.fetchall()
                result = []

                # Convert month names to numbers
                month_map = {
                    'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4,
                    'May': 5, 'Jun': 6, 'Jul': 7, 'Aug': 8,
                    'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12
                }

                for row in rows:
                    year = row['year']
                    month_name = row['month']
                    month_num = month_map.get(month_name, 1)
                    result.append((year, month_num))

                # Sort by year desc, month desc
                result.sort(key=lambda x: (x[0], x[1]), reverse=True)

                # If no data, return current month
                if not result:
                    now = datetime.now()
                    result = [(now.year, now.month)]

                self._set_cache(cache_key, result)
                return result

            finally:
                conn.close()

        except Exception as e:
            logger.error(f"Error getting available months: {e}", exc_info=True)
            now = datetime.now()
            return [(now.year, now.month)]

    def get_x_daily_stats(self, year: int, month: int) -> Optional[Dict[str, List]]:
        """Get daily X stats for graphing.

        Args:
            year: Year to query
            month: Month to query

        Returns:
            Dictionary with 'dates' and 'impressions' lists, or None if no data
        """
        try:
            cache_key = f"x_daily_stats_{year}_{month}"
            cached_result = self._get_cache(cache_key)
            if cached_result is not None:
                return cached_result

            month_name = datetime(year, month, 1).strftime('%b')
            snapshots = self.db_service.get_snapshots(month=month_name, year=year)

            if not snapshots:
                return None

            dates = []
            impressions = []

            for snapshot in snapshots:
                date_str = snapshot.get('date', '')
                if date_str:
                    # Format date for display (e.g., "Jan 15")
                    try:
                        date_obj = datetime.fromisoformat(date_str)
                        dates.append(date_obj.strftime('%b %d'))
                    except Exception:
                        dates.append(date_str)
                    impressions.append(snapshot.get('x_impressions', 0))

            if not dates:
                return None

            result = {'dates': dates, 'impressions': impressions}
            self._set_cache(cache_key, result)
            return result

        except Exception as e:
            logger.error(f"Error getting X daily stats: {e}", exc_info=True)
            return None

    def get_reddit_daily_stats(self, year: int, month: int) -> Optional[Dict[str, List]]:
        """Get daily Reddit stats for graphing.

        Args:
            year: Year to query
            month: Month to query

        Returns:
            Dictionary with 'dates' and 'scores' lists, or None if no data
        """
        try:
            cache_key = f"reddit_daily_stats_{year}_{month}"
            cached_result = self._get_cache(cache_key)
            if cached_result is not None:
                return cached_result

            month_name = datetime(year, month, 1).strftime('%b')
            snapshots = self.db_service.get_snapshots(month=month_name, year=year)

            if not snapshots:
                return None

            dates = []
            scores = []

            for snapshot in snapshots:
                date_str = snapshot.get('date', '')
                if date_str:
                    try:
                        date_obj = datetime.fromisoformat(date_str)
                        dates.append(date_obj.strftime('%b %d'))
                    except Exception:
                        dates.append(date_str)
                    scores.append(snapshot.get('reddit_score', 0))

            if not dates:
                return None

            result = {'dates': dates, 'scores': scores}
            self._set_cache(cache_key, result)
            return result

        except Exception as e:
            logger.error(f"Error getting Reddit daily stats: {e}", exc_info=True)
            return None

    def get_daily_impressions_for_graph(self, year: int, month: int) -> Optional[Dict[str, List]]:
        """Get combined daily stats for the total leaderboard graph.

        Args:
            year: Year to query
            month: Month to query

        Returns:
            Dictionary with 'dates', 'x_impressions', 'reddit_views' lists, or None
        """
        try:
            cache_key = f"daily_impressions_graph_{year}_{month}"
            cached_result = self._get_cache(cache_key)
            if cached_result is not None:
                return cached_result

            month_name = datetime(year, month, 1).strftime('%b')
            snapshots = self.db_service.get_snapshots(month=month_name, year=year)

            if not snapshots:
                return None

            dates = []
            x_impressions = []
            reddit_views = []

            for snapshot in snapshots:
                date_str = snapshot.get('date', '')
                if date_str:
                    try:
                        date_obj = datetime.fromisoformat(date_str)
                        dates.append(date_obj.strftime('%b %d'))
                    except Exception:
                        dates.append(date_str)
                    x_impressions.append(snapshot.get('x_impressions', 0))
                    reddit_views.append(snapshot.get('reddit_views', 0))

            if not dates:
                return None

            result = {
                'dates': dates,
                'x_impressions': x_impressions,
                'reddit_views': reddit_views
            }
            self._set_cache(cache_key, result)
            return result

        except Exception as e:
            logger.error(f"Error getting daily impressions for graph: {e}", exc_info=True)
            return None

    def add_content(self, ambassador: str, content_url: str) -> Tuple[bool, str]:
        """Add new content submission.

        Args:
            ambassador: Ambassador name
            content_url: URL of the content (X or Reddit)

        Returns:
            Tuple of (success, message)
        """
        try:
            # Validate URL
            if not content_url or not content_url.startswith('http'):
                return False, "Invalid URL provided"

            # Determine platform and extract post ID
            now = datetime.now()
            month_name = now.strftime('%b')
            year = now.year

            # X/Twitter URL patterns
            x_patterns = [
                r'(?:twitter\.com|x\.com)/\w+/status/(\d+)',
            ]

            # Reddit URL patterns
            reddit_patterns = [
                r'reddit\.com/r/\w+/comments/(\w+)',
                r'redd\.it/(\w+)',
            ]

            post_id = None
            platform = None

            for pattern in x_patterns:
                match = re.search(pattern, content_url)
                if match:
                    post_id = match.group(1)
                    platform = 'x'
                    break

            if not post_id:
                for pattern in reddit_patterns:
                    match = re.search(pattern, content_url)
                    if match:
                        post_id = match.group(1)
                        platform = 'reddit'
                        break

            if not post_id or not platform:
                return False, "Could not parse URL. Please provide a valid X or Reddit post URL."

            # Insert into database
            if platform == 'x':
                posts = [{
                    'ambassador': ambassador,
                    'tweet_url': content_url,
                    'tweet_id': post_id,
                    'impressions': 0,
                    'likes': 0,
                    'retweets': 0,
                    'replies': 0,
                    'date_posted': now.isoformat(),
                    'submitted_date': now.isoformat(),
                    'month': month_name,
                    'year': year
                }]
                self.db_service.upsert_x_posts(posts)
            else:
                posts = [{
                    'ambassador': ambassador,
                    'url': content_url,
                    'post_id': post_id,
                    'score': 0,
                    'comments': 0,
                    'views': 0,
                    'date_posted': now.isoformat(),
                    'submitted_date': now.isoformat(),
                    'month': month_name,
                    'year': year
                }]
                self.db_service.upsert_reddit_posts(posts)

            # Invalidate relevant caches
            self.clear_cache()

            platform_name = 'X' if platform == 'x' else 'Reddit'
            return True, f"Successfully added {platform_name} content for {ambassador}"

        except Exception as e:
            logger.error(f"Error adding content: {e}", exc_info=True)
            return False, f"Error adding content: {str(e)}"

    def update_reddit_stats(self, year: Optional[int] = None, month: Optional[int] = None) -> Tuple[bool, str]:
        """Trigger Reddit stats refresh.

        Args:
            year: Year to refresh (None for current)
            month: Month to refresh (None for current)

        Returns:
            Tuple of (success, message)
        """
        try:
            # For now, just invalidate cache
            # In a full implementation, this would trigger the Reddit scraper
            self.clear_cache()

            if year and month:
                return True, f"Cache cleared for {year}/{month}. Stats will refresh on next load."
            return True, "Cache cleared. Stats will refresh on next load."

        except Exception as e:
            logger.error(f"Error updating Reddit stats: {e}", exc_info=True)
            return False, f"Error: {str(e)}"
