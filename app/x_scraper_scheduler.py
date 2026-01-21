"""
X Scraper Scheduler - Automated scraping with blocking detection and recovery
"""

import os
import sys
import time
import logging
import random
from datetime import datetime, timedelta
from typing import Dict, Tuple

from dotenv import load_dotenv

from x_scraper import XScraper
from sheets_service import SheetsService
from config_loader import get_config

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('x_scraper.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)


class XScraperScheduler:
    """
    Automated X/Twitter scraper with sophisticated blocking detection and recovery
    """

    def __init__(self):
        self.config = get_config()
        self.sheets_service = SheetsService()
        self.scraper = None

        # Blocking detection
        self.consecutive_failures = 0
        self.last_success_time = datetime.now()
        self.total_processed = 0
        self.total_success = 0
        self.total_failed = 0

        # Configuration
        self.max_consecutive_failures = self.config.x_scraper_max_failures
        self.blocking_base_wait_minutes = self.config.x_scraper_blocking_base_wait
        self.blocking_max_wait_hours = self.config.x_scraper_blocking_max_wait
        self.scrape_delay_seconds = self.config.x_scraper_delay
        self.schedule_interval_minutes = self.config.x_scraper_schedule_interval
        self.cookie_file = self.config.x_scraper_cookie_file

    def _init_scraper(self):
        """Initialize or reinitialize the scraper"""
        if self.scraper:
            try:
                self.scraper.close_driver()
            except:
                pass

        self.scraper = XScraper(cookie_file=self.cookie_file)
        if self.cookie_file:
            logger.info(f"Scraper initialized with cookie file: {self.cookie_file}")
        else:
            logger.info("Scraper initialized without cookies")

    def _is_blocking_error(self, error_message: str) -> bool:
        """
        Detect if an error indicates X/Twitter blocking

        Args:
            error_message: Error message string

        Returns:
            True if error indicates blocking
        """
        blocking_indicators = [
            'rate limit', 'rate-limit', '403', '429', 'captcha',
            'suspended', 'blocked', 'unauthorized', 'protected', 'timeout'
        ]

        error_lower = error_message.lower()
        return any(indicator in error_lower for indicator in blocking_indicators)

    def _is_blocked(self) -> bool:
        """
        Determine if scraper is currently blocked by X/Twitter

        Returns:
            True if blocking is detected
        """
        # Method 1: Too many consecutive failures
        if self.consecutive_failures >= self.max_consecutive_failures:
            logger.warning(f"Blocking detected: {self.consecutive_failures} consecutive failures")
            return True

        # Method 2: Multiple failures + no success in 30 minutes
        time_since_success = datetime.now() - self.last_success_time
        if self.consecutive_failures >= 3 and time_since_success > timedelta(minutes=30):
            logger.warning(f"Blocking detected: {self.consecutive_failures} failures and no success in {time_since_success}")
            return True

        return False

    def _calculate_wait_time(self) -> int:
        """
        Calculate exponential backoff wait time when blocked

        Returns:
            Wait time in seconds
        """
        # Exponential backoff: base_wait * 2^(failures // 5)
        exponent = self.consecutive_failures // 5
        wait_minutes = self.blocking_base_wait_minutes * (2 ** exponent)

        # Cap at max wait time
        max_wait_minutes = self.blocking_max_wait_hours * 60
        wait_minutes = min(wait_minutes, max_wait_minutes)

        return wait_minutes * 60  # Convert to seconds

    def _wait_for_unblock(self):
        """Wait with exponential backoff when blocking detected"""
        wait_seconds = self._calculate_wait_time()
        wait_hours = wait_seconds / 3600

        logger.warning(f"Waiting {wait_hours:.1f} hours for potential unblocking...")

        # Sleep in chunks to allow for interruption
        chunk_size = 60  # 1 minute chunks
        chunks = int(wait_seconds / chunk_size)

        for i in range(chunks):
            time.sleep(chunk_size)
            if (i + 1) % 10 == 0:  # Log every 10 minutes
                elapsed = (i + 1) * chunk_size / 60
                remaining = (chunks - i - 1) * chunk_size / 60
                logger.info(f"Wait progress: {elapsed:.0f}m elapsed, {remaining:.0f}m remaining")

        # Reduce consecutive failures counter after wait
        self.consecutive_failures = max(0, self.consecutive_failures - 2)
        logger.info(f"Wait complete. Reduced failure counter to {self.consecutive_failures}")

    def _scrape_single_tweet(self, post: Dict) -> Tuple[bool, str]:
        """
        Scrape a single tweet and update metrics

        Args:
            post: Dictionary with tweet data from sheets

        Returns:
            Tuple of (success, message)
        """
        tweet_url = post.get('Tweet_URL', '')
        ambassador = post.get('Ambassador', 'Unknown')

        if not tweet_url:
            return False, "Missing tweet URL"

        try:
            logger.info(f"Scraping tweet from {ambassador}: {tweet_url}")

            # Scrape metrics
            metrics, message = self.scraper.scrape_tweet_metrics(
                tweet_url,
                timeout=self.config.x_scraper_timeout
            )

            if metrics:
                # Update sheets
                success, update_msg = self.sheets_service.update_x_post_metrics(tweet_url, metrics)

                if success:
                    self.consecutive_failures = 0
                    self.last_success_time = datetime.now()
                    self.total_success += 1

                    logger.info(f"â Successfully updated: {ambassador} - {metrics}")
                    return True, f"Success: {update_msg}"
                else:
                    # Sheet update failed (not a blocking error)
                    logger.warning(f"Sheet update failed: {update_msg}")
                    return False, f"Sheet update failed: {update_msg}"
            else:
                # Scraping failed
                self.consecutive_failures += 1

                # Check if it's a blocking error
                if self._is_blocking_error(message):
                    logger.error(f"â Blocking error detected: {message}")
                    return False, f"Blocking error: {message}"
                else:
                    logger.warning(f"â Scraping failed (non-blocking): {message}")
                    return False, f"Scraping failed: {message}"

        except Exception as e:
            self.consecutive_failures += 1
            error_msg = f"Exception during scraping: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return False, error_msg

    def process_current_month_tweets(self) -> Dict[str, int]:
        """
        Process all tweets from the current month

        Returns:
            Dictionary with processing statistics
        """
        logger.info("=" * 80)
        logger.info("Starting X scraper run for current month tweets")
        logger.info("=" * 80)

        # Initialize scraper
        self._init_scraper()

        # Get current month tweets
        posts = self.sheets_service.get_current_month_x_posts()

        if not posts:
            logger.info("No tweets found for current month")
            return {
                'total': 0,
                'success': 0,
                'failed': 0,
                'blocked': False
            }

        logger.info(f"Found {len(posts)} tweets to scrape from current month")

        # Reset counters
        self.total_processed = 0
        self.total_success = 0
        self.total_failed = 0

        # Process each tweet
        for i, post in enumerate(posts, 1):
            # Check for blocking before processing
            if self._is_blocked():
                logger.warning(f"Blocking detected after {i-1}/{len(posts)} tweets")
                self._wait_for_unblock()

                # Reinitialize scraper after wait
                logger.info("Reinitializing scraper after unblock wait")
                self._init_scraper()

            # Process tweet
            tweet_url = post.get('Tweet_URL', 'unknown')
            logger.info(f"\n[{i}/{len(posts)}] Processing: {tweet_url}")

            success, message = self._scrape_single_tweet(post)
            self.total_processed += 1

            if not success:
                self.total_failed += 1
                logger.error(f"Failed: {message}")
            else:
                logger.info(f"Success: {message}")

            # Add delay between requests (except after last one)
            if i < len(posts):
                # Randomize delay Â±20%
                delay = self.scrape_delay_seconds * random.uniform(0.8, 1.2)
                logger.debug(f"Waiting {delay:.1f}s before next request...")
                time.sleep(delay)

        # Close scraper
        if self.scraper:
            self.scraper.close_driver()

        # Log summary
        logger.info("=" * 80)
        logger.info("X Scraper Run Complete")
        logger.info(f"Total processed: {self.total_processed}")
        logger.info(f"Successful: {self.total_success}")
        logger.info(f"Failed: {self.total_failed}")
        logger.info(f"Success rate: {(self.total_success/self.total_processed*100) if self.total_processed > 0 else 0:.1f}%")
        logger.info("=" * 80)

        return {
            'total': self.total_processed,
            'success': self.total_success,
            'failed': self.total_failed,
            'blocked': self._is_blocked()
        }

    def run_once(self):
        """Run scraper once and exit"""
        try:
            stats = self.process_current_month_tweets()
            logger.info(f"Run complete: {stats}")
        except Exception as e:
            logger.error(f"Error during scraper run: {e}", exc_info=True)
        finally:
            if self.scraper:
                self.scraper.close_driver()

    def run_continuous(self):
        """
        Run scraper continuously on schedule

        Runs immediately on start, then repeats based on schedule_interval_minutes
        """
        logger.info(f"Starting continuous scraper with {self.schedule_interval_minutes} minute interval")

        while True:
            try:
                # Run scraper
                stats = self.process_current_month_tweets()

                # Calculate next run time
                # If many failures, extend interval
                if stats['failed'] > stats['success']:
                    interval = self.schedule_interval_minutes * 1.5
                    logger.info(f"Many failures detected, extending interval to {interval} minutes")
                else:
                    interval = self.schedule_interval_minutes

                next_run = datetime.now() + timedelta(minutes=interval)
                logger.info(f"Next run scheduled for: {next_run.strftime('%Y-%m-%d %H:%M:%S')}")
                logger.info(f"Sleeping for {interval} minutes...")

                time.sleep(interval * 60)

            except KeyboardInterrupt:
                logger.info("Received shutdown signal, exiting...")
                break
            except Exception as e:
                logger.error(f"Error in continuous run: {e}", exc_info=True)
                logger.info("Waiting 5 minutes before retry...")
                time.sleep(300)
            finally:
                if self.scraper:
                    try:
                        self.scraper.close_driver()
                    except:
                        pass


def main():
    """Main entry point"""
    import argparse

    parser = argparse.ArgumentParser(description='X/Twitter Scraper Scheduler')
    parser.add_argument(
        'mode',
        choices=['once', 'continuous'],
        help='Run mode: "once" for single run, "continuous" for scheduled runs'
    )

    args = parser.parse_args()

    scheduler = XScraperScheduler()

    if args.mode == 'once':
        logger.info("Running in ONE-TIME mode")
        scheduler.run_once()
    elif args.mode == 'continuous':
        logger.info("Running in CONTINUOUS mode")
        scheduler.run_continuous()


if __name__ == "__main__":
    main()
