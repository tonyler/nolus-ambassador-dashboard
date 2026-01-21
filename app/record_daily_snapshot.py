#!/usr/bin/env python3
"""
Daily Snapshot Recording Script

This script records a daily snapshot of metrics to Google Sheets.
Should be run via cron once per day (e.g., at midnight or after scrapers complete).

Example crontab entry (run at 11:59 PM daily):
59 23 * * * cd /root/flyfix/nolus/app && /root/flyfix/nolus/app/venv/bin/python3 record_daily_snapshot.py >> /var/log/snapshot.log 2>&1
"""

import os
import sys
import logging
from datetime import datetime
from dotenv import load_dotenv

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from sheets_service import SheetsService

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)-8s] [%(name)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def main():
    """Main function to record daily snapshot"""
    try:
        logger.info("=" * 60)
        logger.info("Starting daily snapshot recording")
        logger.info(f"Timestamp: {datetime.now().isoformat()}")

        # Load environment variables
        load_dotenv()

        # Initialize sheets service
        sheets_service = SheetsService()

        # Record the snapshot
        success, message = sheets_service.record_daily_snapshot()

        if success:
            logger.info(f"â Success: {message}")
            return 0
        else:
            logger.error(f"â Failed: {message}")
            return 1

    except Exception as e:
        logger.error(f"â Error recording daily snapshot: {e}", exc_info=True)
        return 1
    finally:
        logger.info("Daily snapshot recording completed")
        logger.info("=" * 60)

if __name__ == "__main__":
    sys.exit(main())
