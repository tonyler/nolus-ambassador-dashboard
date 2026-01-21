"""
Configuration loader utility
Loads and provides access to application configuration from config.json
"""

import json
import os
from typing import Dict, List, Any, Optional

class Config:
    """Configuration singleton"""
    _instance = None
    _config_data = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._load_config()
        return cls._instance

    def _load_config(self) -> None:
        """Load configuration from config.json"""
        config_path = os.path.join(os.path.dirname(__file__), 'config.json')

        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Configuration file not found: {config_path}")

        with open(config_path, 'r', encoding='utf-8') as f:
            self._config_data = json.load(f)

    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value by dot-notation key (e.g., 'discord.nolan_role_id')"""
        keys = key.split('.')
        value = self._config_data

        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default

        return value

    @property
    def ambassadors(self) -> List[str]:
        """Get list of ambassador names"""
        return self.get('ambassadors', [])

    @property
    def ambassador_mapping(self) -> Dict[str, str]:
        """Get Discord username to ambassador name mapping"""
        return self.get('ambassador_mapping', {})

    @property
    def excluded_months(self) -> List[tuple]:
        """Get list of excluded (year, month) tuples"""
        excluded = self.get('leaderboard.excluded_months', [])
        return [tuple(item) for item in excluded] if excluded else []

    @property
    def special_positioning(self) -> Dict[str, str]:
        """Get special positioning rules for leaderboard (e.g., {'Tony': 'bottom'})"""
        return self.get('leaderboard.special_positioning', {})

    @property
    def nolan_role_id(self) -> Optional[int]:
        """Get Discord Nolan role ID"""
        return self.get('discord.nolan_role_id')

    @property
    def x_content_sheet_id(self) -> str:
        """Get X content spreadsheet ID"""
        return self.get('spreadsheets.x_content_sheet_id', '')

    @property
    def reddit_content_sheet_id(self) -> str:
        """Get Reddit content spreadsheet ID"""
        return self.get('spreadsheets.reddit_content_sheet_id', '')

    @property
    def cache_ttl(self) -> int:
        """Get cache TTL in seconds"""
        return self.get('cache.ttl_seconds', 300)

    @property
    def reddit_retry_attempts(self) -> int:
        """Get number of retry attempts for Reddit API"""
        return self.get('reddit_api.retry_attempts', 3)

    @property
    def reddit_retry_delay(self) -> int:
        """Get retry delay in seconds for Reddit API"""
        return self.get('reddit_api.retry_delay_seconds', 2)

    @property
    def x_scraper_schedule_interval(self) -> int:
        """Get X scraper schedule interval in minutes"""
        return self.get('x_scraper.schedule_interval_minutes', 1440)

    @property
    def x_scraper_delay(self) -> int:
        """Get delay between scraping requests in seconds"""
        return self.get('x_scraper.scrape_delay_seconds', 5)

    @property
    def x_scraper_timeout(self) -> int:
        """Get page load timeout for scraper in seconds"""
        return self.get('x_scraper.page_timeout_seconds', 15)

    @property
    def x_scraper_max_failures(self) -> int:
        """Get max consecutive failures before blocking detection"""
        return self.get('x_scraper.max_consecutive_failures', 5)

    @property
    def x_scraper_blocking_base_wait(self) -> int:
        """Get base wait time in minutes when blocking detected"""
        return self.get('x_scraper.blocking_base_wait_minutes', 30)

    @property
    def x_scraper_blocking_max_wait(self) -> int:
        """Get max wait time in hours when blocking detected"""
        return self.get('x_scraper.blocking_max_wait_hours', 8)

    @property
    def x_scraper_current_month_only(self) -> bool:
        """Whether to scrape only current month tweets"""
        return self.get('x_scraper.scrape_current_month_only', True)

    @property
    def x_scraper_cookie_file(self) -> Optional[str]:
        """Get X scraper cookie file path (relative to app directory)"""
        return self.get('x_scraper.cookie_file')

    def reload(self) -> None:
        """Reload configuration from file"""
        self._load_config()


def get_config() -> Config:
    """Get configuration instance"""
    return Config()
