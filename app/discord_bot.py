"""
Discord Bot for Nolus Ambassador Dashboard

Monitors specific Discord channels for X and Reddit links,
extracts them, and saves to the database using LocalDataService.
"""

import os
import re
import sys
import logging
from collections import defaultdict
from datetime import datetime
from typing import Optional, Tuple, List, Dict

import discord
from discord.ext import commands

# Add app directory to path for imports
APP_DIR = os.path.dirname(os.path.abspath(__file__))
if APP_DIR not in sys.path:
    sys.path.insert(0, APP_DIR)

from local_data_service import LocalDataService
from db_service import DatabaseService
from config_loader import get_config

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)-8s] [%(name)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Load configuration
config = get_config()


def _validate_discord_config() -> Tuple[int, int]:
    """Validate Discord configuration and return channel IDs.

    Returns:
        Tuple of (x_channel_id, reddit_channel_id)

    Raises:
        ValueError: If configuration is invalid or missing
    """
    x_channel_id = config.get('discord.x_channel_id')
    reddit_channel_id = config.get('discord.reddit_channel_id')

    # Required fields
    if x_channel_id is None:
        raise ValueError("Missing required config field: discord.x_channel_id")
    if reddit_channel_id is None:
        raise ValueError("Missing required config field: discord.reddit_channel_id")

    # Validate types
    if not isinstance(x_channel_id, int):
        raise ValueError("discord.x_channel_id must be an integer")

    if not isinstance(reddit_channel_id, int):
        raise ValueError("discord.reddit_channel_id must be an integer")

    return x_channel_id, reddit_channel_id


# Validate config and get channel IDs
try:
    X_CHANNEL_ID, REDDIT_CHANNEL_ID = _validate_discord_config()
except ValueError as e:
    logger.error(f"Invalid Discord configuration: {e}")
    raise

logger.info(f"Discord channels configured - X: {X_CHANNEL_ID}, Reddit: {REDDIT_CHANNEL_ID}")

# URL patterns with length limits for safety
X_URL_PATTERN = re.compile(r'https?://(?:www\.)?(?:twitter\.com|x\.com)/\w{1,50}/status/\d{10,20}')
REDDIT_URL_PATTERN = re.compile(r'https?://(?:www\.)?(?:reddit\.com/r/\w{1,50}/comments/\w{5,10}|redd\.it/\w{5,10})(?:[/?#][^\s]*)?')


class NolusBot(commands.Bot):
    """Discord bot for handling ambassador content submissions."""

    def __init__(self):
        """Initialize the Discord bot with required intents."""
        intents = discord.Intents.default()
        intents.message_content = True
        intents.messages = True
        intents.guilds = True

        super().__init__(
            command_prefix='!',
            intents=intents,
            description='Nolus Ambassador Content Bot',
            case_insensitive=True,
            help_command=None
        )

        # Initialize services
        self.db_service = DatabaseService()
        self.local_service = LocalDataService(self.db_service)
        self.ambassador_mapping = config.ambassador_mapping

        if not self.ambassador_mapping:
            logger.warning("No ambassador mappings configured - all Discord submissions will fail")

        # Rate limiting: track submission timestamps per user
        self.user_submission_timestamps: Dict[int, List[float]] = defaultdict(list)
        self.submissions_per_hour = 20
        self.rate_limit_window = 3600  # 1 hour in seconds

    def _check_rate_limit(self, user_id: int) -> Tuple[bool, Optional[str]]:
        """Check if user has exceeded rate limits.

        Args:
            user_id: Discord user ID

        Returns:
            Tuple of (is_allowed, error_message)
        """
        now = datetime.now().timestamp()
        timestamps = self.user_submission_timestamps[user_id]

        # Remove old timestamps outside the window
        timestamps[:] = [ts for ts in timestamps if now - ts < self.rate_limit_window]

        if len(timestamps) >= self.submissions_per_hour:
            return False, f"Rate limit exceeded. Max {self.submissions_per_hour} submissions per hour."

        timestamps.append(now)
        return True, None

    def _extract_urls(self, content: str, platform: str) -> List[str]:
        """Extract URLs from message content based on platform.

        Args:
            content: Message content
            platform: 'x' or 'reddit'

        Returns:
            List of extracted URLs
        """
        pattern = X_URL_PATTERN if platform == 'x' else REDDIT_URL_PATTERN
        return pattern.findall(content)

    def _resolve_ambassador(self, display_name: str) -> Tuple[Optional[str], Optional[str]]:
        """Resolve Discord display name to ambassador name.

        Args:
            display_name: Discord user's display name

        Returns:
            Tuple of (ambassador_name, error_message)
        """
        # Validate input
        if not display_name or not isinstance(display_name, str):
            return None, "Invalid display name provided"

        if len(display_name) > 100:
            return None, "Display name too long"

        # Normalize the display name for lookup
        normalized = display_name.lower().strip()

        if not normalized:
            return None, "Display name cannot be empty"

        # Look up in ambassador mapping
        if normalized in self.ambassador_mapping:
            ambassador = self.ambassador_mapping[normalized]
            if not isinstance(ambassador, str) or len(ambassador) > 100:
                return None, "Invalid ambassador mapping configuration"
            return ambassador, None

        # Try without spaces
        normalized_no_spaces = normalized.replace(' ', '')
        if normalized_no_spaces in self.ambassador_mapping:
            ambassador = self.ambassador_mapping[normalized_no_spaces]
            if not isinstance(ambassador, str) or len(ambassador) > 100:
                return None, "Invalid ambassador mapping configuration"
            return ambassador, None

        return None, f"Unknown ambassador: **{display_name}**. Please contact an admin to add your Discord name to the mapping."

    async def on_ready(self):
        """Called when bot is ready and connected."""
        logger.info(f'Bot logged in as {self.user.name} (ID: {self.user.id})')
        logger.info(f'Connected to {len(self.guilds)} guilds')
        logger.info(f'Monitoring X channel: {X_CHANNEL_ID}')
        logger.info(f'Monitoring Reddit channel: {REDDIT_CHANNEL_ID}')

    async def on_error(self, event, *args, **kwargs):
        """Log errors and continue running."""
        logger.error(f"Error in {event}", exc_info=True)

    async def close(self):
        """Graceful shutdown."""
        logger.info("Bot shutting down...")
        await super().close()

    async def on_message(self, message: discord.Message):
        """Handle incoming messages."""
        try:
            # Ignore bot's own messages
            if message.author.bot:
                return

            # Check if message is in monitored channels
            if message.channel.id not in (X_CHANNEL_ID, REDDIT_CHANNEL_ID):
                return

            # Check rate limit
            is_allowed, rate_error = self._check_rate_limit(message.author.id)
            if not is_allowed:
                try:
                    await message.reply(rate_error)
                except discord.DiscordException as e:
                    logger.error(f"Failed to send rate limit message: {e}")
                return

            # Determine expected platform based on channel
            is_x_channel = message.channel.id == X_CHANNEL_ID
            platform = 'X' if is_x_channel else 'Reddit'
            platform_key = 'x' if is_x_channel else 'reddit'

            # Extract URLs based on channel type
            urls = self._extract_urls(message.content, platform_key)

            if not urls:
                # No relevant URLs found, skip silently
                return

            # Get ambassador from Discord display name
            ambassador, error = self._resolve_ambassador(message.author.display_name)
            if error:
                try:
                    await message.reply(error)
                except discord.DiscordException as e:
                    logger.error(f"Failed to send error reply: {e}")
                return

            # Process each URL
            results = []
            for url in urls:
                success, msg = self.local_service.add_content(ambassador, url)
                results.append((url, success, msg))
                log_level = logging.INFO if success else logging.WARNING
                logger.log(log_level, f"Processed URL from {ambassador}: {url} - {'Success' if success else 'Failed'}: {msg}")

            # Build response message
            response_lines = []
            for url, success, msg in results:
                if success:
                    response_lines.append(f"Added {platform} post for **{ambassador}**")
                else:
                    response_lines.append(f"Failed to add post: {msg}")

            response = '\n'.join(response_lines)
            try:
                await message.reply(f"{response}\n\n@everyone")
            except discord.DiscordException as e:
                logger.error(f"Failed to send response reply: {e}")

        except Exception as e:
            logger.error(f"Unexpected error in on_message: {e}", exc_info=True)


def run_bot():
    """Run the Discord bot."""
    token = os.getenv('DISCORD_BOT_TOKEN')
    if not token:
        logger.error("DISCORD_BOT_TOKEN environment variable not set!")
        raise ValueError("DISCORD_BOT_TOKEN environment variable is required")

    bot = NolusBot()
    bot.run(token)


if __name__ == '__main__':
    run_bot()
