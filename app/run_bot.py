#!/usr/bin/env python3
"""
Entry point for running the Discord bot independently.

Usage:
    python run_bot.py

Requires DISCORD_BOT_TOKEN environment variable to be set.
"""

import sys
import os
import logging

# Add app directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from dotenv import load_dotenv

# Load environment variables before importing bot
load_dotenv()

# Validate environment before import
if not os.getenv('DISCORD_BOT_TOKEN'):
    logging.error("DISCORD_BOT_TOKEN not set in environment")
    sys.exit(1)

from discord_bot import run_bot

if __name__ == '__main__':
    try:
        run_bot()
    except KeyboardInterrupt:
        print("\nBot interrupted by user")
        sys.exit(0)
    except Exception as e:
        logging.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)
