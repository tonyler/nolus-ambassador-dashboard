# Nolus Ambassador Dashboard - Application Structure

A Python Flask web application for tracking community ambassador engagement metrics across X/Twitter and Reddit.

## Tech Stack

| Component | Technology |
|-----------|------------|
| Framework | Flask 3.0+ |
| Database | SQLite3 |
| Frontend | Bootstrap 5 + Chart.js |
| X Scraping | Selenium |
| Reddit Scraping | BeautifulSoup |
| Production Server | Gunicorn |

## Directory Structure

```
/root/nolus/
├── app/
│   ├── app.py                    # Flask entry point, routes
│   ├── config.json               # Ambassadors, scraper settings
│   ├── config_loader.py          # Singleton config reader
│   ├── db_service.py             # SQLite CRUD operations
│   ├── sheets_service.py         # Public service interface
│   ├── local_data_service.py     # Business logic, leaderboards
│   ├── x_scraper.py              # Selenium X/Twitter scraper
│   ├── x_scraper_scheduler.py    # Automated scraping scheduler
│   ├── reddit_scraper.py         # Reddit metrics scraper
│   ├── record_daily_snapshot.py  # Daily cron script
│   ├── templates/
│   │   ├── base.html             # Base layout
│   │   ├── submit.html           # Content submission form
│   │   ├── x_leaderboard.html    # X leaderboard
│   │   ├── reddit_leaderboard.html
│   │   └── total_leaderboard.html
│   └── data/
│       └── nolus_ambassador.db   # SQLite database
├── .env.example                  # Environment template
└── requirements.txt              # Python dependencies
```

## Core Components

### Data Layer

| File | Purpose |
|------|---------|
| `db_service.py` | SQLite abstraction with parameterized queries |
| `local_data_service.py` | Leaderboard computation, caching, aggregation |
| `sheets_service.py` | Service wrapper for app interface |

### Scrapers

| File | Purpose |
|------|---------|
| `x_scraper.py` | Selenium-based X scraper with anti-detection |
| `x_scraper_scheduler.py` | Scheduler with blocking detection & backoff |
| `reddit_scraper.py` | HTTP + BeautifulSoup Reddit scraper |

### Configuration

| File | Purpose |
|------|---------|
| `config.json` | Ambassadors list, sheet IDs, scraper params |
| `config_loader.py` | Singleton pattern config access |
| `.env` | Secrets (Flask key, credentials) |

## Database Schema

### x_posts
```sql
id, ambassador, tweet_url, tweet_id, impressions,
likes, retweets, replies, date_posted, month, year, last_updated
```

### reddit_posts
```sql
id, ambassador, url, post_id, score, comments,
views, date_posted, month, year, last_updated
```

### snapshots
```sql
id, date, x_impressions, x_likes, x_retweets, x_replies,
x_posts, reddit_score, reddit_comments, reddit_views,
reddit_posts, month, year, last_updated
```

## Routes

| Route | Method | Purpose |
|-------|--------|---------|
| `/` | GET | Redirect to submit |
| `/submit` | GET/POST | Content submission form |
| `/x-leaderboard` | GET | X/Twitter leaderboard |
| `/reddit-leaderboard` | GET | Reddit leaderboard |
| `/total-leaderboard` | GET | Combined leaderboard |
| `/api/refresh-reddit` | POST | Trigger Reddit refresh |
| `/api/clear-cache` | POST | Clear cached data |

## Architecture Patterns

1. **Service-Oriented** - Clear separation: SheetsService → LocalDataService → DatabaseService
2. **Singleton Config** - Centralized configuration management
3. **Thread-Safe Caching** - TTL-based cache with locks
4. **Daily Snapshots** - Historical metric tracking via cron

## Running the App

```bash
# Development
cd app && python app.py

# Production
gunicorn -w 4 app:app

# Daily snapshots (cron)
python record_daily_snapshot.py

# Automated X scraping
python x_scraper_scheduler.py
```

## Ambassadors

Tony, Emlanis, Sir Thanos, Martinezz, Beltein, Odi, Frifalin, BlackOwl
