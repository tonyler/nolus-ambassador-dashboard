"""
Flask Ambassador Dashboard - Main application file
"""

import os
import logging
from datetime import datetime

from flask import Flask, render_template, request, jsonify, redirect, url_for, flash
from dotenv import load_dotenv
from werkzeug.middleware.proxy_fix import ProxyFix

from sheets_service import SheetsService
from config_loader import get_config

load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)-8s] [%(name)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)
# Add ProxyFix to handle X-Forwarded-* headers from nginx
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_prefix=1)

# Get secret key from environment or generate a secure warning
secret_key = os.getenv('FLASK_SECRET_KEY')
if not secret_key:
    logger.warning("FLASK_SECRET_KEY not set in environment! Using insecure default for development only.")
    secret_key = 'dev-secret-key-change-in-production'
app.secret_key = secret_key

# Initialize configuration and services
config = get_config()
sheets_service = SheetsService()

logger.info("Flask application initialized")

def get_selected_month():
    """Helper to get selected month from query params or current month"""
    current = datetime.now()
    if request.args.get('year') and request.args.get('month'):
        return request.args.get('year', type=int), request.args.get('month', type=int)
    return current.year, current.month

@app.route('/')
def index():
    """Main dashboard - redirect to X leaderboard"""
    return redirect(url_for('x_leaderboard'))

@app.route('/x-leaderboard')
def x_leaderboard():
    """X/Twitter leaderboard page"""
    try:
        selected_year, selected_month = get_selected_month()
        current_month = datetime.now()

        leaderboard, total_impressions_all = sheets_service.get_x_leaderboard(selected_year, selected_month)

        return render_template(
            'x_leaderboard.html',
            leaderboard=leaderboard,
            total_impressions=total_impressions_all,
            total_posts=sum(amb['tweets'] for amb in leaderboard),
            active_ambassadors=len(leaderboard),
            available_months=sheets_service.get_available_months(),
            selected_year=selected_year,
            selected_month=selected_month,
            current_year=current_month.year,
            current_month_num=current_month.month,
            daily_stats=sheets_service.get_x_daily_stats(selected_year, selected_month)
        )
    except Exception as e:
        logger.error(f"Error rendering X leaderboard: {e}", exc_info=True)
        flash(f"Error loading leaderboard: {str(e)}", 'error')
        return redirect(url_for('index'))

@app.route('/reddit-leaderboard')
def reddit_leaderboard():
    """Reddit leaderboard page"""
    try:
        selected_year, selected_month = get_selected_month()
        current_month = datetime.now()

        leaderboard = sheets_service.get_reddit_leaderboard(selected_year, selected_month)

        return render_template(
            'reddit_leaderboard.html',
            leaderboard=leaderboard,
            total_score=sum(amb['total_score'] for amb in leaderboard),
            total_posts=sum(amb['posts'] for amb in leaderboard),
            total_comments=sum(amb['total_comments'] for amb in leaderboard),
            total_views=sum(amb['total_views'] for amb in leaderboard),
            available_months=sheets_service.get_available_months(),
            selected_year=selected_year,
            selected_month=selected_month,
            current_year=current_month.year,
            current_month_num=current_month.month,
            daily_stats=sheets_service.get_reddit_daily_stats(selected_year, selected_month)
        )
    except Exception as e:
        logger.error(f"Error rendering Reddit leaderboard: {e}", exc_info=True)
        flash(f"Error loading leaderboard: {str(e)}", 'error')
        return redirect(url_for('index'))

@app.route('/total-leaderboard')
def total_leaderboard():
    """Total combined leaderboard page"""
    try:
        selected_year, selected_month = get_selected_month()
        current_month = datetime.now()

        leaderboard = sheets_service.get_total_leaderboard(selected_year, selected_month)

        return render_template(
            'total_leaderboard.html',
            leaderboard=leaderboard,
            total_x_views=sum(amb['x_views'] for amb in leaderboard),
            total_reddit_views=sum(amb['reddit_views'] for amb in leaderboard),
            total_combined_views=sum(amb['total_views'] for amb in leaderboard),
            available_months=sheets_service.get_available_months(),
            selected_year=selected_year,
            selected_month=selected_month,
            current_year=current_month.year,
            current_month_num=current_month.month,
            daily_stats=sheets_service.get_daily_impressions_for_graph(selected_year, selected_month)
        )
    except Exception as e:
        logger.error(f"Error rendering total leaderboard: {e}", exc_info=True)
        flash(f"Error loading leaderboard: {str(e)}", 'error')
        return redirect(url_for('index'))

@app.route('/api/refresh-reddit', methods=['POST'])
def refresh_reddit():
    """API endpoint to refresh Reddit stats"""
    try:
        year = request.json.get('year') if request.json else None
        month = request.json.get('month') if request.json else None

        logger.info(f"Reddit refresh requested for {year}/{month}")
        success, message = sheets_service.update_reddit_stats(year, month)

        if success:
            logger.info(f"Reddit stats refreshed successfully: {message}")
        else:
            logger.warning(f"Reddit stats refresh failed: {message}")

        return jsonify({'success': success, 'message': message})
    except Exception as e:
        logger.error(f"Error refreshing Reddit stats: {e}", exc_info=True)
        return jsonify({'success': False, 'message': f"Error: {str(e)}"})

@app.route('/api/clear-cache', methods=['POST'])
def clear_cache():
    """API endpoint to clear all caches"""
    try:
        logger.info("Cache clear requested")
        sheets_service._invalidate_cache()
        logger.info("Cache cleared successfully")
        return jsonify({'success': True, 'message': 'Cache cleared successfully'})
    except Exception as e:
        logger.error(f"Error clearing cache: {e}", exc_info=True)
        return jsonify({'success': False, 'message': f"Error: {str(e)}"})

@app.template_filter('month_name')
def month_name_filter(month_num):
    """Template filter to convert month number to name"""
    return datetime(2000, month_num, 1).strftime('%B')

if __name__ == '__main__':
    # For development
    app.run(host='0.0.0.0', port=5000, debug=True)
