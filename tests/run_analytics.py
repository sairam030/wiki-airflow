#!/usr/bin/env python3
"""
Manual Analytics Runner
Run analytics independently from Airflow
Usage: python run_analytics.py [days]
"""

import sys
import os

# Add plugins to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'plugins'))

from utils.analytics_trends import run_analytics


if __name__ == "__main__":
    # Get number of days from command line (default 5)
    days = int(sys.argv[1]) if len(sys.argv) > 1 else 5
    
    print(f"\n🎯 Running analytics for last {days} days...")
    
    # Run analytics
    analytics_data = run_analytics(days=days)
    
    if analytics_data:
        print("\n✅ Analytics completed successfully!")
        print(f"\n💡 Query examples:")
        print(f"""
        -- Top growing articles
        SELECT page_display, views_change, views_change_pct 
        FROM wiki_trends_growth 
        ORDER BY views_change DESC 
        LIMIT 10;
        
        -- Top declining articles
        SELECT page_display, views_change, views_change_pct 
        FROM wiki_trends_growth 
        ORDER BY views_change ASC 
        LIMIT 10;
        
        -- Most volatile articles
        SELECT page_display, volatility, max_views, min_views
        FROM wiki_trends_growth 
        ORDER BY volatility DESC 
        LIMIT 10;
        
        -- Trending by category
        SELECT category, AVG(views_change_pct) as avg_growth
        FROM wiki_trends_growth 
        GROUP BY category 
        ORDER BY avg_growth DESC;
        
        -- Articles present all {days} days
        SELECT COUNT(*) FROM wiki_trends_common;
        
        -- Daily view trends for specific article
        SELECT date, views, rank
        FROM wiki_trends_timeseries
        WHERE page_display = 'Your Article Name'
        ORDER BY date;
        """)
    else:
        print("\n❌ Analytics failed - check error messages above")
        sys.exit(1)
