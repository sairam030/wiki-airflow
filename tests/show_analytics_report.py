#!/usr/bin/env python3
"""
Analytics Visualization Script
Creates summary reports from analytics tables
"""

import psycopg2
import pandas as pd
from datetime import datetime


def connect_db():
    """Connect to PostgreSQL"""
    return psycopg2.connect(
        host="postgres",
        database="airflow",
        user="airflow",
        password="airflow",
        port=5432
    )


def print_section(title):
    """Print section header"""
    print("\n" + "="*80)
    print(f"  {title}")
    print("="*80 + "\n")


def get_top_growing():
    """Get top 10 growing articles"""
    conn = connect_db()
    
    query = """
    SELECT 
        page_display as Article,
        category as Category,
        first_views as "Day 1 Views",
        last_views as "Last Day Views",
        views_change as "Views Change",
        views_change_pct as "Change %"
    FROM wiki_trends_growth 
    ORDER BY views_change DESC 
    LIMIT 10;
    """
    
    df = pd.read_sql(query, conn)
    conn.close()
    
    return df


def get_top_declining():
    """Get top 10 declining articles"""
    conn = connect_db()
    
    query = """
    SELECT 
        page_display as Article,
        category as Category,
        first_views as "Day 1 Views",
        last_views as "Last Day Views",
        views_change as "Views Change",
        views_change_pct as "Change %"
    FROM wiki_trends_growth 
    ORDER BY views_change ASC 
    LIMIT 10;
    """
    
    df = pd.read_sql(query, conn)
    conn.close()
    
    return df


def get_most_volatile():
    """Get most volatile articles"""
    conn = connect_db()
    
    query = """
    SELECT 
        page_display as Article,
        category as Category,
        min_views as "Min Views",
        max_views as "Max Views",
        volatility as Volatility,
        avg_views as "Avg Views"
    FROM wiki_trends_growth 
    ORDER BY volatility DESC 
    LIMIT 10;
    """
    
    df = pd.read_sql(query, conn)
    conn.close()
    
    return df


def get_category_trends():
    """Get category-level trends"""
    conn = connect_db()
    
    query = """
    SELECT 
        category as Category,
        COUNT(*) as "Article Count",
        ROUND(AVG(views_change_pct), 2) as "Avg Growth %",
        SUM(CASE WHEN views_change > 0 THEN 1 ELSE 0 END) as Growing,
        SUM(CASE WHEN views_change < 0 THEN 1 ELSE 0 END) as Declining
    FROM wiki_trends_growth 
    GROUP BY category 
    ORDER BY "Avg Growth %" DESC;
    """
    
    df = pd.read_sql(query, conn)
    conn.close()
    
    return df


def get_sustained_trending():
    """Get articles present in all days"""
    conn = connect_db()
    
    query = """
    SELECT 
        page_display as Article,
        category as Category,
        ROUND(AVG(views), 0) as "Avg Views",
        ROUND(AVG(rank), 0) as "Avg Rank"
    FROM wiki_trends_common
    GROUP BY page_display, category
    ORDER BY "Avg Views" DESC
    LIMIT 15;
    """
    
    df = pd.read_sql(query, conn)
    conn.close()
    
    return df


def get_rank_improvements():
    """Get articles with best rank improvements"""
    conn = connect_db()
    
    query = """
    SELECT 
        page_display as Article,
        category as Category,
        first_rank as "Starting Rank",
        last_rank as "Ending Rank",
        rank_change as "Rank Change",
        views_change_pct as "Views Change %"
    FROM wiki_trends_growth 
    WHERE rank_change > 0
    ORDER BY rank_change DESC 
    LIMIT 10;
    """
    
    df = pd.read_sql(query, conn)
    conn.close()
    
    return df


def get_summary_stats():
    """Get overall summary statistics"""
    conn = connect_db()
    cursor = conn.cursor()
    
    # Get date range
    cursor.execute("""
        SELECT MIN(first_date), MAX(last_date), MAX(days_tracked)
        FROM wiki_trends_growth
    """)
    start_date, end_date, days = cursor.fetchone()
    
    # Get total articles
    cursor.execute("SELECT COUNT(DISTINCT page) FROM wiki_trends_timeseries")
    total_articles = cursor.fetchone()[0]
    
    # Get common articles
    cursor.execute("SELECT COUNT(DISTINCT page) FROM wiki_trends_common")
    common_articles = cursor.fetchone()[0]
    
    # Get total records
    cursor.execute("SELECT COUNT(*) FROM wiki_trends_timeseries")
    total_records = cursor.fetchone()[0]
    
    conn.close()
    
    return {
        'start_date': start_date,
        'end_date': end_date,
        'days': days,
        'total_articles': total_articles,
        'common_articles': common_articles,
        'total_records': total_records
    }


def generate_report():
    """Generate complete analytics report"""
    
    print("\n" + "╔" + "="*78 + "╗")
    print("║" + " "*20 + "WIKIPEDIA TRENDS ANALYTICS REPORT" + " "*25 + "║")
    print("╚" + "="*78 + "╝")
    
    # Summary stats
    try:
        stats = get_summary_stats()
        print(f"\n📊 Analysis Period: {stats['start_date']} to {stats['end_date']} ({stats['days']} days)")
        print(f"📈 Total Unique Articles: {stats['total_articles']:,}")
        print(f"🔄 Sustained Trending (all {stats['days']} days): {stats['common_articles']:,}")
        print(f"💾 Total Records Analyzed: {stats['total_records']:,}")
    except Exception as e:
        print(f"⚠️ Summary stats unavailable: {e}")
        print("   Make sure analytics tables are populated")
        return
    
    # Top growing
    print_section("📈 TOP 10 GROWING ARTICLES")
    try:
        df = get_top_growing()
        print(df.to_string(index=False))
    except Exception as e:
        print(f"⚠️ No data: {e}")
    
    # Top declining
    print_section("📉 TOP 10 DECLINING ARTICLES")
    try:
        df = get_top_declining()
        print(df.to_string(index=False))
    except Exception as e:
        print(f"⚠️ No data: {e}")
    
    # Most volatile
    print_section("🎢 TOP 10 MOST VOLATILE ARTICLES")
    try:
        df = get_most_volatile()
        print(df.to_string(index=False))
    except Exception as e:
        print(f"⚠️ No data: {e}")
    
    # Rank improvements
    print_section("🚀 TOP 10 RANK IMPROVEMENTS")
    try:
        df = get_rank_improvements()
        print(df.to_string(index=False))
    except Exception as e:
        print(f"⚠️ No data: {e}")
    
    # Category trends
    print_section("📊 CATEGORY-LEVEL TRENDS")
    try:
        df = get_category_trends()
        print(df.to_string(index=False))
    except Exception as e:
        print(f"⚠️ No data: {e}")
    
    # Sustained trending
    print_section(f"🔥 TOP 15 SUSTAINED TRENDING (Present All {stats['days']} Days)")
    try:
        df = get_sustained_trending()
        print(df.to_string(index=False))
    except Exception as e:
        print(f"⚠️ No data: {e}")
    
    print("\n" + "="*80)
    print(f"  Report Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80 + "\n")


if __name__ == "__main__":
    generate_report()
