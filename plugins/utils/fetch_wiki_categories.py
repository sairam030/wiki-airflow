"""
Task to fetch Wikipedia categories for pages
Fetches categories in parallel with retry logic for reliability
"""

from datetime import datetime, timedelta
import pandas as pd
from io import StringIO
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import BRONZE_BUCKET, create_s3_client, ensure_bucket_exists


WIKIPEDIA_API_URL = "https://en.wikipedia.org/w/api.php"
REQUEST_TIMEOUT = 15
MAX_RETRIES = 3  # Retry failed requests
MAX_WORKERS = 5  # Parallel workers (not too many to avoid rate limiting)


def fetch_categories_for_single_page(page_title, retry_count=0):
    """
    Fetch Wikipedia categories for a single page with retry logic
    
    Args:
        page_title: Wikipedia page title (with underscores)
        retry_count: Current retry attempt
    
    Returns:
        Tuple: (page_title, list of category names, success_flag)
    """
    # Convert underscores to spaces
    normalized_title = page_title.replace("_", " ")
    
    try:
        headers = {
            'User-Agent': 'WikiTrendsPipeline/1.0 (Educational Project; Python/requests)'
        }
        
        response = requests.get(
            WIKIPEDIA_API_URL,
            params={
                "action": "query",
                "titles": normalized_title,
                "prop": "categories",
                "cllimit": "max",       # Get all categories (up to 500)
                "format": "json",
                "redirects": 1          # Follow redirects
            },
            headers=headers,
            timeout=REQUEST_TIMEOUT
        )
        
        if response.status_code != 200:
            if retry_count < MAX_RETRIES:
                time.sleep(1)  # Wait before retry
                return fetch_categories_for_single_page(page_title, retry_count + 1)
            return (page_title, [], False)
        
        data = response.json()
        pages = data.get("query", {}).get("pages", {})
        
        for page_id, page_data in pages.items():
            if page_id == "-1":  # Page not found
                return (page_title, [], True)  # Success but no categories
            
            # Extract category names (remove "Category:" prefix)
            if "categories" in page_data:
                categories = [
                    cat.get("title", "").replace("Category:", "")
                    for cat in page_data["categories"]
                ]
                return (page_title, categories, True)
        
        return (page_title, [], True)
        
    except requests.exceptions.Timeout:
        if retry_count < MAX_RETRIES:
            print(f"⚠️  Timeout for {page_title}, retrying ({retry_count + 1}/{MAX_RETRIES})...")
            time.sleep(2)  # Wait longer before retry
            return fetch_categories_for_single_page(page_title, retry_count + 1)
        print(f"❌ Failed after {MAX_RETRIES} retries: {page_title}")
        return (page_title, [], False)
    except Exception as e:
        if retry_count < MAX_RETRIES:
            print(f"⚠️  Error for {page_title}: {str(e)}, retrying ({retry_count + 1}/{MAX_RETRIES})...")
            time.sleep(1)
            return fetch_categories_for_single_page(page_title, retry_count + 1)
        print(f"❌ Failed after {MAX_RETRIES} retries: {page_title} - {str(e)}")
        return (page_title, [], False)


def fetch_wikipedia_categories_for_pages(**context):
    """
    Read pages from Bronze bucket and fetch Wikipedia categories in PARALLEL
    with retry logic to ensure NO DATA LOSS
    """
    # Calculate yesterday's date
    yesterday = datetime.now() - timedelta(days=1)
    filename = f"top_pages_{yesterday.strftime('%Y_%m_%d')}.csv"
    categories_filename = f"wiki_categories_{yesterday.strftime('%Y_%m_%d')}.csv"
    
    print(f"📥 Fetching Wikipedia categories for: {filename}")
    
    # Read pages from Bronze bucket
    s3_client = create_s3_client()
    
    try:
        obj = s3_client.get_object(Bucket=BRONZE_BUCKET, Key=filename)
        df = pd.read_csv(obj['Body'])
        print(f"✓ Loaded {len(df)} pages from Bronze bucket")
    except Exception as e:
        print(f"❌ Could not read {filename}: {str(e)}")
        raise
    
    # Get page titles
    page_titles = df['article'].tolist()
    
    # ========== Fetch Wikipedia categories in PARALLEL with RETRY LOGIC ==========
    print("\n" + "="*70)
    print("🌐 FETCHING WIKIPEDIA CATEGORIES (Parallel with Retry)")
    print("="*70)
    print(f"   Total pages: {len(page_titles)}")
    print(f"   Parallel workers: {MAX_WORKERS}")
    print(f"   Max retries per page: {MAX_RETRIES}")
    print(f"   Progress updates every 100 pages")
    print()
    
    wiki_categories_dict = {}
    failed_pages = []
    completed = 0
    pages_with_cats = 0
    
    start_time = time.time()
    
    # Use ThreadPoolExecutor for parallel fetching
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit all tasks
        future_to_page = {
            executor.submit(fetch_categories_for_single_page, page): page
            for page in page_titles
        }
        
        # Process completed tasks
        for future in as_completed(future_to_page):
            page = future_to_page[future]
            try:
                page_title, categories, success = future.result()
                wiki_categories_dict[page_title] = categories
                
                if not success:
                    failed_pages.append(page_title)
                elif categories:
                    pages_with_cats += 1
                
                completed += 1
                
                # Progress updates
                if completed % 100 == 0 or completed == len(page_titles):
                    elapsed = time.time() - start_time
                    avg_time = elapsed / completed
                    remaining = (len(page_titles) - completed) * avg_time
                    
                    print(f"   ✓ Progress: {completed}/{len(page_titles)} ({(completed/len(page_titles))*100:.1f}%)")
                    print(f"      • Elapsed: {elapsed:.1f}s | Avg: {avg_time:.3f}s/page | ETA: {remaining:.1f}s")
                    print(f"      • Pages with categories: {pages_with_cats}")
                    print(f"      • Failed pages: {len(failed_pages)}")
                    
            except Exception as e:
                print(f"❌ Unexpected error processing {page}: {str(e)}")
                wiki_categories_dict[page] = []
                failed_pages.append(page)
                completed += 1
    
    elapsed_time = time.time() - start_time
    avg_categories = sum(len(cats) for cats in wiki_categories_dict.values()) / len(page_titles) if page_titles else 0
    
    # ========== RETRY FAILED PAGES (if any) ==========
    if failed_pages:
        print(f"\n⚠️  Retrying {len(failed_pages)} failed pages sequentially...")
        for idx, page in enumerate(failed_pages, 1):
            page_title, categories, success = fetch_categories_for_single_page(page, retry_count=0)
            wiki_categories_dict[page_title] = categories
            if categories:
                pages_with_cats += 1
            if idx % 10 == 0 or idx == len(failed_pages):
                print(f"   Retry progress: {idx}/{len(failed_pages)}")
    
    # Show final stats
    print("\n📊 Wikipedia Categories Summary:")
    print(f"   • Total pages: {len(page_titles)}")
    print(f"   • Successfully fetched: {len(wiki_categories_dict)} ({(len(wiki_categories_dict)/len(page_titles))*100:.1f}%)")
    print(f"   • Pages with categories: {pages_with_cats} ({(pages_with_cats/len(page_titles))*100:.1f}%)")
    print(f"   • Avg categories per page: {avg_categories:.1f}")
    print(f"   • Total time: {elapsed_time:.1f}s ({elapsed_time/60:.1f} minutes)")
    print(f"   • Avg time per page: {elapsed_time/len(page_titles):.3f}s")
    print(f"   • Failed pages (after retries): {len([p for p in failed_pages if not wiki_categories_dict.get(p)])}")
    
    # Verify we have data for ALL pages
    missing_pages = [p for p in page_titles if p not in wiki_categories_dict]
    if missing_pages:
        print(f"\n⚠️  WARNING: {len(missing_pages)} pages missing from results!")
        for page in missing_pages:
            wiki_categories_dict[page] = []  # Add empty entry
    
    # Create DataFrame with page and categories
    categories_df = pd.DataFrame([
        {
            'page': page,
            'wiki_categories': '|'.join(wiki_categories_dict.get(page, [])) if wiki_categories_dict.get(page) else ''
        }
        for page in page_titles  # Maintain original order
    ])
    
    # Verify counts match
    print(f"\n✅ Data Integrity Check:")
    print(f"   • Input pages: {len(page_titles)}")
    print(f"   • Output pages: {len(categories_df)}")
    print(f"   • Match: {'✓ YES' if len(page_titles) == len(categories_df) else '✗ NO - DATA LOSS!'}")
    
    # Save to Bronze bucket
    csv_buffer = StringIO()
    categories_df.to_csv(csv_buffer, index=False)
    
    s3_client.put_object(
        Bucket=BRONZE_BUCKET,
        Key=categories_filename,
        Body=csv_buffer.getvalue(),
        ContentType='text/csv'
    )
    
    print(f"\n✅ Saved Wikipedia categories to: s3://{BRONZE_BUCKET}/{categories_filename}")
    print("="*70)
    
    return {
        'categories_file': categories_filename,
        'total_pages': len(page_titles),
        'output_pages': len(categories_df),
        'pages_with_categories': pages_with_cats,
        'avg_categories': round(avg_categories, 1),
        'fetch_time_seconds': round(elapsed_time, 1)
    }


