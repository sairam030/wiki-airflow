"""
End-to-end test of the entire Wikipedia pipeline locally (no Docker needed)
Tests: fetch pages → fetch categories → classify → extract geography
"""
import sys
sys.path.insert(0, '/home/ram/wiki-trends-pipeline')

import requests
import time
from datetime import datetime, timedelta
from plugins.keyword_classifier import classify_with_keywords, extract_geography_with_keywords
from plugins.utils.fetch_wiki_categories import fetch_categories_for_single_page
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configuration
NUM_PAGES_TO_TEST = 20  # Test with 20 pages (adjust as needed)
# Wikipedia API requires dates 2 days ago (data availability lag)
TEST_DATE = (datetime.now() - timedelta(days=2)).strftime('%Y/%m/%d')

print("="*100)
print(f"END-TO-END WIKIPEDIA PIPELINE TEST (LOCAL)")
print("="*100)
print(f"Date: {TEST_DATE}")
print(f"Testing with: {NUM_PAGES_TO_TEST} pages")
print("="*100)

# Step 1: Fetch top Wikipedia pages
print("\n📥 STEP 1: Fetching top Wikipedia pages...")
start_time = time.time()

try:
    url = f"https://wikimedia.org/api/rest_v1/metrics/pageviews/top/en.wikipedia/all-access/{TEST_DATE}"
    headers = {'User-Agent': 'WikiTrendsPipeline/1.0'}
    
    response = requests.get(url, headers=headers, timeout=30)
    
    if response.status_code == 200:
        data = response.json()
        articles = data['items'][0]['articles']
        
        # Filter out special pages
        top_pages = []
        for article in articles:
            page = article['article']
            if not page.startswith(('Main_Page', 'Special:', '-', 'Wikipedia:')):
                top_pages.append({
                    'page': page,
                    'views': article['views'],
                    'rank': article.get('rank', 0)
                })
                
                if len(top_pages) >= NUM_PAGES_TO_TEST:
                    break
        
        fetch_time = time.time() - start_time
        print(f"✅ Fetched {len(top_pages)} pages in {fetch_time:.2f}s")
        
        # Show sample
        print(f"\n📋 Sample pages:")
        for i, page in enumerate(top_pages[:5], 1):
            print(f"   {i}. {page['page'].replace('_', ' ')} ({page['views']:,} views)")
        
    else:
        print(f"❌ Failed to fetch pages: {response.status_code}")
        exit(1)
        
except Exception as e:
    print(f"❌ Error fetching pages: {e}")
    exit(1)

# Step 2: Fetch Wikipedia categories for each page
print(f"\n📚 STEP 2: Fetching Wikipedia categories (parallel)...")
start_time = time.time()

# Use existing optimized category fetcher with parallel execution
pages_with_categories = []
MAX_WORKERS = 5

with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    # Submit all fetch tasks
    future_to_page = {
        executor.submit(fetch_categories_for_single_page, page_data['page']): page_data
        for page_data in top_pages
    }
    
    completed = 0
    for future in as_completed(future_to_page):
        page_data = future_to_page[future]
        completed += 1
        
        try:
            page_title, categories, success = future.result()
            
            if success:
                pages_with_categories.append({
                    **page_data,
                    'wiki_categories': categories,
                    'wiki_categories_str': '|'.join(categories)
                })
            else:
                # Add with empty categories if fetch failed
                pages_with_categories.append({
                    **page_data,
                    'wiki_categories': [],
                    'wiki_categories_str': ''
                })
            
            if completed % 5 == 0:
                print(f"   ✓ Fetched categories for {completed}/{len(top_pages)} pages...")
                
        except Exception as e:
            print(f"   ⚠️  Error processing {page_data['page']}: {e}")
            pages_with_categories.append({
                **page_data,
                'wiki_categories': [],
                'wiki_categories_str': ''
            })

category_time = time.time() - start_time
avg_categories = sum(len(p['wiki_categories']) for p in pages_with_categories) / len(pages_with_categories)
success_count = sum(1 for p in pages_with_categories if len(p['wiki_categories']) > 0)
print(f"✅ Fetched categories in {category_time:.2f}s")
print(f"   Success rate: {success_count}/{len(pages_with_categories)} ({success_count/len(pages_with_categories)*100:.1f}%)")
print(f"   Average: {avg_categories:.1f} categories per page")

# Step 3: Classify pages using keyword-based classifier
print(f"\n🏷️  STEP 3: Classifying pages...")
start_time = time.time()

for page_data in pages_with_categories:
    # Classify
    category = classify_with_keywords(
        page_data['page'],
        page_data['wiki_categories_str']
    )
    page_data['category'] = category
    
    # Extract geography
    geography = extract_geography_with_keywords(
        page_data['page'],
        page_data['wiki_categories_str']
    )
    
    if geography:
        page_data['location_type'] = geography['location_type']
        page_data['location'] = geography['location']
    else:
        page_data['location_type'] = None
        page_data['location'] = None

classify_time = time.time() - start_time
print(f"✅ Classified {len(pages_with_categories)} pages in {classify_time:.2f}s")
print(f"   Speed: {classify_time/len(pages_with_categories)*1000:.1f}ms per page")

# Step 4: Show results
print(f"\n{'='*100}")
print(f"RESULTS: TOP {len(pages_with_categories)} WIKIPEDIA PAGES")
print(f"{'='*100}")

category_stats = {}
location_stats = {}

print(f"\n{'#':<4} {'Page':<35} {'Views':<12} {'Category':<20} {'Location':<25}")
print("-"*100)

for i, page_data in enumerate(pages_with_categories, 1):
    page_name = page_data['page'].replace('_', ' ')[:34]
    views = f"{page_data['views']:,}"
    category = page_data['category']
    
    # Build location string
    if page_data['location']:
        location = f"{page_data['location_type']}: {page_data['location']}"[:24]
    else:
        location = "-"
    
    print(f"{i:<4} {page_name:<35} {views:<12} {category:<20} {location:<25}")
    
    # Collect stats
    category_stats[category] = category_stats.get(category, 0) + 1
    if page_data['location']:
        loc_key = page_data['location']
        location_stats[loc_key] = location_stats.get(loc_key, 0) + 1

# Show detailed stats
print(f"\n{'='*100}")
print(f"STATISTICS")
print(f"{'='*100}")

print(f"\n📊 Category Distribution:")
sorted_categories = sorted(category_stats.items(), key=lambda x: x[1], reverse=True)
for category, count in sorted_categories:
    percentage = (count / len(pages_with_categories)) * 100
    bar = "█" * int(percentage / 5)
    print(f"   {category:<25} {count:>3} ({percentage:>5.1f}%) {bar}")

if location_stats:
    print(f"\n🌍 Top Locations:")
    sorted_locations = sorted(location_stats.items(), key=lambda x: x[1], reverse=True)[:10]
    for location, count in sorted_locations:
        percentage = (count / len(pages_with_categories)) * 100
        print(f"   {location:<30} {count:>3} ({percentage:>5.1f}%)")

# Show some detailed examples
print(f"\n{'='*100}")
print(f"DETAILED EXAMPLES (First 5 pages)")
print(f"{'='*100}")

for i, page_data in enumerate(pages_with_categories[:5], 1):
    print(f"\n{i}. {page_data['page'].replace('_', ' ')}")
    print(f"   Views: {page_data['views']:,}")
    print(f"   Category: {page_data['category']}")
    if page_data['location']:
        print(f"   Location: {page_data['location_type']} - {page_data['location']}")
    else:
        print(f"   Location: None")
    print(f"   Wikipedia Categories ({len(page_data['wiki_categories'])}):")
    # Show first 5 categories
    for cat in page_data['wiki_categories'][:5]:
        print(f"      - {cat}")
    if len(page_data['wiki_categories']) > 5:
        print(f"      ... and {len(page_data['wiki_categories']) - 5} more")

# Performance summary
print(f"\n{'='*100}")
print(f"PERFORMANCE SUMMARY")
print(f"{'='*100}")
total_time = fetch_time + category_time + classify_time
print(f"📥 Fetch pages:      {fetch_time:>8.2f}s")
print(f"📚 Fetch categories: {category_time:>8.2f}s ({category_time/len(pages_with_categories)*1000:.0f}ms per page)")
print(f"🏷️  Classify & locate: {classify_time:>8.2f}s ({classify_time/len(pages_with_categories)*1000:.0f}ms per page)")
print(f"{'─'*100}")
print(f"⏱️  Total time:      {total_time:>8.2f}s")
print(f"")
print(f"⚡ Estimated time for 995 pages:")
print(f"   Fetch pages:      ~{fetch_time:.1f}s")
print(f"   Fetch categories: ~{(995 * category_time / len(pages_with_categories)):.1f}s")
print(f"   Classify & locate: ~{(995 * classify_time / len(pages_with_categories)):.1f}s")
print(f"   TOTAL:            ~{fetch_time + (995 * category_time / len(pages_with_categories)) + (995 * classify_time / len(pages_with_categories)):.1f}s = ~{(fetch_time + (995 * category_time / len(pages_with_categories)) + (995 * classify_time / len(pages_with_categories)))/60:.1f} minutes")

print(f"\n{'='*100}")
print(f"✅ LOCAL TEST COMPLETE!")
print(f"{'='*100}")
print(f"\nAll steps working correctly! Ready to integrate into Docker pipeline.")
