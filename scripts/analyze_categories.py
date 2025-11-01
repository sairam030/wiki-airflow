"""
Analyze Wikipedia categories to improve keyword classifier
This script reads actual Wikipedia categories and suggests patterns to add
"""

import boto3
import csv
from io import StringIO
from collections import Counter
import re

# Create S3 client
s3 = boto3.client(
    's3',
    endpoint_url='http://minio:9000',
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadmin',
    region_name='us-east-1'
)

# Read categories file
print("📖 Reading Wikipedia categories from Bronze bucket...")
response = s3.get_object(Bucket='bronze', Key='wiki_categories_2025_10_31.csv')
content = response['Body'].read().decode('utf-8')

reader = csv.DictReader(StringIO(content))
rows = list(reader)

print(f"✓ Loaded {len(rows)} pages\n")

# Read categorized results to see what was classified as "Other"
print("📖 Reading classification results from Gold bucket...")
response_gold = s3.get_object(Bucket='gold', Key='categorized_2025_10_31.csv')
content_gold = response_gold['Body'].read().decode('utf-8')

# Find the actual CSV file (Spark outputs to a directory)
try:
    response_gold_list = s3.list_objects_v2(Bucket='gold', Prefix='categorized_2025_10_31.csv')
    csv_files = [obj['Key'] for obj in response_gold_list.get('Contents', []) 
                 if obj['Key'].endswith('.csv') and 'part-' in obj['Key']]
    if csv_files:
        response_gold = s3.get_object(Bucket='gold', Key=csv_files[0])
        content_gold = response_gold['Body'].read().decode('utf-8')
except:
    pass

reader_gold = csv.DictReader(StringIO(content_gold))
classified = {row['page']: row['category'] for row in reader_gold}

print(f"✓ Loaded {len(classified)} classified pages\n")

# Analyze pages classified as "Other"
other_pages = [page for page, cat in classified.items() if cat == 'Other']

print(f"📊 Found {len(other_pages)} pages classified as 'Other' ({len(other_pages)/len(classified)*100:.1f}%)\n")

# Collect all Wikipedia categories from "Other" pages
other_categories = Counter()
for row in rows:
    page = row['page']
    if page in other_pages:
        cats = row.get('wiki_categories', '').split('|')
        for cat in cats:
            if cat and cat.strip():
                other_categories[cat] += 1

# Show top categories that appear in "Other" pages
print("=" * 80)
print("TOP 50 WIKIPEDIA CATEGORIES IN 'OTHER' PAGES")
print("=" * 80)
print("These are the most common Wikipedia categories for pages we couldn't classify.\n")

for cat, count in other_categories.most_common(50):
    # Suggest which category this might belong to
    cat_lower = cat.lower()
    suggested_category = None
    
    if any(word in cat_lower for word in ['actor', 'actress', 'film', 'movie', 'television', 'music', 'singer', 'musician', 'entertainer']):
        suggested_category = "Entertainment"
    elif any(word in cat_lower for word in ['sport', 'football', 'basketball', 'cricket', 'athlete']):
        suggested_category = "Sports"
    elif any(word in cat_lower for word in ['politic', 'president', 'minister', 'government']):
        suggested_category = "Politics"
    elif any(word in cat_lower for word in ['writer', 'author', 'novelist', 'poet']):
        suggested_category = "Entertainment"  # or Jobs and education
    
    if suggested_category:
        print(f"{count:4d}x  {cat:<60s} → {suggested_category}")
    else:
        print(f"{count:4d}x  {cat}")

# Show sample "Other" pages with their categories
print("\n" + "=" * 80)
print("SAMPLE 'OTHER' PAGES (showing first 20)")
print("=" * 80)

sample_count = 0
for row in rows:
    page = row['page']
    if page in other_pages and sample_count < 20:
        cats = row.get('wiki_categories', '').split('|')[:5]  # First 5 categories
        print(f"\n{page}:")
        for cat in cats:
            if cat:
                print(f"  - {cat}")
        sample_count += 1

print("\n" + "=" * 80)
print("RECOMMENDATIONS")
print("=" * 80)
print("""
To improve accuracy, add these patterns to keyword_classifier.py:

1. ENTERTAINMENT - Add patterns for people:
   r'\\b(actor|actress|film actor|television actor|american actors|british actors)\\b'
   r'\\b(21st-century|20th-century).*(actor|actress|musician|singer|entertainer)'
   r'\\b(living people)\\b.*\\b(film|television|music|actor|musician)'
   
2. ENTERTAINMENT - Add patterns for writers/authors:
   r'\\b(writer|author|novelist|poet|playwright|screenwriter)\\b'
   r'\\b(21st-century|20th-century).*(writer|author|novelist)'
   
3. SPORTS - Add patterns for athletes:
   r'\\b(21st-century|20th-century).*(sportspeople|sportsmen|sportswomen)'
   r'\\b(american sportspeople|british sportspeople|indian sportspeople)'
   
4. POLITICS - Add patterns for politicians:
   r'\\b(21st-century|20th-century).*(politician)'
   r'\\b(american politicians|british politicians)\\b'

5. Use "living people" + century + profession pattern:
   Many Wikipedia pages have categories like "Living people", "21st-century American actors", etc.
   These are very reliable indicators!
""")
