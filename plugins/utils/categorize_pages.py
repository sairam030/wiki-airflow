"""
Task functions for categorizing data with Keyword Classifier or Ollama LLM
Supports: "keyword" (fast, no API) or "ollama" (local GPU)
"""

import pandas as pd
import requests
import time
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from plugins.config import SILVER_BUCKET, GOLD_BUCKET, OLLAMA_HOST, CLASSIFIER_METHOD, create_s3_client, ensure_bucket_exists
from plugins.operators.ollama_classifier import classify_page_with_llm, extract_geography_with_llm, get_category_stats
from plugins.operators.keyword_classifier import classify_with_keywords, extract_geography_with_keywords


def check_ollama():
    """Check if Ollama is running on host"""
    try:
        response = requests.get(f"{OLLAMA_HOST}/api/tags", timeout=5)
        if response.status_code == 200:
            print("✅ Ollama is running on host")
            return True
        return False
    except Exception as e:
        print(f"❌ Ollama not accessible: {str(e)}")
        print("   Make sure to run: ollama serve")
        return False


def process_single_page(page, wiki_categories):
    """Process a single page (for parallel execution)"""
    if CLASSIFIER_METHOD == "keyword":
        # Fast keyword-based classification
        category = classify_with_keywords(page, wiki_categories)
        geo = extract_geography_with_keywords(page, wiki_categories)
    else:
        # Ollama LLM-based classification
        category = classify_page_with_llm(page, wiki_categories=wiki_categories)
        geo = extract_geography_with_llm(page, wiki_categories=wiki_categories)
    
    return {
        'page': page,
        'category': category,
        'geography': geo
    }


def categorize_pages(**context):
    """Read from Silver (with wiki_categories), categorize, write to Gold"""
    
    # Check requirements based on classifier method
    if CLASSIFIER_METHOD == "keyword":
        print("🔤 Using KEYWORD classifier (fast, no external dependencies)")
    elif CLASSIFIER_METHOD == "ollama":
        if not check_ollama():
            raise Exception("Ollama not running. Start with: ollama serve")
        print(f"🤖 Using OLLAMA LLM classifier")
    else:
        raise ValueError(f"Unknown CLASSIFIER_METHOD: {CLASSIFIER_METHOD}. Use 'keyword' or 'ollama'")
    
    # Get filename
    ti = context['ti']
    metadata = ti.xcom_pull(task_ids='clean_top_pages')
    
    if not metadata:
        raise ValueError("No metadata from clean_top_pages")
    
    output_filename = metadata['output_file']
    print(f"📊 Categorizing: {output_filename}")
    
    ensure_bucket_exists(GOLD_BUCKET)
    
    # Read CSV from Silver
    s3_client = create_s3_client()
    response = s3_client.list_objects_v2(Bucket=SILVER_BUCKET, Prefix=output_filename)
    
    csv_files = [
        obj['Key'] for obj in response.get('Contents', [])
        if obj['Key'].endswith('.csv') and not obj['Key'].endswith('/_SUCCESS')
    ]
    
    if not csv_files:
        raise ValueError(f"No CSV in {SILVER_BUCKET}/{output_filename}")
    
    # Read data
    obj = s3_client.get_object(Bucket=SILVER_BUCKET, Key=csv_files[0])
    df = pd.read_csv(obj['Body'])
    
    print(f"✓ Loaded {len(df)} pages from Silver")
    print(f"   Columns: {list(df.columns)}")
    
    # ========== Use Wikipedia categories from CSV (already fetched!) ==========
    print("\n" + "="*70)
    print(f"🎯 CLASSIFIER: {CLASSIFIER_METHOD.upper()}")
    print("="*70)
    
    # Convert pipe-separated categories back to lists
    wiki_categories_dict = {}
    for idx, row in df.iterrows():
        page = row['page']
        cats_str = row.get('wiki_categories', '')
        if pd.notna(cats_str) and cats_str:
            wiki_categories_dict[page] = cats_str.split('|')
        else:
            wiki_categories_dict[page] = []
    
    # Show stats
    pages_with_cats = sum(1 for cats in wiki_categories_dict.values() if cats)
    avg_cats = sum(len(cats) for cats in wiki_categories_dict.values()) / len(wiki_categories_dict) if wiki_categories_dict else 0
    
    print(f"\n📊 Wikipedia Categories Available:")
    print(f"   • Total pages: {len(wiki_categories_dict)}")
    print(f"   • Pages with categories: {pages_with_cats}")
    print(f"   • Avg categories per page: {avg_cats:.1f}")
    
    # Determine worker count based on classifier method
    if CLASSIFIER_METHOD == "keyword":
        print(f"\n⚡ Starting KEYWORD classification (instant, no API calls)...")
        print(f"   Method: Pattern matching on Wikipedia categories")
        print(f"   Workers: 10 concurrent (CPU-bound, very fast)")
        print(f"   Expected time: ~5-10 seconds for 995 pages")
        workers = 10
    else:  # ollama
        print(f"\n🤖 Starting OLLAMA LLM classification...")
        print(f"   Model: Qwen2.5:1.5b (GPU-accelerated)")
        print(f"   Workers: 2 concurrent (RTX 2060 optimized)")
        workers = 2
    
    print(f"   Progress will be shown every 10 pages\n")
    
    start_time = time.time()
    results = {}
    completed_count = 0
    
    # Use ThreadPoolExecutor for parallel LLM calls
    with ThreadPoolExecutor(max_workers=workers) as executor:
        # Submit all tasks
        future_to_idx = {}
        for idx, row in df.iterrows():
            page = row['page']
            page_cats = wiki_categories_dict.get(page, [])
            future = executor.submit(process_single_page, page, page_cats)
            future_to_idx[future] = idx
        
        # Process results as they complete
        for future in as_completed(future_to_idx):
            idx = future_to_idx[future]
            try:
                result = future.result()
                results[idx] = result
                completed_count += 1
                
                # Progress updates every 10 pages
                if completed_count % 10 == 0 or completed_count == len(df):
                    elapsed = time.time() - start_time
                    avg_time = elapsed / completed_count
                    remaining = (len(df) - completed_count) * avg_time
                    progress_pct = (completed_count / len(df)) * 100
                    
                    print(f"✓ {completed_count}/{len(df)} ({progress_pct:.1f}%) | "
                          f"Elapsed: {elapsed:.1f}s | Avg: {avg_time:.2f}s/page | "
                          f"ETA: {remaining:.1f}s (~{remaining/60:.1f} min)")
                    
                    # Show last classified page
                    print(f"  └─ Last: {result['page'][:40]:40} → {result['category']}")
                    
            except Exception as e:
                print(f"⚠️  Error processing page at index {idx}: {str(e)}")
                # Add fallback result
                results[idx] = {
                    'page': df.iloc[idx]['page'],
                    'category': 'Other',
                    'geography': None
                }
                completed_count += 1
    
    # Sort results by original index and extract data
    categories = []
    geography_data = []
    for idx in range(len(df)):
        if idx in results:
            categories.append(results[idx]['category'])
            geography_data.append(results[idx]['geography'])
        else:
            # Fallback for any missing results
            categories.append('Other')
            geography_data.append(None)
    
    total_time = time.time() - start_time
    print(f"\n⏱️  Total classification time: {total_time:.1f}s ({total_time/60:.1f} minutes)")
    print(f"   Average: {total_time/len(df):.2f}s per page")
    if CLASSIFIER_METHOD == "keyword":
        print(f"   ⚡ Keyword-based classification (instant, no API calls)")
    elif CLASSIFIER_METHOD == "ollama":
        print(f"   🚀 GPU-accelerated with {workers} parallel workers")
    else:
        print(f"   🌐 API-based classification with {workers} parallel workers")

    # Assign after processing
    df['category'] = categories
    df['location_type'] = [g['location_type'] if g else None for g in geography_data]
    df['location'] = [g['location'] if g else None for g in geography_data]
    
    # Replace NaN/None geography with "No specific location" for better visualization
    df['location'] = df['location'].fillna('No specific location')
    df['location_type'] = df['location_type'].fillna('General')
    
    # Stats
    cat_stats = get_category_stats(categories)
    geo_count = len([g for g in geography_data if g])
    
    print("\n📊 Category Distribution:")
    for cat, count in sorted(cat_stats.items(), key=lambda x: x[1], reverse=True):
        print(f"   • {cat}: {count}")
    
    print(f"\n🌍 Geography: {geo_count} pages with location")
    
    # Save to temporary Gold location for clean_gold_layer to process
    # Note: This is an intermediate file, clean_gold_layer will create final Gold output
    gold_filename = output_filename.replace("cleaned_", "categorized_")
    csv_buffer = df.to_csv(index=False)
    
    s3_client.put_object(
        Bucket=GOLD_BUCKET,
        Key=gold_filename,
        Body=csv_buffer,
        ContentType='text/csv'
    )
    
    print(f"✅ Saved categorized data (intermediate): {gold_filename}")
    print("   → Will be cleaned by clean_gold_layer before final Gold")
    
    return {
        'output_file': gold_filename,
        'total_pages': len(df),
        'category_stats': cat_stats,
        'pages_with_location': geo_count
    }