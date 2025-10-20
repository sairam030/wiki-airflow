"""
Task functions for categorizing data with LLM
"""

import pandas as pd
import requests
from plugins.config import SILVER_BUCKET, GOLD_BUCKET, OLLAMA_HOST, create_s3_client, ensure_bucket_exists
from plugins.category import classify_page_with_llm, extract_geography_with_llm, get_category_stats


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


def categorize_pages(**context):
    """Read from Silver, categorize with LLM, write to Gold"""
    
    # Check Ollama
    if not check_ollama():
        raise Exception("Ollama not running. Start with: ollama serve")
    
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
    
    print(f"✓ Loaded {len(df)} pages")
    print("🤖 Classifying with Ollama...")
    
    categories = []
    geography_data = []

    for idx, page in enumerate(df['page'].tolist(), 1):
        category = classify_page_with_llm(page)
        categories.append(category)
        
        geo = extract_geography_with_llm(page)
        geography_data.append(geo)
        
        if idx % 10 == 0 or idx == len(df):
            print(f"Processed {idx}/{len(df)} pages")
            # Preview last 10 rows
            preview_df = pd.DataFrame({
                "page": df['page'][:idx],
                "category": categories,
                "location_type": [g['location_type'] if g else None for g in geography_data],
                "location": [g['location'] if g else None for g in geography_data]
            })
            print(preview_df.tail(10))

    # Assign after processing
    df['category'] = categories
    df['location_type'] = [g['location_type'] if g else None for g in geography_data]
    df['location'] = [g['location'] if g else None for g in geography_data]


    
    # Stats
    cat_stats = get_category_stats(categories)
    geo_count = len([g for g in geography_data if g])
    
    print("\n📊 Category Distribution:")
    for cat, count in sorted(cat_stats.items(), key=lambda x: x[1], reverse=True):
        print(f"   • {cat}: {count}")
    
    print(f"\n🌍 Geography: {geo_count} pages with location")
    
    # Save to Gold
    gold_filename = output_filename.replace("cleaned_", "categorized_")
    csv_buffer = df.to_csv(index=False)
    
    s3_client.put_object(
        Bucket=GOLD_BUCKET,
        Key=gold_filename,
        Body=csv_buffer,
        ContentType='text/csv'
    )
    
    print(f"✅ Saved to Gold: {gold_filename}")
    
    return {
        'output_file': gold_filename,
        'total_pages': len(df),
        'category_stats': cat_stats,
        'pages_with_location': geo_count
    }