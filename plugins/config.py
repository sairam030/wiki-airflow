# plugins/config.py - Update OLLAMA_HOST

"""
Configuration for Wikipedia Pipeline
"""

import boto3
import os

# MinIO Configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

# Bucket names
BRONZE_BUCKET = "bronze"
SILVER_BUCKET = "silver"
GOLD_BUCKET = "gold"
DIAMOND_BUCKET = "diamond"  

# ============================================================================
# CLASSIFIER CONFIGURATION - Choose your classification method
# ============================================================================
# Options:
#   "keyword" - Fast keyword-based (87.5% accuracy, ~2 min for 995 pages, no API)
#   "ollama"  - Local GPU LLM (90%+ accuracy, requires Ollama running)
# ============================================================================
CLASSIFIER_METHOD = os.getenv("CLASSIFIER_METHOD", "keyword")  # DEFAULT: keyword (fast & reliable)

# Ollama Configuration
OLLAMA_HOST = os.getenv("OLLAMA_HOST", "http://ollama:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5:1.5b")  # Fast and accurate model


def create_s3_client():
    """Create boto3 S3 client for MinIO"""
    return boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name='us-east-1'
    )


def ensure_bucket_exists(bucket_name):
    """Create bucket if it doesn't exist"""
    s3_client = create_s3_client()
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"✓ Bucket '{bucket_name}' exists")
    except:
        s3_client.create_bucket(Bucket=bucket_name)
        print(f"✓ Created bucket '{bucket_name}'")