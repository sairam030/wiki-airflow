"""
Test if Hugging Face API token is valid
"""
import requests

HF_API_KEY = "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"

# Test 1: Check if token is valid by hitting the whoami endpoint
print("Testing HF API Token...")
print(f"Token: {HF_API_KEY[:20]}...")

try:
    response = requests.get(
        "https://huggingface.co/api/whoami",
        headers={"Authorization": f"Bearer {HF_API_KEY}"}
    )
    print(f"\nWhoami Status: {response.status_code}")
    if response.status_code == 200:
        print(f"✅ Token is valid!")
        print(f"User info: {response.json()}")
    else:
        print(f"❌ Token validation failed: {response.text}")
except Exception as e:
    print(f"❌ Error: {e}")

# Test 2: Try a simple inference with a known working model
print("\n" + "="*80)
print("Testing Inference API with gpt2 (should always be available)...")

try:
    response = requests.post(
        "https://api-inference.huggingface.co/models/gpt2",
        headers={"Authorization": f"Bearer {HF_API_KEY}"},
        json={"inputs": "The answer is"},
        timeout=30
    )
    print(f"Status: {response.status_code}")
    if response.status_code == 200:
        print(f"✅ Inference API works!")
        print(f"Response: {response.json()}")
    else:
        print(f"❌ Inference failed: {response.text}")
except Exception as e:
    print(f"❌ Error: {e}")
