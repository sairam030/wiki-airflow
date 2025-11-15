"""
Test new HuggingFace Inference Providers API endpoint
NEW: https://router.huggingface.co/hf-inference/
OLD: https://api-inference.huggingface.co/ (deprecated, returns 404)
"""
import requests
import json
import re

HF_API_KEY = "your_hf_api_token_here"  # Replace with your actual token


# Test different models
MODELS_TO_TEST = [
    "google/flan-t5-base",
    "mistralai/Mistral-7B-Instruct-v0.2",
    "HuggingFaceH4/zephyr-7b-beta",
]

print("="*100)
print("TESTING NEW HUGGINGFACE INFERENCE PROVIDERS API")
print("="*100)
print(f"API Key: {HF_API_KEY[:20]}...")
print()

# Test 1: Check if token is valid with new API
print("Test 1: Validating API token...")
try:
    response = requests.get(
        "https://huggingface.co/api/whoami",
        headers={"Authorization": f"Bearer {HF_API_KEY}"}
    )
    print(f"Status: {response.status_code}")
    if response.status_code == 200:
        print(f"✅ Token is VALID!")
        print(f"User info: {response.json()}")
    else:
        print(f"❌ Token is INVALID: {response.text}")
        print("\n⚠️  You need to get a new token from: https://huggingface.co/settings/tokens")
        exit(1)
except Exception as e:
    print(f"❌ Error: {e}")
    exit(1)

print("\n" + "="*100)

# Test 2: Try the new endpoint with different models
test_prompt = """Title: "Google Chrome"
Wikipedia Categories: ['2008 software', 'Google Chrome', 'Google software', 'Web browsers', 'Cross-platform web browsers']

Which category best fits this page? Return only one:
- Sports
- Technology  
- Entertainment
- Politics
- Other

Return ONLY the category name."""

for model in MODELS_TO_TEST:
    print(f"\nTest 2.{MODELS_TO_TEST.index(model) + 1}: Testing model: {model}")
    print("-" * 100)
    
    # Test OLD endpoint (should fail with 404)
    print(f"   OLD endpoint: https://api-inference.huggingface.co/models/{model}")
    try:
        response = requests.post(
            f"https://api-inference.huggingface.co/models/{model}",
            headers={"Authorization": f"Bearer {HF_API_KEY}"},
            json={"inputs": test_prompt, "parameters": {"max_new_tokens": 20}},
            timeout=30
        )
        print(f"   Status: {response.status_code}")
        if response.status_code == 404:
            print(f"   ✅ Expected 404 (endpoint deprecated)")
        else:
            print(f"   Response: {response.text[:200]}")
    except Exception as e:
        print(f"   Error: {e}")
    
    # Test NEW endpoint
    print(f"\n   NEW endpoint: https://router.huggingface.co/hf-inference/models/{model}")
    try:
        response = requests.post(
            f"https://router.huggingface.co/hf-inference/models/{model}",
            headers={"Authorization": f"Bearer {HF_API_KEY}"},
            json={"inputs": test_prompt, "parameters": {"max_new_tokens": 20}},
            timeout=30
        )
        print(f"   Status: {response.status_code}")
        
        if response.status_code == 200:
            result = response.json()
            print(f"   ✅ SUCCESS!")
            print(f"   Response: {result}")
            
            # Try to extract the answer
            if isinstance(result, list) and len(result) > 0:
                content = result[0].get('generated_text', '').strip()
                if content.startswith(test_prompt):
                    content = content[len(test_prompt):].strip()
                print(f"   Answer: {content}")
        else:
            print(f"   ❌ Failed: {response.text[:300]}")
            
    except Exception as e:
        print(f"   ❌ Error: {e}")
    
    print()

print("\n" + "="*100)
print("TESTING COMPLETE")
print("="*100)
