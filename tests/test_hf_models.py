
#!/usr/bin/env python3
"""
Test HuggingFace Inference Providers API with correct endpoint
According to: https://huggingface.co/docs/inference-providers
"""
import os
import requests

HF_TOKEN = os.environ.get("HF_TOKEN", "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")

print("="*80)
print("Testing HuggingFace Inference Providers API")
print("Docs: https://huggingface.co/docs/inference-providers")
print("="*80)

# Step 1: Validate token with correct permission
print("\n1. Validating token...")
try:
    response = requests.get(
        "https://huggingface.co/api/whoami",
        headers={"Authorization": f"Bearer {HF_TOKEN}"}
    )
    if response.status_code == 200:
        user_info = response.json()
        print(f"✅ Token is valid: {user_info.get('name', 'Unknown')}")
        
        # Check token permissions
        auth = user_info.get('auth', {})
        access_repo = auth.get('accessToken', {})
        print(f"   Token type: {access_repo.get('type', 'N/A')}")
        print(f"   Token role: {access_repo.get('role', 'N/A')}")
    else:
        print(f"❌ Token invalid: {response.status_code} - {response.text}")
        print("\n⚠️  You need a token with 'Make calls to Inference Providers' permission")
        print("   Create one at: https://huggingface.co/settings/tokens/new?tokenType=fineGrained")
        exit(1)
except Exception as e:
    print(f"❌ Error: {e}")
    exit(1)

# Step 2: Test with OpenAI-compatible endpoint (CORRECT endpoint from docs)
print("\n2. Testing with OpenAI-compatible endpoint...")
print("   Endpoint: https://router.huggingface.co/v1/chat/completions")

# Popular models available via Inference Providers (with free tier credits)
# Free users get $0.10/month, PRO users get $2.00/month
# Check available models: https://huggingface.co/models?inference_provider=all
test_models = [
 "zai-org/GLM-4.6"
]

test_prompt = "Classify this article: 'Lionel Messi' into one category: Sports, Technology, Politics, Entertainment, Business, Science, Health, or Other."

for model in test_models:
    print(f"\n   Testing: {model}")
    print("   " + "-"*76)
    
    try:
        response = requests.post(
            "https://router.huggingface.co/v1/chat/completions",  # ✅ CORRECT ENDPOINT
            headers={
                "Authorization": f"Bearer {HF_TOKEN}",
                "Content-Type": "application/json"
            },
            json={
                "model": model,
                "messages": [
                    {"role": "user", "content": test_prompt}
                ],
                "max_tokens": 50,
                "stream": False
            },
            timeout=30
        )
        
        if response.status_code == 200:
            result = response.json()
            content = result.get('choices', [{}])[0].get('message', {}).get('content', 'No content')
            print(f"   ✅ Status: {response.status_code}")
            print(f"   Response: {content[:100]}")
        else:
            print(f"   ❌ Status: {response.status_code}")
            print(f"   Error: {response.text[:200]}")
            
    except Exception as e:
        print(f"   ❌ Error: {e}")

print("\n" + "="*80)
print("If models return 404, they might not be available via Inference Providers.")
print("Check available models at: https://huggingface.co/models?inference_provider=all")
print("="*80)
