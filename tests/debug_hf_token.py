"""
Debug HuggingFace token - check for issues
"""
import requests

# Your token
HF_API_KEY = "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"

print("Token Debug Info:")
print(f"Length: {len(HF_API_KEY)}")
print(f"First 10 chars: {HF_API_KEY[:10]}")
print(f"Last 10 chars: {HF_API_KEY[-10:]}")
print(f"Has spaces at start: {HF_API_KEY[0] == ' '}")
print(f"Has spaces at end: {HF_API_KEY[-1] == ' '}")
print(f"Starts with 'hf_': {HF_API_KEY.startswith('hf_')}")
print()

# Try with stripped token (remove any accidental whitespace)
HF_API_KEY = HF_API_KEY.strip()

print("Testing token validation...")
try:
    response = requests.get(
        "https://huggingface.co/api/whoami",
        headers={"Authorization": f"Bearer {HF_API_KEY}"},
        timeout=10
    )
    print(f"Status Code: {response.status_code}")
    print(f"Response: {response.text}")
    
    if response.status_code == 200:
        print("\n✅ TOKEN IS VALID!")
        user_info = response.json()
        print(f"Username: {user_info.get('name', 'N/A')}")
        print(f"Full info: {user_info}")
    elif response.status_code == 401:
        print("\n❌ TOKEN IS INVALID")
        print("\nPossible reasons:")
        print("1. Token was revoked or expired")
        print("2. Token was not copied correctly (missing characters)")
        print("3. Wrong token type (need 'Fine-grained' or 'Read' token)")
        print("\nPlease:")
        print("- Go to: https://huggingface.co/settings/tokens")
        print("- Create a NEW token (delete old one if needed)")
        print("- Choose 'Fine-grained' and enable 'Make calls to serverless Inference API'")
        print("- Copy the ENTIRE token (starts with hf_)")
    else:
        print(f"\n⚠️ Unexpected status: {response.status_code}")
        
except Exception as e:
    print(f"❌ Error making request: {e}")
