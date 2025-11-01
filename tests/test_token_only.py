#!/usr/bin/env python3
import requests

# Your new token from the screenshot
token = "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"

print(f"Token length: {len(token)}")
print(f"Token starts with: {token[:5]}")
print(f"Token ends with: {token[-5:]}")
print(f"Has whitespace: {token != token.strip()}")

print("\nTesting token validation...")
response = requests.get(
    "https://huggingface.co/api/whoami",
    headers={"Authorization": f"Bearer {token}"}
)

print(f"Status: {response.status_code}")
print(f"Response: {response.text}")

if response.status_code == 200:
    print("\n✅ Token is VALID!")
    user_info = response.json()
    print(f"User: {user_info.get('name')}")
    print(f"Type: {user_info.get('type')}")
    
    # Check auth details
    if 'auth' in user_info:
        print("\nAuth details:")
        print(user_info['auth'])
else:
    print(f"\n❌ Token is INVALID")
    print("Please check:")
    print("1. Token was copied correctly")
    print("2. Token has 'Make calls to Inference Providers' permission")
    print("3. Token is 'Fine-grained' type")
