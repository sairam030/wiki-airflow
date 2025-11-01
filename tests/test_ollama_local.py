"""
Test Ollama API locally before running in Docker
"""
import requests
import json
import re

OLLAMA_HOST = "http://localhost:11434"
MODEL = "qwen2.5:1.5b"

# Test data
test_pages = [
    {
        "title": "Trey_Yesavage",
        "categories": ["2003 births", "21st-century American sportsmen", "All-American college baseball players", 
                      "Baseball players from Berks County, Pennsylvania", "Buffalo Bisons (minor league) players",
                      "Living people", "Major League Baseball pitchers", "Toronto Blue Jays players"]
    },
    {
        "title": "Google_Chrome",
        "categories": ["2008 software", "Google Chrome", "Google software", "Web browsers", 
                      "Cross-platform web browsers", "Software based on WebKit"]
    },
]

CATEGORY_PROMPT = """You are a Wikipedia page classifier. Return ONLY the category name from this list:
- Sports
- Technology
- Entertainment
- Politics
- Science
- Business and finance
- Health
- Travel and transportation
- Other

OUTPUT FORMAT: Return ONLY the category name, nothing else."""

def test_ollama_classification(title, categories):
    """Test category classification with Ollama"""
    print(f"\n{'='*80}")
    print(f"Testing: {title.replace('_', ' ')}")
    print(f"Categories: {categories[:3]}...")
    
    prompt = f'Title: "{title.replace("_", " ")}"\nWikipedia Categories: {categories[:5]}'
    
    try:
        print("\n🔄 Calling Ollama API...")
        response = requests.post(
            f"{OLLAMA_HOST}/api/chat",
            json={
                "model": MODEL,
                "messages": [
                    {"role": "system", "content": CATEGORY_PROMPT},
                    {"role": "user", "content": prompt}
                ],
                "stream": False,
                "options": {
                    "temperature": 0.0,
                    "num_predict": 20,
                    "num_gpu": 99,
                }
            },
            timeout=30
        )
        
        print(f"Status: {response.status_code}")
        
        if response.status_code == 200:
            content = response.json()['message']['content'].strip()
            print(f"✅ Category: {content}")
            return content
        else:
            print(f"❌ Error: {response.status_code}")
            print(f"Response: {response.text[:300]}")
            
    except Exception as e:
        print(f"❌ Exception: {e}")
    
    return None


if __name__ == "__main__":
    print(f"Testing Ollama API")
    print(f"Host: {OLLAMA_HOST}")
    print(f"Model: {MODEL}")
    
    # First check if Ollama is running
    try:
        response = requests.get(f"{OLLAMA_HOST}/api/tags", timeout=5)
        if response.status_code == 200:
            models = response.json().get('models', [])
            print(f"\n✅ Ollama is running!")
            print(f"Available models: {[m['name'] for m in models]}")
        else:
            print(f"\n❌ Ollama not responding properly")
            exit(1)
    except Exception as e:
        print(f"\n❌ Ollama not running: {e}")
        print("\nStart Ollama with: ollama serve")
        exit(1)
    
    for page in test_pages:
        test_ollama_classification(page["title"], page["categories"])
    
    print(f"\n{'='*80}")
    print("✅ Test completed!")
