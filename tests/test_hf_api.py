"""
Test Hugging Face API with the Wikipedia data
"""
import requests
import json
import re

HF_API_KEY = "your_huggingface_api_key_here"

# Try different models - some may work better with Inference API
MODELS_TO_TEST = [
    "google/flan-t5-base",  # Smaller, fast, widely available
    "mistralai/Mistral-7B-Instruct-v0.2",
    "HuggingFaceH4/zephyr-7b-beta",
    "tiiuae/falcon-7b-instruct",
]

MODEL = MODELS_TO_TEST[0]  # Start with FLAN-T5

# Test data from the file
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
    {
        "title": "Women's_Cricket_World_Cup",
        "categories": ["International Cricket Council events", "Women's Cricket World Cup", 
                      "Women's One Day International cricket competitions", "World cups"]
    }
]

def test_classification(title, categories):
    """Test category classification"""
    print(f"\n{'='*80}")
    print(f"Testing: {title.replace('_', ' ')}")
    print(f"Categories: {categories[:5]}...")  # Show first 5
    
    prompt = f"""You are a Wikipedia page classifier. Analyze the page title and Wikipedia categories to return ONLY the category name from the list below.

CATEGORIES:
- Autos and vehicles
- Beauty and fashion
- Business and finance
- Climate
- Entertainment
- Food and drink
- Games
- Health
- Hobbies and leisure
- Jobs and education
- Law and government
- Pets and animals
- Politics
- Science
- Shopping
- Sports
- Technology
- Travel and transportation
- Other

Title: "{title.replace('_', ' ')}"
Wikipedia Categories: {categories}

Return ONLY the category name, nothing else."""

    try:
        print("\n🔄 Calling HF API for classification...")
        response = requests.post(
            f"https://api-inference.huggingface.co/models/{MODEL}",
            headers={"Authorization": f"Bearer {HF_API_KEY}"},
            json={"inputs": prompt, "parameters": {"max_new_tokens": 20}},
            timeout=30
        )
        
        print(f"Status: {response.status_code}")
        
        if response.status_code == 200:
            result = response.json()
            print(f"Raw response: {result}")
            
            if isinstance(result, list) and len(result) > 0:
                content = result[0].get('generated_text', '').strip()
                # Remove the original prompt if included
                if content.startswith(prompt):
                    content = content[len(prompt):].strip()
                print(f"✅ Category: {content}")
                return content
        else:
            print(f"❌ Error: {response.status_code}")
            print(f"Response: {response.text[:300]}")
            
    except Exception as e:
        print(f"❌ Exception: {e}")
    
    return None


def test_geography(title, categories):
    """Test geography extraction"""
    print(f"\n🔄 Calling HF API for geography...")
    
    prompt = f"""Extract the primary geographical location for this Wikipedia page. Return a JSON object ONLY.

Title: "{title.replace('_', ' ')}"
Wikipedia Categories: {categories}

Return JSON only:
{{"has_location": true/false, "location_type": "Country|City|State|Region|null", "location_name": "name or null"}}"""

    try:
        response = requests.post(
            f"https://api-inference.huggingface.co/models/{MODEL}",
            headers={"Authorization": f"Bearer {HF_API_KEY}"},
            json={"inputs": prompt, "parameters": {"max_new_tokens": 50}},
            timeout=30
        )
        
        if response.status_code == 200:
            result = response.json()
            
            if isinstance(result, list) and len(result) > 0:
                content = result[0].get('generated_text', '').strip()
                # Remove the original prompt if included
                if content.startswith(prompt):
                    content = content[len(prompt):].strip()
                
                print(f"Raw geography: {content}")
                
                # Try to extract JSON
                json_match = re.search(r'\{[^{}]*\}', content, re.DOTALL)
                if json_match:
                    geo_data = json.loads(json_match.group())
                    print(f"✅ Geography: {geo_data}")
                    return geo_data
        else:
            print(f"❌ Error: {response.status_code}")
            print(f"Response: {response.text[:300]}")
            
    except Exception as e:
        print(f"❌ Exception: {e}")
    
    return None


if __name__ == "__main__":
    print(f"Testing Hugging Face API with model: {MODEL}")
    print(f"API Key: {HF_API_KEY[:20]}...")
    
    for page in test_pages:
        category = test_classification(page["title"], page["categories"])
        geography = test_geography(page["title"], page["categories"])
        print(f"\n{'─'*80}")
    
    print(f"\n{'='*80}")
    print("✅ Test completed!")
