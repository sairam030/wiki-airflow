"""
Category definitions and classification logic using Ollama in Docker
"""
import requests
import json
import re
import time

# Ollama API endpoint (Docker container)
OLLAMA_HOST = "http://ollama:11434"

MODEL_NAME = "llama3.2:3b"

# All 19 categories
CATEGORIES = [
    "Autos and vehicles",
    "Beauty and fashion",
    "Business and finance",
    "Climate",
    "Entertainment",
    "Food and drink",
    "Games",
    "Health",
    "Hobbies and leisure",
    "Jobs and education",
    "Law and government",
    "Pets and animals",
    "Politics",
    "Science",
    "Shopping",
    "Sports",
    "Technology",
    "Travel and transportation",
    "Other"
]

# System prompts
CATEGORY_SYSTEM_PROMPT = """
You are an expert in Wikipedia page classification. Classify the Wikipedia page title into ONE category.

Categories: Autos and vehicles, Beauty and fashion, Business and finance, Climate, Entertainment, Food and drink, Games, Health, Hobbies and leisure, Jobs and education, Law and government, Pets and animals, Politics, Science, Shopping, Sports, Technology, Travel and transportation, Other

Rules:
1. Return ONLY the category name.
2. For people (celebrities, politicians, scientists), classify based on their profession (e.g., singer → Entertainment, politician → Politics, football player → Sports).
3. For movies, books, TV shows, classify them under Entertainment.
4. If unsure, return "Other".

Examples:
"2024 Olympics" → Sports
"iPhone 15" → Technology
"Taylor Swift" → Entertainment
"Barack Obama" → Politics
"The Godfather" → Entertainment
"""


GEOGRAPHY_SYSTEM_PROMPT = """
Extract the geographical location associated with this Wikipedia page title.

Rules:
1. For people, return their country of origin or nationality.
2. For cities, states, regions, countries, return the proper location.
3. Return a JSON object like: {"has_location": true/false, "location_type": "Country|City|State|Region", "location_name": "name"}
4. If no location, set has_location to false and leave others null.

Examples:
"Paris Olympics" → {"has_location": true, "location_type": "City", "location_name": "Paris"}
"California fires" → {"has_location": true, "location_type": "State", "location_name": "California"}
"Barack Obama" → {"has_location": true, "location_type": "Country", "location_name": "USA"}
"AI technology" → {"has_location": false, "location_type": null, "location_name": null}
"""



def call_ollama_api(prompt, system_prompt, model=MODEL_NAME, max_retries=2):
    """
    Call Ollama API with retry logic
    """
    for attempt in range(max_retries):
        try:
            response = requests.post(
                f"{OLLAMA_HOST}/api/chat",
                json={
                    "model": model,
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": prompt}
                    ],
                    "stream": False,
                    "options": {
                        "temperature": 0.1,
                        "num_predict": 20  # Shorter responses
                    }
                },
                timeout=60  # Increased timeout
            )
            
            if response.status_code == 200:
                return response.json()['message']['content'].strip()
            elif response.status_code == 500:
                # Model not loaded, wait and retry
                if attempt < max_retries - 1:
                    print(f"⚠️  Model loading, retry {attempt + 1}/{max_retries}...")
                    time.sleep(5)
                    continue
                else:
                    print(f"❌ Ollama 500 error after {max_retries} retries")
                    return None
            else:
                print(f"⚠️  Ollama API error: {response.status_code}")
                return None
                
        except Exception as e:
            print(f"⚠️  Ollama error: {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(2)
            else:
                return None
    
    return None


def classify_page_with_llm(page_title, model=MODEL_NAME):
    """Classify a Wikipedia page using Ollama"""
    clean_title = page_title.replace("_", " ")
    result = call_ollama_api(clean_title, CATEGORY_SYSTEM_PROMPT, model)
    
    if not result:
        return "Other"
    
    # Validate category
    if result in CATEGORIES:
        return result
    
    # Fuzzy match
    for cat in CATEGORIES:
        if cat.lower() in result.lower():
            return cat
    
    return "Other"


def extract_geography_with_llm(page_title, model=MODEL_NAME):
    """Extract geographical information using Ollama"""
    clean_title = page_title.replace("_", " ")
    result = call_ollama_api(clean_title, GEOGRAPHY_SYSTEM_PROMPT, model)
    
    if not result:
        return None
    
    try:
        json_match = re.search(r'\{.*\}', result, re.DOTALL)
        if json_match:
            geo_data = json.loads(json_match.group())
            if geo_data.get('has_location'):
                return {
                    'location_type': geo_data.get('location_type'),
                    'location': geo_data.get('location_name')
                }
    except:
        pass
    
    return None


def get_category_stats(categories_list):
    """Get category distribution statistics"""
    from collections import Counter
    return dict(Counter(categories_list))