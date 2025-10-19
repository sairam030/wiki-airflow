"""
Category definitions and classification logic using Local LLM on HOST machine
"""

import requests
import json
import re

# Ollama API endpoint (running on host machine)
OLLAMA_HOST = "http://ollama:11434"  # ← Points to Ollama container
# If running Airflow outside Docker, use: http://localhost:11434

# Predefined categories
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
CATEGORY_SYSTEM_PROMPT = """You are a classification expert. Given a Wikipedia page title, classify it into ONE of these categories:

Categories:
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

Rules:
1. Return ONLY the category name, nothing else
2. Choose the MOST relevant category
3. If unsure, choose "Other"
4. Be concise and accurate

Example:
Input: "2024 Summer Olympics"
Output: Sports

Input: "Tesla Model 3"
Output: Autos and vehicles"""

GEOGRAPHY_SYSTEM_PROMPT = """You are a geography extraction expert. Given a Wikipedia page title, extract geographical information.

Return a JSON object with:
{
  "has_location": true/false,
  "location_type": "Country" | "City" | "State" | "Region" | null,
  "location_name": "name of location" | null
}

Examples:
Input: "2024 Paris Summer Olympics"
Output: {"has_location": true, "location_type": "City", "location_name": "Paris"}

Input: "California wildfires"
Output: {"has_location": true, "location_type": "State", "location_name": "California"}

Input: "Artificial intelligence"
Output: {"has_location": false, "location_type": null, "location_name": null}

Rules:
1. Return ONLY valid JSON
2. Extract the most specific location
3. Common locations: USA, UK, China, India, New York, London, California, Texas, Europe, Asia"""


def call_ollama_api(prompt, system_prompt, model="llama3.2:3b"):
    """
    Call Ollama API running on host machine
    
    Args:
        prompt: User prompt
        system_prompt: System instructions
        model: Model name
        
    Returns:
        Response text
    """
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
                    "num_predict": 50
                }
            },
            timeout=30
        )
        
        if response.status_code == 200:
            return response.json()['message']['content'].strip()
        else:
            print(f"⚠️  Ollama API error: {response.status_code}")
            return None
            
    except Exception as e:
        print(f"⚠️  Ollama connection error: {str(e)}")
        return None


def classify_page_with_llm(page_title, model="llama3.2:3b"):
    """
    Classify a Wikipedia page using local LLM on host
    
    Args:
        page_title: Page title to classify
        model: Ollama model to use
        
    Returns:
        Category name
    """
    # Clean title
    clean_title = page_title.replace("_", " ")
    
    # Call Ollama
    result = call_ollama_api(clean_title, CATEGORY_SYSTEM_PROMPT, model)
    
    if not result:
        return "Other"
    
    # Validate category
    if result in CATEGORIES:
        return result
    
    # Try to match partial
    for cat in CATEGORIES:
        if cat.lower() in result.lower():
            return cat
    
    return "Other"


def extract_geography_with_llm(page_title, model="llama3.2:3b"):
    """
    Extract geographical information using local LLM on host
    
    Args:
        page_title: Page title to analyze
        model: Ollama model to use
        
    Returns:
        Dictionary with location info or None
    """
    # Clean title
    clean_title = page_title.replace("_", " ")
    
    # Call Ollama
    result = call_ollama_api(clean_title, GEOGRAPHY_SYSTEM_PROMPT, model)
    
    if not result:
        return None
    
    try:
        # Extract JSON from response
        json_match = re.search(r'\{.*\}', result, re.DOTALL)
        if json_match:
            geo_data = json.loads(json_match.group())
            
            if geo_data.get('has_location'):
                return {
                    'location_type': geo_data.get('location_type'),
                    'location': geo_data.get('location_name')
                }
        
        return None
        
    except Exception as e:
        print(f"⚠️  Geography parsing error: {str(e)}")
        return None


def get_category_stats(categories_list):
    """Get statistics about category distribution"""
    from collections import Counter
    return dict(Counter(categories_list))