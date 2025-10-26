"""
Category definitions and classification logic using Ollama in Docker
"""
import requests
import json
import re
import time

# Ollama API endpoint (Docker container)
OLLAMA_HOST = "http://ollama:11434"

# Using Qwen2.5:3B - optimized for classification and structured output
MODEL_NAME = "qwen2.5:3b"

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

# System prompts optimized for Qwen2.5
CATEGORY_SYSTEM_PROMPT = """You are a Wikipedia page classifier. Analyze the page title and return ONLY the category name from the list below.

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

CLASSIFICATION RULES:
1. People → classify by their primary profession or fame:
   - Athletes, sports figures → Sports
   - Musicians, actors, celebrities → Entertainment
   - Politicians, government officials → Politics
   - Scientists, researchers → Science
   - Business leaders → Business and finance

2. Media content (movies, TV shows, books, music) → Entertainment

3. Events → classify by the event's nature:
   - Olympics, World Cup, championships → Sports
   - Elections, summits → Politics
   - Festivals, award shows → Entertainment

4. Products/services → classify by industry:
   - Phones, computers, software → Technology
   - Cars, motorcycles → Autos and vehicles
   - Retail products → Shopping

5. If genuinely uncertain or doesn't fit → Other

OUTPUT FORMAT: Return ONLY the category name, nothing else.

EXAMPLES:
"2024 Summer Olympics" → Sports
"iPhone 16" → Technology
"Taylor Swift" → Entertainment
"Joe Biden" → Politics
"Climate change" → Climate
"ChatGPT" → Technology
"FIFA World Cup" → Sports
"The Godfather" → Entertainment"""


GEOGRAPHY_SYSTEM_PROMPT = """Extract the primary geographical location for this Wikipedia page title. Return a JSON object ONLY.

EXTRACTION RULES:
1. People → their country of origin, citizenship, or primary residence
2. Cities/States/Countries → the location itself
3. Events → where the event takes place
4. Organizations/Companies → headquarters or primary location
5. Natural phenomena/Disasters → affected location
6. If no clear location → set has_location to false

LOCATION TYPES:
- Country: Nation-level (USA, France, Japan, etc.)
- City: Specific city (Paris, Tokyo, New York, etc.)
- State: State/Province (California, Texas, Ontario, etc.)
- Region: Broader region (Middle East, Southeast Asia, etc.)

OUTPUT FORMAT (JSON only, no markdown):
{"has_location": true/false, "location_type": "Country|City|State|Region|null", "location_name": "name or null"}

EXAMPLES:
"2024 Paris Olympics" → {"has_location": true, "location_type": "City", "location_name": "Paris"}
"California wildfires" → {"has_location": true, "location_type": "State", "location_name": "California"}
"Emmanuel Macron" → {"has_location": true, "location_type": "Country", "location_name": "France"}
"Elon Musk" → {"has_location": true, "location_type": "Country", "location_name": "USA"}
"Artificial intelligence" → {"has_location": false, "location_type": null, "location_name": null}
"COVID-19 pandemic" → {"has_location": false, "location_type": null, "location_name": null}"""



def call_ollama_api(prompt, system_prompt, model=MODEL_NAME, max_retries=3):
    """
    Call Ollama API with retry logic and optimized parameters for Qwen2.5
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
                        "temperature": 0.0,  # Deterministic output for classification
                        "top_p": 0.1,        # More focused responses
                        "num_predict": 50,   # Enough for category name or JSON
                        "stop": ["\n\n"]     # Stop at double newline
                    }
                },
                timeout=90
            )
            
            if response.status_code == 200:
                content = response.json()['message']['content'].strip()
                # Remove markdown code blocks if present
                content = re.sub(r'```json\s*|\s*```', '', content)
                return content
            elif response.status_code == 500:
                # Model not loaded, wait and retry
                if attempt < max_retries - 1:
                    print(f"⚠️  Model loading, retry {attempt + 1}/{max_retries}...")
                    time.sleep(10)
                    continue
                else:
                    print(f"❌ Ollama 500 error after {max_retries} retries")
                    return None
            else:
                print(f"⚠️  Ollama API error: {response.status_code}")
                if attempt < max_retries - 1:
                    time.sleep(3)
                else:
                    return None
                
        except requests.exceptions.Timeout:
            print(f"⚠️  Ollama timeout on attempt {attempt + 1}/{max_retries}")
            if attempt < max_retries - 1:
                time.sleep(5)
            else:
                return None
        except Exception as e:
            print(f"⚠️  Ollama error: {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(3)
            else:
                return None
    
    return None


def classify_page_with_llm(page_title, model=MODEL_NAME):
    """Classify a Wikipedia page using Qwen2.5"""
    clean_title = page_title.replace("_", " ")
    result = call_ollama_api(clean_title, CATEGORY_SYSTEM_PROMPT, model)
    
    if not result:
        print(f"⚠️  No response from model for: {page_title}")
        return "Other"
    
    # Clean the result
    result = result.strip().strip('"').strip("'")
    
    # Direct match
    if result in CATEGORIES:
        return result
    
    # Case-insensitive exact match
    for cat in CATEGORIES:
        if cat.lower() == result.lower():
            return cat
    
    # Partial match (in case model adds extra text)
    for cat in CATEGORIES:
        if cat.lower() in result.lower():
            return cat
    
    print(f"⚠️  Unrecognized category '{result}' for '{page_title}', defaulting to Other")
    return "Other"


def extract_geography_with_llm(page_title, model=MODEL_NAME):
    """Extract geographical information using Qwen2.5"""
    clean_title = page_title.replace("_", " ")
    result = call_ollama_api(clean_title, GEOGRAPHY_SYSTEM_PROMPT, model)
    
    if not result:
        print(f"⚠️  No geography response for: {page_title}")
        return None
    
    try:
        # Try to extract JSON from the response
        json_match = re.search(r'\{[^{}]*\}', result, re.DOTALL)
        if json_match:
            geo_data = json.loads(json_match.group())
            
            # Validate the JSON structure
            if not isinstance(geo_data, dict):
                return None
            
            has_location = geo_data.get('has_location', False)
            
            if has_location and geo_data.get('location_name'):
                return {
                    'location_type': geo_data.get('location_type', 'Country'),
                    'location': geo_data.get('location_name')
                }
        
        return None
        
    except json.JSONDecodeError as e:
        print(f"⚠️  JSON parse error for '{page_title}': {e}")
        return None
    except Exception as e:
        print(f"⚠️  Geography extraction error for '{page_title}': {e}")
        return None


def get_category_stats(categories_list):
    """Get category distribution statistics"""
    from collections import Counter
    return dict(Counter(categories_list))