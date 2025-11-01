"""
Geocoding utility to add latitude/longitude to locations
Uses Nominatim (OpenStreetMap) - free, no API key required
"""

import time
import requests
from functools import lru_cache

# Nominatim API endpoint
NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
REQUEST_TIMEOUT = 5
RATE_LIMIT_DELAY = 1  # 1 second between requests (Nominatim requires max 1 req/sec)

# User agent required by Nominatim
HEADERS = {
    'User-Agent': 'WikiTrendsPipeline/1.0 (Educational Project; Python/requests)'
}


@lru_cache(maxsize=500)
def geocode_location(location_name, location_type):
    """
    Geocode a location to get latitude and longitude
    
    Args:
        location_name: Name of the location (e.g., "Paris", "California", "United States")
        location_type: Type of location ("City", "State", "Country", "Region", "General")
    
    Returns:
        Tuple: (latitude, longitude) or (None, None) if not found
    """
    if not location_name or location_name == "No specific location":
        return (None, None)
    
    try:
        # Add delay to respect rate limits (1 req/sec)
        time.sleep(RATE_LIMIT_DELAY)
        
        # Query Nominatim
        params = {
            'q': location_name,
            'format': 'json',
            'limit': 1,
            'addressdetails': 0
        }
        
        response = requests.get(
            NOMINATIM_URL,
            params=params,
            headers=HEADERS,
            timeout=REQUEST_TIMEOUT
        )
        
        if response.status_code == 200:
            data = response.json()
            if data and len(data) > 0:
                lat = float(data[0]['lat'])
                lon = float(data[0]['lon'])
                return (lat, lon)
        
        return (None, None)
        
    except Exception as e:
        print(f"⚠️  Geocoding error for {location_name}: {str(e)}")
        return (None, None)


def batch_geocode_locations(locations_list):
    """
    Geocode multiple locations with progress tracking
    
    Args:
        locations_list: List of tuples [(location_name, location_type), ...]
    
    Returns:
        Dict: {location_name: (lat, lon), ...}
    """
    print(f"\n🌍 Geocoding {len(locations_list)} unique locations...")
    print(f"   ⏱️  Estimated time: ~{len(locations_list)} seconds (1 req/sec rate limit)")
    
    results = {}
    successful = 0
    failed = 0
    
    for idx, (location_name, location_type) in enumerate(locations_list, 1):
        lat, lon = geocode_location(location_name, location_type)
        results[location_name] = (lat, lon)
        
        if lat is not None and lon is not None:
            successful += 1
        else:
            failed += 1
        
        # Progress updates every 20 locations
        if idx % 20 == 0 or idx == len(locations_list):
            print(f"   ✓ Progress: {idx}/{len(locations_list)} | Success: {successful} | Failed: {failed}")
    
    print(f"\n📊 Geocoding Summary:")
    print(f"   • Total locations: {len(locations_list)}")
    print(f"   • Successfully geocoded: {successful} ({successful/len(locations_list)*100:.1f}%)")
    print(f"   • Failed: {failed}")
    
    return results


# Pre-defined coordinates for common locations (to speed up processing)
KNOWN_LOCATIONS = {
    # Countries
    "United States": (37.0902, -95.7129),
    "United Kingdom": (55.3781, -3.4360),
    "France": (46.2276, 2.2137),
    "Germany": (51.1657, 10.4515),
    "Italy": (41.8719, 12.5674),
    "Spain": (40.4637, -3.7492),
    "Russia": (61.5240, 105.3188),
    "China": (35.8617, 104.1954),
    "Japan": (36.2048, 138.2529),
    "India": (20.5937, 78.9629),
    "Brazil": (-14.2350, -51.9253),
    "Canada": (56.1304, -106.3468),
    "Australia": (-25.2744, 133.7751),
    "Mexico": (23.6345, -102.5528),
    "Argentina": (-38.4161, -63.6167),
    "South Korea": (35.9078, 127.7669),
    
    # Major cities
    "New York": (40.7128, -74.0060),
    "London": (51.5074, -0.1278),
    "Paris": (48.8566, 2.3522),
    "Tokyo": (35.6762, 139.6503),
    "Beijing": (39.9042, 116.4074),
    "Los Angeles": (34.0522, -118.2437),
    "Chicago": (41.8781, -87.6298),
    "San Francisco": (37.7749, -122.4194),
    "Mumbai": (19.0760, 72.8777),
    "Delhi": (28.7041, 77.1025),
    "Shanghai": (31.2304, 121.4737),
    "Moscow": (55.7558, 37.6173),
    "Sydney": (-33.8688, 151.2093),
    "Toronto": (43.6532, -79.3832),
    "Singapore": (1.3521, 103.8198),
    "Dubai": (25.2048, 55.2708),
    "Hong Kong": (22.3193, 114.1694),
    
    # US States
    "California": (36.7783, -119.4179),
    "Texas": (31.9686, -99.9018),
    "Florida": (27.6648, -81.5158),
    "New York": (42.1657, -74.9481),
    "Pennsylvania": (41.2033, -77.1945),
    "Illinois": (40.6331, -89.3985),
    "Ohio": (40.4173, -82.9071),
    "Georgia": (32.1656, -82.9001),
    "North Carolina": (35.7596, -79.0193),
    "Michigan": (44.3148, -85.6024),
}


def get_coordinates(location_name, location_type, use_cache=True):
    """
    Get coordinates for a location, using cache if available
    
    Args:
        location_name: Name of the location
        location_type: Type of location
        use_cache: Whether to use pre-defined coordinates cache
    
    Returns:
        Tuple: (latitude, longitude) or (None, None)
    """
    if not location_name or location_name == "No specific location":
        return (None, None)
    
    # Check cache first
    if use_cache and location_name in KNOWN_LOCATIONS:
        return KNOWN_LOCATIONS[location_name]
    
    # Otherwise geocode
    return geocode_location(location_name, location_type)
