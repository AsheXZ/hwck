import requests
import pandas as pd
import spacy
import h3
import yt_dlp
import time
from datetime import datetime
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

# --- 1. SETUP & AUTO-DOWNLOADER ---
try:
    nlp = spacy.load("en_core_web_sm")
except OSError:
    print("Downloading NLP model...")
    from spacy.cli import download
    download("en_core_web_sm")
    nlp = spacy.load("en_core_web_sm")

# --- 2. CONFIGURATION ---
# Target Keywords
KERALA_DISTRICTS = [
    "Wayanad", "Idukki", "Palakkad", "Kannur", "Pathanamthitta", 
    "Kollam", "Kottayam", "Thrissur", "Malappuram", "Kozhikode", "Kasargod", "Thiruvananthapuram"
]

# Geocoder Setup
geolocator = Nominatim(user_agent="kerala_hwc_hybrid_miner_v3")
geocode_service = RateLimiter(geolocator.geocode, min_delay_seconds=1.0)
LOCATION_CACHE = {}

# --- 3. SOURCES: GDELT (News/Blogs) ---

def fetch_gdelt_data():
    """
    Queries the GDELT 2.0 Doc API.
    GDELT monitors news globally. We ask it for links about attacks in Kerala.
    """
    print("\n[Phase 1] Querying GDELT Project (Global News Aggregator)...")
    
    # Query: (Kerala) AND (Elephant OR Tiger OR Boar) AND (Attack OR Conflict)
    # GDELT supports complex boolean logic.
    base_url = "https://api.gdeltproject.org/api/v2/doc/doc"
    
    # We construct a query looking for Kerala + Animal Keywords + Conflict Keywords
    query = 'Kerala (elephant OR tiger OR boar OR gaur) (attack OR conflict OR rampage OR destroy)'
    
    params = {
        'query': query,
        'mode': 'artlist',  # Return list of articles
        'maxrecords': 250,  # Max allowed by GDELT per call
        'timespan': '3y',   # Look back 3 years (API limit varies, usually 3 months to 2 years)
        'format': 'json'
    }
    
    try:
        response = requests.get(base_url, params=params)
        data = response.json()
        
        if 'articles' in data:
            print(f"  > GDELT found {len(data['articles'])} records.")
            return data['articles']
        else:
            print("  > No records found in GDELT for this timeframe.")
            return []
    except Exception as e:
        print(f"  ! GDELT Error: {e}")
        return []

# --- 4. SOURCES: YOUTUBE (Citizen Reports) ---

def fetch_youtube_data():
    """
    Uses yt-dlp to scrape video metadata (not the video itself).
    Citizen reports often appear here first.
    """
    print("\n[Phase 2] Mining YouTube for Citizen Reports...")
    
    ydl_opts = {
        'quiet': True,
        'extract_flat': True, # Only get metadata, don't download video
        'dump_single_json': True,
        'ignoreerrors': True
    }
    
    results = []
    queries = ["Kerala elephant attack", "Kerala tiger attack", "Wayanad wildlife conflict", "Idukki wild boar"]
    
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        for q in queries:
            print(f"  > Searching YouTube for: '{q}'...")
            try:
                # ytsearch15: means get top 15 results
                info = ydl.extract_info(f"ytsearch20:{q}", download=False)
                if 'entries' in info:
                    results.extend(info['entries'])
            except Exception as e:
                continue
                
    print(f"  > YouTube found {len(results)} videos.")
    return results

# --- 5. PROCESSING: NLP & GEOCODING ---

def extract_location_from_text(text):
    """
    Uses NLP to find non-district GPEs (Villages/Towns) in title/description.
    """
    if not text: return None, None
    
    doc = nlp(text)
    
    # Extract GPE (Geo-Political Entities)
    locs = [ent.text for ent in doc.ents if ent.label_ == 'GPE']
    
    # Clean list
    ignore = ['Kerala', 'India', 'State', 'District', 'Forest', 'South', 'North', 'West', 'East', 'Wild', 'News']
    candidates = [l for l in locs if l not in ignore and l not in KERALA_DISTRICTS]
    
    # Find Context (Which district is this in?)
    district_context = None
    for dist in KERALA_DISTRICTS:
        if dist in text:
            district_context = dist
            break
    
    if candidates:
        # Return most likely village + the district context found
        return max(set(candidates), key=candidates.count), district_context
    
    return None, district_context

def get_lat_lon(location_name, district_hint):
    """
    Geocodes with caching.
    """
    if not location_name: return None, None
    
    key = f"{location_name}_{district_hint}"
    if key in LOCATION_CACHE: return LOCATION_CACHE[key]
    
    search_query = f"{location_name}, {district_hint if district_hint else ''}, Kerala"
    
    try:
        loc = geocode_service(search_query)
        if loc and 8.0 < loc.latitude < 13.0: # Kerala Lat Bounds
            LOCATION_CACHE[key] = (loc.latitude, loc.longitude)
            return loc.latitude, loc.longitude
    except:
        pass
        
    return None, None

# --- 6. MAIN PIPELINE ---

def run_hybrid_miner():
    all_events = []
    
    # A. Fetch GDELT Data
    gdelt_raw = fetch_gdelt_data()
    for item in gdelt_raw:
        try:
            date_str = item.get('seendate', '')[:8] # YYYYMMDD
            fmt_date = datetime.strptime(date_str, "%Y%m%d").strftime("%Y-%m-%d")
        except:
            fmt_date = "Unknown"
            
        # FIX: Ensure title is a string
        title_text = item.get('title') or ""
        
        all_events.append({
            'text': title_text,
            'date': fmt_date,
            'source_type': 'News/Blog',
            'url': item.get('url', '')
        })

    # B. Fetch YouTube Data
    yt_raw = fetch_youtube_data()
    for item in yt_raw:
        try:
            date_str = item.get('upload_date', '')
            fmt_date = datetime.strptime(date_str, "%Y%m%d").strftime("%Y-%m-%d") if date_str else "Unknown"
        except:
            fmt_date = "Unknown"
            
        # FIX: Handle NoneType explicitly before slicing
        title = item.get('title') or ""
        desc = item.get('description') or "" # Converts None to ""
            
        all_events.append({
            'text': f"{title} {desc[:100]}", 
            'date': fmt_date,
            'source_type': 'Citizen Video',
            'url': item.get('webpage_url', item.get('url', ''))
        })
        
    # C. Process & Geocode
    print(f"\n[Phase 3] Processing {len(all_events)} items for geospatial data...")
    final_data = []
    
    for event in all_events:
        loc, district = extract_location_from_text(event['text'])
        
        if loc:
            lat, lon = get_lat_lon(loc, district)
            
            if lat:
                # Identify Species from text
                species = "Unknown"
                text_lower = event['text'].lower()
                if 'elephant' in text_lower: species = 'Elephant'
                elif 'tiger' in text_lower: species = 'Tiger'
                elif 'boar' in text_lower: species = 'Wild Boar'
                elif 'gaur' in text_lower or 'bison' in text_lower: species = 'Gaur'
                elif 'leopard' in text_lower: species = 'Leopard'
                
                # H3 Indexing
                # New (v4)
                hex_id = h3.latlng_to_cell(lat, lon, 8)
                
                final_data.append({
                    'event_date': event['date'],
                    'hex_id': hex_id,
                    'latitude': lat,
                    'longitude': lon,
                    'location_name': loc,
                    'district_context': district,
                    'species': species,
                    'source_type': event['source_type'],
                    'description': event['text'][:100], 
                    'url': event['url']
                })
                print(f"  > [MATCH] {species} @ {loc} ({event['date']})")

    # D. Save
    if final_data:
        df = pd.DataFrame(final_data)
        # Sort by date
        try:
            df = df.sort_values(by='event_date', ascending=False)
        except:
            pass # Use default order if sorting fails
            
        filename = "kerala_hwc_hybrid_data.csv"
        df.to_csv(filename, index=False)
        print(f"\nSUCCESS: Saved {len(df)} geocoded events to '{filename}'")
        if not df.empty:
            print(df[['event_date', 'location_name', 'species', 'source_type']].head())
    else:
        print("\nNo geospatial matches found. Try widening the query list.")

if __name__ == "__main__":
    run_hybrid_miner()