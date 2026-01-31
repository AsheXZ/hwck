import pandas as pd
import re
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

# Load data
df = pd.read_csv("validation-set.csv")

geolocator = Nominatim(user_agent="hwc_geocoder_kerala")
geocode = RateLimiter(geolocator.geocode, min_delay_seconds=0.5)

# Simple cache to avoid repeated calls
cache = {}

def normalize(text):
    if pd.isna(text):
        return ""
    text = text.lower()
    text = re.sub(r"\(.*?\)", "", text)   # remove brackets
    text = re.sub(r"[-/]", " ", text)     # split hybrids
    text = re.sub(r"\s+", " ", text)
    return text.strip()

def generate_queries(row):
    place = normalize(row["place"])
    range_ = normalize(row["range"])
    district = normalize(row["district"])

    return [
        f"{place}, {range_}, {district}, Kerala, India",
        f"{place}, {district}, Kerala, India",
        f"{place}, Kerala, India",
        f"{range_}, {district}, Kerala, India",
        f"{district}, Kerala, India"
    ]

def geocode_row(idx, row):
    print(f"\n[{idx+1}/{len(df)}] Processing: {row['place']}")

    queries = generate_queries(row)

    for q in queries:
        if not q.strip():
            continue

        if q in cache:
            lat, lon = cache[q]
            if lat is not None:
                print(f"  ✓ Cached hit → {q}")
                return lat, lon
            continue

        print(f"  → Trying: {q}")
        loc = geocode(q)

        if loc:
            lat, lon = loc.latitude, loc.longitude
            print(f"    ✓ Found: ({lat:.6f}, {lon:.6f})")
            cache[q] = (lat, lon)
            return lat, lon
        else:
            cache[q] = (None, None)

    print("    ✗ All strategies failed")
    return None, None

# Apply geocoding
df[["lat", "long"]] = pd.DataFrame(
    [geocode_row(i, row) for i, row in df.iterrows()],
    index=df.index
)

df.to_csv("conflict_locations_geocoded.csv", index=False)

print("\nGeocoding finished.")
print(f"Success rate: {df['lat'].notna().mean()*100:.2f}%")
