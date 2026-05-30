from pathlib import Path
import time
import requests
import pandas as pd

INPUT_FILE = Path("data/cities/miasta.csv")
OUTPUT_FILE = Path("data/cities/miasta.csv")

df = pd.read_csv(INPUT_FILE)

results = []

for city in df["city"].dropna().unique():
    query = f"{city}, małopolskie, Polska"

    print(f"Geocoding: {query}")

    response = requests.get(
        "https://nominatim.openstreetmap.org/search",
        params={
            "q": query,
            "format": "jsonv2",
            "limit": 1,
            "countrycodes": "pl",
        },
        headers={
            "User-Agent": "nfz-waiting-times-project"
        },
        timeout=30,
    )

    data = response.json()

    if data:
        results.append({
            "city": city,
            "voivodeship_name": "MAŁOPOLSKIE",
            "latitude": data[0]["lat"],
            "longitude": data[0]["lon"],
            "source_name": "nominatim",
        })
    else:
        results.append({
            "city": city,
            "voivodeship_name": "MAŁOPOLSKIE",
            "latitude": None,
            "longitude": None,
            "source_name": "not_found",
        })

    time.sleep(1)

out = pd.DataFrame(results)
out.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")

print(f"Saved: {OUTPUT_FILE}")