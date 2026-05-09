"""Valida la extracción og:title contra los 3 Maserati Levante 2018 del card."""
import re
import requests
from sqlalchemy import create_engine, text

DB_URL = "postgresql://autopulse:autopulse_secret@46.225.217.94:5432/autopulse"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/124.0.0.0 Safari/537.36"}

engine = create_engine(DB_URL)


def fetch_og_title(url: str) -> str:
    r = requests.get(url, headers=HEADERS, timeout=12, allow_redirects=True)
    if r.url.rstrip("/") == "https://www.encuentra24.com":
        return "(URL muerta — redirige al homepage)"
    m = re.search(r'property="og:title"\s+content="([^"]+)"', r.text)
    if not m:
        m = re.search(r'content="([^"]+)"\s+property="og:title"', r.text)
    return m.group(1) if m else "(og:title no encontrado)"


def parse_og_title(og_title: str):
    km_m  = re.search(r"(\d[\d,.]*)\s*km", og_title, re.IGNORECASE)
    loc_m = re.search(r" en ([^|]+)", og_title)
    km  = km_m.group(1) if km_m else None
    loc = loc_m.group(1).strip() if loc_m else None
    return km, loc


with engine.connect() as conn:
    rows = conn.execute(text("""
        SELECT source_url, mileage_km, price_usd, location_city
        FROM public_silver.silver_listings
        WHERE brand ILIKE '%maserati%'
          AND model ILIKE '%levante%'
          AND year = 2018
        ORDER BY price_usd
    """)).fetchall()

print(f"Maserati Levante 2018 en DB: {len(rows)}\n")
for source_url, db_km, price, db_loc in rows:
    og = fetch_og_title(source_url)
    km, loc = parse_og_title(og)
    match_km = str(db_km) in (km or "").replace(",", "") if km else False
    print(f"URL: {source_url}")
    print(f"  og:title   : {og}")
    print(f"  km  DB={db_km:>7} | og={km!r:>10} | {'OK' if match_km else 'DIFF'}")
    print(f"  loc DB={db_loc!r} | og={loc!r}")
    print()
