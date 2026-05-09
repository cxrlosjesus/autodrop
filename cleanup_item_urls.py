"""
1. Elimina registros con URLs /item/{id} de Encuentra24 (duplicados con URLs inválidas).
2. Actualiza km y location_city en silver para slug URLs usando og:title de cada página.
   Encuentra24 siempre embebe el km real del anuncio en el og:title.
"""
import re
import time
import requests
from sqlalchemy import create_engine, text

DB_URL = "postgresql://autopulse:autopulse_secret@46.225.217.94:5432/autopulse"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/124.0.0.0 Safari/537.36"}
KM_MAX = 500_000  # km imposible para un auto de uso normal

engine = create_engine(DB_URL)


def fetch_og_title(url: str) -> str:
    try:
        r = requests.get(url, headers=HEADERS, timeout=12, allow_redirects=True)
        if r.url.rstrip("/") == "https://www.encuentra24.com":
            return ""
        m = re.search(r'property="og:title"\s+content="([^"]+)"', r.text)
        if not m:
            m = re.search(r'content="([^"]+)"\s+property="og:title"', r.text)
        return m.group(1) if m else ""
    except Exception:
        return ""


def parse_og_title(og_title: str):
    """Devuelve (km_int_or_None, location_or_None) desde og:title de Encuentra24."""
    km_m = re.search(r"(\d[\d,.]*)\s*km", og_title, re.IGNORECASE)
    loc_m = re.search(r" en ([^|]+)", og_title)

    km_num = None
    if km_m:
        try:
            # float maneja "119907.99" correctamente; sin él int("11990799") daría basura
            km_num = round(float(km_m.group(1).replace(",", "")))
        except ValueError:
            pass
        if km_num is not None and km_num > KM_MAX:
            km_num = None  # valor imposible — ignorar

    loc = loc_m.group(1).strip() if loc_m else None
    return km_num, loc


# item/ URLs ya eliminadas en run anterior — solo actualizar km/location

# ── Paso 2: actualizar km/location en slug entries ─────────────────
with engine.connect() as conn:
    rows = conn.execute(text("""
        SELECT listing_id, source_url, mileage_km, location_city
        FROM public_silver.silver_listings
        WHERE source_site = 'encuentra24'
          AND source_url NOT LIKE '%/item/%'
        ORDER BY listing_id
    """)).fetchall()

print(f"\n[update] {len(rows)} listings encuentra24 a verificar...")

BATCH_SIZE = 50  # guarda cada 50 cambios para no perder progreso si se corta

total_updated = 0
batch = []
skipped = 0

def flush_batch(b):
    if not b:
        return 0
    with engine.begin() as conn:
        for listing_id, km_num, loc in b:
            conn.execute(text("""
                UPDATE public_silver.silver_listings
                SET mileage_km    = :km,
                    location_city = :loc,
                    updated_at    = NOW()
                WHERE listing_id = :lid
            """), {"km": km_num, "loc": loc, "lid": listing_id})
    return len(b)

for i, (listing_id, source_url, db_km, db_loc) in enumerate(rows, 1):
    og = fetch_og_title(source_url)
    if not og:
        skipped += 1
        continue

    new_km, new_loc = parse_og_title(og)

    km_to_set  = new_km  if new_km  is not None else db_km
    loc_to_set = new_loc if new_loc is not None else db_loc

    changed = (new_km is not None and new_km != db_km) or \
              (new_loc is not None and new_loc != db_loc)

    if changed:
        batch.append((listing_id, km_to_set, loc_to_set))
        print(f"  [{i}/{len(rows)}] {source_url.split('/')[-1]:>10} | km: {db_km} -> {km_to_set} | loc: {db_loc!r} -> {loc_to_set!r}")

    if len(batch) >= BATCH_SIZE:
        total_updated += flush_batch(batch)
        batch = []
        print(f"  [checkpoint] {total_updated} actualizados hasta ahora...")

    time.sleep(0.3)

# flush final
total_updated += flush_batch(batch)
print(f"\n[done] {total_updated} registros actualizados | {skipped} omitidos")
print("Listo.")
