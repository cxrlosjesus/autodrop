import re, requests

headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/124.0.0.0 Safari/537.36"}

# IDs con valores imposibles
bad_ids = [
    ("32342747", "https://www.encuentra24.com/panama-es/autos-usados/-/32342747"),
    ("32342746", "https://www.encuentra24.com/panama-es/autos-usados/-/32342746"),
    ("31928834", "https://www.encuentra24.com/panama-es/autos-usados/bmw-x4-m40i-2018/31928834"),
]

for label, url in bad_ids:
    r = requests.get(url, headers=headers, timeout=12, allow_redirects=True)
    m = re.search(r'property="og:title"\s+content="([^"]+)"', r.text)
    if not m:
        m = re.search(r'content="([^"]+)"\s+property="og:title"', r.text)
    og = m.group(1) if m else "(no encontrado)"
    print(f"{label}: {og}")
