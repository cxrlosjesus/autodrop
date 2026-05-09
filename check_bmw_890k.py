import re, requests

url = "https://www.encuentra24.com/panama-es/autos-usados/bmw-x4-del-2018-excelente-condiciones/30574596"
headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/124.0.0.0 Safari/537.36"}
r = requests.get(url, headers=headers, timeout=12, allow_redirects=True)

m = re.search(r'property="og:title"\s+content="([^"]+)"', r.text)
if not m:
    m = re.search(r'content="([^"]+)"\s+property="og:title"', r.text)
print("og:title:", m.group(1) if m else "(no encontrado)")
print("final url:", r.url)
