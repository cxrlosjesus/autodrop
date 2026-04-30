import asyncio
import json
from playwright.async_api import async_playwright

URL = "https://www.encuentra24.com/panama-es/autos-usados/byd-seal-2024-electric-awd-dual-motor/32273463"

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"]
        )
        page = await browser.new_page()
        await page.set_extra_http_headers({
            "Accept-Language": "es-PA,es;q=0.9",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36"
        })
        await page.goto(URL, wait_until="domcontentloaded", timeout=30000)
        await page.wait_for_timeout(3000)

        # JSON-LD completo
        json_lds = await page.evaluate("""
            () => Array.from(document.querySelectorAll('script[type="application/ld+json"]'))
                       .map(s => s.innerText)
        """)
        for raw in json_lds:
            try:
                data = json.loads(raw)
                if isinstance(data, dict) and data.get("@type") in ("Car", "Vehicle", "Product"):
                    print("=== JSON-LD completo ===")
                    print(json.dumps(data, indent=2, ensure_ascii=False))
            except:
                pass

        # Precio del anuncio principal (NO de los cards relacionados)
        main_price = await page.evaluate("""
            () => {
                // Buscar precio fuera de la sección de recomendados
                const selectors = [
                    'h2[class*="price"]',
                    'span[class*="detailPrice"]',
                    '[class*="listing"] [class*="price"]',
                    '[class*="adDetail"] [class*="price"]',
                    'main [class*="price"]',
                ];
                for (const sel of selectors) {
                    const el = document.querySelector(sel);
                    if (el) return { selector: sel, text: el.innerText };
                }
                // Fallback: primer elemento con precio que sea h2/h3
                for (const tag of ['h2','h3','h4']) {
                    const els = document.querySelectorAll(tag);
                    for (const el of els) {
                        if (el.innerText.includes('$') && el.innerText.trim().length < 20) {
                            return { selector: tag, text: el.innerText };
                        }
                    }
                }
                return null;
            }
        """)
        print(f"\n=== Precio principal ===")
        print(main_price)

        # Atributos estructurados del anuncio (marca, modelo, año, km)
        attrs = await page.evaluate("""
            () => {
                const result = {};
                // Buscar dl/dt/dd o listas de specs
                document.querySelectorAll('dl dt, dl dd').forEach((el, i, arr) => {
                    if (el.tagName === 'DT') {
                        const dd = arr[i+1];
                        if (dd) result[el.innerText.trim()] = dd.innerText.trim();
                    }
                });
                // Buscar divs con label/value pattern
                document.querySelectorAll('[class*="spec"], [class*="attr"], [class*="feature"]').forEach(el => {
                    const children = el.children;
                    if (children.length === 2) {
                        result[children[0].innerText.trim()] = children[1].innerText.trim();
                    }
                });
                return result;
            }
        """)
        print(f"\n=== Atributos estructurados ===")
        print(json.dumps(attrs, indent=2, ensure_ascii=False))

        await browser.close()

asyncio.run(main())
