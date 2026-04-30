"""
Inspección de automarketpanama.com/public/inventario
Corre dentro del contenedor: docker compose run --rm scrapy python inspect_automarket.py
"""
import asyncio
import json
from playwright.async_api import async_playwright

LISTING_URL = "https://automarketpanama.com/public/inventario"

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-blink-features=AutomationControlled"]
        )
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
            extra_http_headers={"Accept-Language": "es-PA,es;q=0.9"}
        )
        page = await context.new_page()

        print(f"\n{'='*60}")
        print(f"Cargando: {LISTING_URL}")
        print('='*60)

        await page.goto(LISTING_URL, wait_until="domcontentloaded", timeout=45000)
        await page.wait_for_timeout(5000)

        # 1. Título de la página y URL final
        print(f"\nTítulo: {await page.title()}")
        print(f"URL final: {page.url}")

        # 2. Cards de autos — buscar patrones comunes
        card_info = await page.evaluate("""
            () => {
                const results = [];

                // Intentar varios selectores comunes
                const selectors = [
                    'a[href*="/inventario/"]',
                    'a[href*="/auto/"]',
                    'a[href*="/vehiculo/"]',
                    'a[href*="/detalle/"]',
                    '.card a',
                    '.vehicle-card',
                    '[class*="car"] a',
                    '[class*="vehicle"] a',
                    '[class*="listing"] a',
                    '[class*="item"] a',
                ];

                for (const sel of selectors) {
                    const els = document.querySelectorAll(sel);
                    if (els.length > 0) {
                        results.push({
                            selector: sel,
                            count: els.length,
                            sample_hrefs: Array.from(els).slice(0, 3).map(e => e.href)
                        });
                    }
                }
                return results;
            }
        """)
        print("\n=== Links encontrados ===")
        print(json.dumps(card_info, indent=2, ensure_ascii=False))

        # 3. HTML de los primeros cards para ver estructura
        cards_html = await page.evaluate("""
            () => {
                // Buscar el contenedor principal de cards
                const containers = [
                    '[class*="grid"]',
                    '[class*="list"]',
                    '[class*="inventory"]',
                    '[class*="cars"]',
                    '[class*="vehicles"]',
                    'main',
                ];
                for (const sel of containers) {
                    const el = document.querySelector(sel);
                    if (el && el.children.length > 2) {
                        const first = el.children[0];
                        return {
                            container_selector: sel,
                            children_count: el.children.length,
                            first_child_html: first.outerHTML.slice(0, 800)
                        };
                    }
                }
                return { body_preview: document.body.innerHTML.slice(0, 1000) };
            }
        """)
        print("\n=== Estructura del contenedor ===")
        print(json.dumps(cards_html, indent=2, ensure_ascii=False))

        # 4. Paginación
        pagination = await page.evaluate("""
            () => {
                const selectors = [
                    '[class*="pagination"] a',
                    '[class*="page"] a',
                    'nav a',
                    'a[href*="page="]',
                    'a[href*="pagina="]',
                    'a[href*="/page/"]',
                ];
                for (const sel of selectors) {
                    const els = document.querySelectorAll(sel);
                    if (els.length > 0) {
                        return {
                            selector: sel,
                            count: els.length,
                            hrefs: Array.from(els).slice(0, 5).map(e => e.href)
                        };
                    }
                }
                return null;
            }
        """)
        print("\n=== Paginación ===")
        print(json.dumps(pagination, indent=2, ensure_ascii=False))

        # 5. Todos los links únicos de la página (para detectar patrón de detalle)
        all_links = await page.evaluate("""
            () => {
                const hrefs = Array.from(document.querySelectorAll('a[href]'))
                    .map(a => a.href)
                    .filter(h => h.includes('automarketpanama'));
                return [...new Set(hrefs)].slice(0, 20);
            }
        """)
        print("\n=== Todos los links del dominio (primeros 20) ===")
        for link in all_links:
            print(f"  {link}")

        # 6. Precio e info en el primer card visible
        first_card_data = await page.evaluate("""
            () => {
                const priceEls = document.querySelectorAll('[class*="price"], [class*="precio"]');
                const prices = Array.from(priceEls).slice(0, 3).map(el => ({
                    selector: el.className,
                    text: el.innerText.trim()
                }));

                const imgEls = document.querySelectorAll('img[alt]');
                const imgs = Array.from(imgEls).slice(0, 3).map(img => img.alt);

                return { prices, img_alts: imgs };
            }
        """)
        print("\n=== Precios e imágenes detectados ===")
        print(json.dumps(first_card_data, indent=2, ensure_ascii=False))

        await browser.close()
        print("\n✅ Inspección completada")

asyncio.run(main())
