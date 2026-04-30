"""
Inspección profunda de automarketpanama.com
- Estructura interna de un card
- Página de detalle por placa
- Si hay API requests en background
"""
import asyncio
import json
from playwright.async_api import async_playwright

LISTING_URL = "https://automarketpanama.com/public/inventario"
DETAIL_URL  = "https://automarketpanama.com/public/detail?placa=EA9283"

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

        # Interceptar requests de red para detectar APIs
        api_calls = []
        def on_request(request):
            if any(x in request.url for x in ["api", "json", "ajax", "inventario", "detail", "placa"]):
                api_calls.append({"url": request.url, "method": request.method})

        page = await context.new_page()
        page.on("request", on_request)

        # ── LISTADO ──────────────────────────────────────────
        print("\n" + "="*60)
        print("LISTADO: " + LISTING_URL)
        print("="*60)

        await page.goto(LISTING_URL, wait_until="domcontentloaded", timeout=45000)
        await page.wait_for_timeout(5000)

        # HTML completo del primer card
        first_card = await page.evaluate("""
            () => {
                const card = document.querySelector('.card');
                return card ? card.outerHTML.slice(0, 1500) : 'No encontrado';
            }
        """)
        print("\n=== HTML primer card ===")
        print(first_card)

        # Extraer todos los links de detalle de la página
        detail_links = await page.evaluate("""
            () => {
                const links = document.querySelectorAll('a[href*="placa="], a[href*="detail"]');
                return Array.from(links).map(a => ({
                    href: a.href,
                    text: a.innerText.trim().slice(0, 100),
                    parent_class: a.parentElement?.className?.slice(0, 80)
                })).slice(0, 10);
            }
        """)
        print("\n=== Links de detalle encontrados ===")
        print(json.dumps(detail_links, indent=2, ensure_ascii=False))

        # Total de cards y placas en la página
        cards_data = await page.evaluate("""
            () => {
                const allLinks = document.querySelectorAll('a[href*="placa="]');
                return {
                    total_detail_links: allLinks.length,
                    placas: Array.from(allLinks).slice(0, 8).map(a => a.href)
                };
            }
        """)
        print("\n=== Total links de detalle ===")
        print(json.dumps(cards_data, indent=2, ensure_ascii=False))

        print("\n=== API calls detectados en listado ===")
        print(json.dumps(api_calls[:10], indent=2, ensure_ascii=False))
        api_calls.clear()

        # ── DETALLE ──────────────────────────────────────────
        print("\n" + "="*60)
        print("DETALLE: " + DETAIL_URL)
        print("="*60)

        await page.goto(DETAIL_URL, wait_until="domcontentloaded", timeout=45000)
        await page.wait_for_timeout(4000)

        detail_data = await page.evaluate("""
            () => {
                // Título
                const h1 = document.querySelector('h1, h2, [class*="title"]');
                const title = h1 ? h1.innerText.trim() : '';

                // Precio
                const priceEls = document.querySelectorAll(
                    '[class*="price"], [class*="precio"], b, strong'
                );
                const prices = Array.from(priceEls)
                    .map(el => el.innerText.trim())
                    .filter(t => t.includes('$') || /\\d{4,}/.test(t))
                    .slice(0, 5);

                // Specs: buscar tabla, dl, listas
                const specs = {};

                // Patrón tabla
                document.querySelectorAll('table tr').forEach(tr => {
                    const cells = tr.querySelectorAll('td, th');
                    if (cells.length === 2) {
                        specs[cells[0].innerText.trim()] = cells[1].innerText.trim();
                    }
                });

                // Patrón dl
                document.querySelectorAll('dl dt, dl dd').forEach((el, i, arr) => {
                    if (el.tagName === 'DT') {
                        const dd = arr[i+1];
                        if (dd) specs[el.innerText.trim()] = dd.innerText.trim();
                    }
                });

                // Patrón div label/value
                document.querySelectorAll('[class*="spec"] li, [class*="detail"] li, [class*="info"] li').forEach(li => {
                    const text = li.innerText.replace(/\\s+/g, ' ').trim();
                    const idx = text.indexOf(':');
                    if (idx > 0) specs[text.slice(0, idx).trim()] = text.slice(idx+1).trim();
                });

                // Descripción
                const desc = Array.from(document.querySelectorAll('p'))
                    .map(p => p.innerText.trim())
                    .filter(t => t.length > 50)
                    .join(' ').slice(0, 500);

                return { title, prices, specs, description: desc };
            }
        """)
        print("\n=== Datos del detalle ===")
        print(json.dumps(detail_data, indent=2, ensure_ascii=False))

        # HTML crudo de la sección principal del detalle
        detail_html = await page.evaluate("""
            () => {
                const main = document.querySelector('main, [class*="detail"], [class*="single"], article');
                return main ? main.outerHTML.slice(0, 2000) : document.body.innerHTML.slice(0, 2000);
            }
        """)
        print("\n=== HTML sección principal detalle ===")
        print(detail_html[:2000])

        print("\n=== API calls detectados en detalle ===")
        print(json.dumps(api_calls[:10], indent=2, ensure_ascii=False))

        await browser.close()
        print("\n✅ Inspección 2 completada")

asyncio.run(main())
