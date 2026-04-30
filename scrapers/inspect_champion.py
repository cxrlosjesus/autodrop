import asyncio
import json
from playwright.async_api import async_playwright

DETAIL_URL = "https://championmotors.com.pa/cars/nissan/qashqai/1217"

def inspect_html():
    import urllib.request
    import re
    req = urllib.request.Request(DETAIL_URL, headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36"})
    html = urllib.request.urlopen(req, timeout=15).read().decode("utf-8", errors="ignore")

    # Extraer sección del precio (entre los botones de acción y la sección de detalles)
    # Buscar el bloque que tiene el precio
    price_match = re.search(r'([\s\S]{0,500})12,899([\s\S]{0,200})', html)
    if price_match:
        snippet = price_match.group(0)
        print("=== HTML ALREDEDOR DE $12,899 ===")
        print(snippet[:600])

    # Buscar ul.details-list
    details_match = re.search(r'<ul class="details-list[^"]*">([\s\S]*?)</ul>', html)
    if details_match:
        print("\n=== details-list HTML ===")
        print(details_match.group(0)[:2000])

    # Buscar title/h1
    h1_match = re.search(r'<h1[^>]*>([\s\S]*?)</h1>', html)
    if h1_match:
        print(f"\n=== H1 ===\n{h1_match.group(0)[:200]}")

    # Buscar descripcion (párrafos principales)
    desc_matches = re.findall(r'<p[^>]*>([\s\S]{20,500}?)</p>', html)
    if desc_matches:
        print(f"\n=== PARRAFOS ===")
        for m in desc_matches[:3]:
            print(m[:200])
            print("---")

inspect_html()

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"]
        )
        page = await browser.new_page()
        await page.set_extra_http_headers({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36"
        })
        await page.goto(DETAIL_URL, wait_until="domcontentloaded", timeout=30000)
        await page.wait_for_timeout(4000)

        # Obtener HTML de la sección principal (sin sidebar ni relacionados)
        html_snippet = await page.evaluate("""
            () => {
                // Buscar el elemento que contiene el precio principal y los detalles
                // Estrategia: buscar el section/div más cercano que tenga tanto precio como año
                const allEls = document.querySelectorAll('section, main, article, [class*=detail], [class*=single], [class*=car-info]');
                for (const el of allEls) {
                    const t = el.innerText || '';
                    if (t.includes('Año:') && t.includes('Kilometraje:')) {
                        return {
                            tag: el.tagName,
                            cls: el.className.substring(0, 80),
                            html: el.outerHTML.substring(0, 2000)
                        };
                    }
                }
                return null;
            }
        """)
        print("=== SECTION CON SPECS ===")
        print(json.dumps(html_snippet, indent=2, ensure_ascii=False))

        # Ver el HTML alrededor del precio principal $12,899
        price_ctx = await page.evaluate("""
            () => {
                // Buscar el texto '$' seguido del precio del auto principal
                // Sabemos que el precio es $12,899 y aparece antes de Detalles
                const allDivs = document.querySelectorAll('*');
                for (const el of allDivs) {
                    if (el.children.length === 0) {
                        const t = (el.textContent || '').trim();
                        if (t.startsWith('$') && t.length < 15) {
                            const parent = el.parentElement;
                            const gp = parent ? parent.parentElement : null;
                            return {
                                text: t,
                                tag: el.tagName,
                                cls: el.className.substring(0, 80),
                                parent_cls: parent ? parent.className.substring(0, 80) : '',
                                grandparent_cls: gp ? gp.className.substring(0, 80) : '',
                                parent_html: parent ? parent.outerHTML.substring(0, 300) : ''
                            };
                        }
                    }
                }
                return null;
            }
        """)
        print("\n=== PRECIO PRINCIPAL ===")
        print(json.dumps(price_ctx, indent=2, ensure_ascii=False))

        # Ver estructura de la lista de detalles (key: value)
        specs_html = await page.evaluate("""
            () => {
                const els = document.querySelectorAll('*');
                for (const el of els) {
                    const t = (el.innerText || '').trim();
                    if (t.startsWith('Año:') || (t.includes('Año:') && t.includes('Kilometraje:') && t.length < 500)) {
                        return {
                            tag: el.tagName,
                            cls: el.className.substring(0, 80),
                            html: el.outerHTML.substring(0, 1500)
                        };
                    }
                }
                return null;
            }
        """)
        print("\n=== SPECS HTML ===")
        print(json.dumps(specs_html, indent=2, ensure_ascii=False))

        # Verificar si el sitio funciona sin JS (via requests)
        import urllib.request
        try:
            req = urllib.request.Request(DETAIL_URL, headers={"User-Agent": "Mozilla/5.0"})
            resp = urllib.request.urlopen(req, timeout=10)
            html = resp.read().decode("utf-8", errors="ignore")
            has_year = "Año:" in html
            has_price = "12,899" in html
            print(f"\n=== SIN JS: año_en_html={has_year}, precio_en_html={has_price} ===")
        except Exception as e:
            print(f"\n=== SIN JS: error={e} ===")

        await browser.close()

asyncio.run(main())
