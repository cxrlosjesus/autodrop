import asyncio
import json
from playwright.async_api import async_playwright

URL = "https://www.carspotpanama.com/encuentra-un-carro/"

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])
        page = await browser.new_page(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0 Safari/537.36")
        await page.goto(URL, wait_until="domcontentloaded", timeout=45000)
        await page.wait_for_timeout(6000)
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await page.wait_for_timeout(3000)

        # 1. Todos los hrefs únicos de la página
        all_links = await page.evaluate("""
            () => [...new Set(Array.from(document.querySelectorAll('a[href]')).map(a => a.href))]
                .filter(h => h.includes('carspotpanama'))
                .slice(0, 30)
        """)
        print("=== LINKS DEL DOMINIO ===")
        for l in all_links:
            print(" ", l)

        # 2. Cuántos elementos a hay en total
        total_a = await page.evaluate("() => document.querySelectorAll('a').length")
        print(f"\nTotal <a> tags: {total_a}")

        # 3. HTML del primer card de auto
        first_card = await page.evaluate("""
            () => {
                const cards = document.querySelectorAll('[class*="listing"], [class*="vehicle"], [class*="car-card"], article');
                return cards.length > 0
                    ? { count: cards.length, first_html: cards[0].outerHTML.slice(0, 800) }
                    : { count: 0, body_sample: document.body.innerHTML.slice(0, 1000) };
            }
        """)
        print("\n=== CARDS ENCONTRADOS ===")
        print(json.dumps(first_card, indent=2, ensure_ascii=False))

        # 4. Título de la página y URL final
        print(f"\nTítulo: {await page.title()}")
        print(f"URL final: {page.url}")

        await browser.close()

asyncio.run(main())
