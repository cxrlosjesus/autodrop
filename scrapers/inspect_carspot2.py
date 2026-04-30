import asyncio
import json
from playwright.async_api import async_playwright

URL = "https://www.carspotpanama.com/encuentra-un-carro/"

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])
        page = await browser.new_page(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0 Safari/537.36")

        # Interceptar TODAS las requests para encontrar el endpoint AJAX
        ajax_calls = []
        async def on_request(req):
            if req.method == "POST" or any(x in req.url for x in ["ajax", "wp-admin", "listings", "query", "cars", "inventory"]):
                ajax_calls.append({"url": req.url, "method": req.method, "post_data": req.post_data})

        ajax_responses = []
        async def on_response(resp):
            if resp.request.method == "POST" or any(x in resp.url for x in ["ajax", "wp-admin"]):
                try:
                    body = await resp.text()
                    if len(body) > 100:
                        ajax_responses.append({"url": resp.url, "status": resp.status, "body_sample": body[:500]})
                except:
                    pass

        page.on("request", on_request)
        page.on("response", on_response)

        await page.goto(URL, wait_until="domcontentloaded", timeout=45000)
        await page.wait_for_timeout(8000)

        print("=== AJAX CALLS POST ===")
        for call in ajax_calls[:10]:
            print(json.dumps(call, indent=2, ensure_ascii=False))

        print("\n=== AJAX RESPONSES ===")
        for resp in ajax_responses[:5]:
            print(json.dumps(resp, indent=2, ensure_ascii=False))

        # Buscar links de listings después de la carga AJAX
        listing_links = await page.evaluate("""
            () => {
                const all = Array.from(document.querySelectorAll('a[href]')).map(a => a.href);
                return [...new Set(all)].filter(h => h.includes('carspotpanama') && !h.includes('#'));
            }
        """)
        print("\n=== LINKS DESPUÉS DE AJAX ===")
        for l in listing_links[:20]:
            print(" ", l)

        # Ver el HTML de los cards reales
        cards_sample = await page.evaluate("""
            () => {
                const links = document.querySelectorAll('a[href*="listings"]');
                return {
                    count: links.length,
                    samples: Array.from(links).slice(0, 3).map(a => ({href: a.href, text: a.innerText.slice(0,80)}))
                };
            }
        """)
        print("\n=== CARDS CON /listings/ ===")
        print(json.dumps(cards_sample, indent=2, ensure_ascii=False))

        await browser.close()

asyncio.run(main())
