"""
AutoPulse Panamá — Spider: CarSpot Panamá
URL: carspotpanama.com/encuentra-un-carro/

Estructura del sitio:
    - WordPress con plugin de listings de autos
    - JavaScript-rendered (requiere Playwright — el HTML sin JS no tiene listings)
    - 173 vehículos en inventario (Tumba Muerto y Chiriquí)
    - Paginación WordPress: /encuentra-un-carro/page/N/
    - URL de detalle: /listings/[marca]-[modelo]-[año]-[numero]/

Estrategia:
    1. Cargar listado con Playwright y extraer URLs de detalle via JS
    2. Paginar incrementando /page/N/ hasta que no haya más links
    3. En cada detalle, extraer specs del bloque de especificaciones via JS
"""
import re

from playwright.async_api import Page
from scrapy_playwright.page import PageMethod

from .base import AutoPulseSpider


class CarspotSpider(AutoPulseSpider):
    name = "carspot"
    version = "2.0.0"
    uses_playwright = True

    START_URL = "https://www.carspotpanama.com/encuentra-un-carro/"

    custom_settings = {
        "DOWNLOAD_DELAY": 2.0,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 2,
        "PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT": 30000,
        "CLOSESPIDER_TIMEOUT": 900,    # Máximo 15 minutos
        "HTTPCACHE_ENABLED": False,
    }

    def start_requests(self):
        yield self.playwright_request(
            url=self.START_URL,
            callback=self.parse_listing_page,
            page_methods=[
                # El plugin STM Car Dealer carga listings via AJAX — necesita ~8s
                PageMethod("wait_for_timeout", 8000),
                PageMethod("evaluate", "window.scrollTo(0, document.body.scrollHeight)"),
                PageMethod("wait_for_timeout", 2000),
            ]
        )

    async def parse_listing_page(self, response):
        page: Page = response.meta.get("playwright_page")

        if page is None:
            self.logger.error("playwright_page es None")
            return

        try:
            # Todo el inventario (173 autos) carga en una sola página via AJAX
            # El card text ya contiene: título, transmisión, km y precio
            cards = await page.evaluate("""
                () => {
                    const seen = new Set();
                    const results = [];

                    document.querySelectorAll('a[href*="/listings/"]').forEach(a => {
                        const href = a.href;
                        if (seen.has(href)) return;
                        seen.add(href);

                        results.push({
                            url: href,
                            card_text: a.innerText.replace(/\\s+/g, ' ').trim()
                        });
                    });

                    return results;
                }
            """)

            self.logger.info(f"[carspot] {len(cards)} autos encontrados")

            for card in cards:
                yield self.playwright_request(
                    url=card["url"],
                    callback=self.parse_detail,
                    meta={"card_data": card},
                    page_methods=[
                        PageMethod("wait_for_load_state", "domcontentloaded"),
                        PageMethod("wait_for_timeout", 2000),
                    ]
                )

        finally:
            if page:
                await page.close()

    async def parse_detail(self, response):
        page: Page = response.meta.get("playwright_page")
        url = response.url

        try:
            data = await page.evaluate("""
                () => {
                    const h1 = document.querySelector('h1');
                    const title = h1 ? h1.innerText.trim() : '';

                    const specs = {};
                    document.querySelectorAll('ul li, dl dt, tr, [class*="detail"] div').forEach(el => {
                        const txt = el.innerText.replace(/\\s+/g, ' ').trim();
                        const idx = txt.indexOf(':');
                        if (idx > 0 && idx < 35) {
                            const k = txt.slice(0, idx).trim().toLowerCase();
                            const v = txt.slice(idx + 1).trim();
                            if (k && v && v.length < 80) specs[k] = v;
                        }
                    });

                    const desc = Array.from(document.querySelectorAll('p'))
                        .map(p => p.innerText.trim())
                        .filter(t => t.length > 40)
                        .join(' ');

                    return { title, specs, description: desc };
                }
            """)

            title     = data.get("title", "")
            specs     = data.get("specs", {})
            card_data = response.meta.get("card_data", {})
            card      = self._parse_card_text(card_data.get("card_text", ""))

            item = self.create_item(
                source_url   = url,
                title        = title or card_data.get("card_text", "").split("\n")[0],
                price_raw    = card.get("price", ""),
                brand_raw    = specs.get("marca", "") or self._brand_from_title(title),
                model_raw    = specs.get("modelo", ""),
                year_raw     = specs.get("año", "") or self._extract_year(title),
                mileage_raw  = card.get("mileage") or specs.get("km", specs.get("kilometraje", "")),
                transmission = self._normalize_transmission(
                    card.get("transmission") or specs.get("transmisión", specs.get("transmision", ""))
                ),
                body_type    = specs.get("carrocería", specs.get("carroceria", specs.get("tipo", ""))),
                fuel_type    = specs.get("combustible", ""),
                color        = specs.get("colores", specs.get("color", "")),
                condition    = self._normalize_condition(
                    specs.get("condición", specs.get("condicion", ""))
                ) or "Usado",
                location_city = specs.get("tienda", specs.get("ubicación", "")),
                description  = data.get("description", ""),
                extra_data   = {"specs": specs},
            )

            self.track_listing()
            yield item

        except Exception as e:
            self._errors_count += 1
            self.logger.error(f"Error parseando {url}: {e}")
        finally:
            if page:
                await page.close()

    # ── Helpers ──────────────────────────────────────────────────────

    def _parse_card_text(self, text: str) -> dict:
        """
        Parsea texto del card del listado.
        Formato: "Kia Seltos 2024\nautomatica 59,352 km\n$18,995\n$150/quincenal*"
        """
        result = {}
        price_match = re.search(r"\$\s*([\d,]+)", text)
        if price_match:
            result["price"] = price_match.group(0)
        km_match = re.search(r"([\d,]+)\s*km", text, re.IGNORECASE)
        if km_match:
            result["mileage"] = km_match.group(0)
        if re.search(r"automatica|automático|automatic", text, re.IGNORECASE):
            result["transmission"] = "Automatico"
        elif re.search(r"manual", text, re.IGNORECASE):
            result["transmission"] = "Manual"
        return result

    def _extract_year(self, title: str) -> str:
        match = re.search(r"\b(19[9]\d|20[0-3]\d)\b", title)
        return match.group(0) if match else ""

    def _brand_from_title(self, title: str) -> str:
        return title.split()[0] if title else ""

    def _normalize_transmission(self, value: str) -> str:
        v = (value or "").lower().strip()
        if "auto" in v: return "Automatico"
        if "manual" in v: return "Manual"
        if "cvt" in v: return "CVT"
        return value.strip() or None

    def _normalize_condition(self, value: str) -> str:
        v = (value or "").lower().strip()
        if "nuev" in v: return "Nuevo"
        if "usad" in v or "semi" in v: return "Usado"
        return value or None
