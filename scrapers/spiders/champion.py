"""
AutoPulse Panamá — Spider: Champion Motors
URL: championmotors.com.pa/cars

El listing carga los cards via JS (necesita Playwright).
Los detalles son server-rendered (HTTP puro, más rápido).
Paginación via ?page=N. Specs en ul.details-list.
"""
import re

from playwright.async_api import Page
from scrapy_playwright.page import PageMethod

from .base import AutoPulseSpider


class ChampionSpider(AutoPulseSpider):
    name = "champion"
    version = "1.0.0"
    uses_playwright = True

    BASE_URL  = "https://championmotors.com.pa"
    START_URLS = [
        "https://championmotors.com.pa/cars",
        "https://championmotors.com.pa/premiums",
    ]

    custom_settings = {
        "DOWNLOAD_DELAY": 1.5,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 1,
        "PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT": 30000,
        "CLOSESPIDER_TIMEOUT": 1800,   # Máximo 30 minutos
        "HTTPCACHE_ENABLED": False,
        "CLOSESPIDER_ITEMCOUNT": 0,
        "CLOSESPIDER_PAGECOUNT": 0,
        "CLOSESPIDER_ERRORCOUNT": 0,
    }

    def start_requests(self):
        for url in self.START_URLS:
            yield self.playwright_request(
                url=url,
                callback=self.parse_listing_page,
                meta={"page_number": 1, "category_url": url},
                page_methods=[
                    PageMethod("wait_for_timeout", 4000),
                    PageMethod("evaluate", "window.scrollTo(0, document.body.scrollHeight)"),
                    PageMethod("wait_for_timeout", 2000),
                ],
            )

    async def parse_listing_page(self, response):
        page: Page = response.meta.get("playwright_page")
        page_number = response.meta.get("page_number", 1)
        category_url = response.meta.get("category_url", self.START_URLS[0])
        cat_name = category_url.rstrip("/").split("/")[-1]

        if page is None:
            self.logger.error(f"[{cat_name}] Página {page_number}: playwright_page es None")
            return

        try:
            car_urls = await page.evaluate("""
                () => {
                    const links = document.querySelectorAll('a[href*="/cars/"], a[href*="/premiums/"]');
                    const hrefs = Array.from(links).map(a => a.href);
                    return [...new Set(hrefs)].filter(h => /\\/\\d+$/.test(h));
                }
            """)

            self.logger.info(f"[{cat_name}] Página {page_number}: {len(car_urls)} autos")

            for url in car_urls:
                yield self.playwright_request(
                    url=url,
                    callback=self.parse_detail,
                    page_methods=[
                        PageMethod("wait_for_load_state", "domcontentloaded"),
                        PageMethod("wait_for_timeout", 1500),
                    ],
                )

            # Paginación
            if car_urls and page_number < 30:
                next_url = f"{category_url.rstrip('/')}?page={page_number + 1}"
                yield self.playwright_request(
                    url=next_url,
                    callback=self.parse_listing_page,
                    meta={"page_number": page_number + 1, "category_url": category_url},
                    page_methods=[
                        PageMethod("wait_for_timeout", 4000),
                        PageMethod("evaluate", "window.scrollTo(0, document.body.scrollHeight)"),
                        PageMethod("wait_for_timeout", 2000),
                    ],
                )
        finally:
            if page:
                await page.close()

    async def parse_detail(self, response):
        page: Page = response.meta.get("playwright_page")
        url = response.url

        try:
            # Extraer todos los datos via JS desde la página renderizada
            data = await page.evaluate("""
                () => {
                    // Título
                    const h1 = document.querySelector('h1');
                    const title = h1 ? h1.innerText.trim() : '';

                    // Precio: primer div.bg-blue-main en el documento
                    const priceEl = document.querySelector('div.bg-blue-main');
                    const priceText = priceEl ? priceEl.innerText.replace(/\\s+/g, ' ').trim() : '';

                    // Specs desde ul.details-list
                    const specs = {};
                    document.querySelectorAll('ul.details-list li').forEach(li => {
                        const text = li.innerText.replace(/\\s+/g, ' ').trim();
                        const idx = text.indexOf(':');
                        if (idx > 0) {
                            const key = text.slice(0, idx).trim().toLowerCase();
                            const val = text.slice(idx + 1).trim();
                            if (key && val) specs[key] = val;
                        }
                    });

                    // Descripción: párrafos largos
                    const paras = Array.from(document.querySelectorAll('p'))
                        .map(p => p.innerText.trim())
                        .filter(t => t.length > 80);

                    return { title, priceText, specs, description: paras.join(' ') };
                }
            """)

            title    = data.get("title", "")
            specs    = data.get("specs", {})
            # Precio: extraer solo dígitos y comas del texto "$  12,899"
            price_raw = re.sub(r"[^\d,.]", "", data.get("priceText", "")).strip()
            description = data.get("description", "")

            brand_raw    = specs.get("marca", "") or self._brand_from_url(url)
            model_raw    = specs.get("modelo", "")
            year_raw     = specs.get("año", "") or self._extract_year(title)
            mileage_raw  = specs.get("kilometraje", "")
            transmission = self._normalize_transmission(specs.get("transmisión", specs.get("transmision", "")))
            fuel_type    = specs.get("tipo de combustible", "")
            body_type    = specs.get("tipo", specs.get("categoría", specs.get("categoria", "")))
            color        = specs.get("color", "")
            condition    = specs.get("condición", specs.get("condicion", ""))

            item = self.create_item(
                source_url    = url,
                title         = title,
                price_raw     = price_raw,
                brand_raw     = brand_raw,
                model_raw     = model_raw,
                year_raw      = year_raw,
                mileage_raw   = mileage_raw,
                transmission  = transmission,
                body_type     = body_type,
                fuel_type     = fuel_type,
                color         = color,
                condition     = self._normalize_condition(condition),
                location_city = "Ciudad de Panamá",
                description   = description,
                extra_data    = {"specs": specs},
            )

            self.track_listing()
            yield item

        except Exception as e:
            self._errors_count += 1
            self.logger.error(f"Error parseando {url}: {e}")
        finally:
            if page:
                await page.close()

    # ── Helpers ─────────────────────────────────────────────────

    def _extract_year(self, title: str) -> str:
        match = re.search(r"\b(19|20)\d{2}\b", title)
        return match.group(0) if match else ""

    def _brand_from_url(self, url: str) -> str:
        """Extrae la marca desde la URL: /cars/{brand}/{model}/{id}"""
        parts = url.rstrip("/").split("/")
        # /cars/nissan/qashqai/1217 → index -3 = nissan
        if len(parts) >= 4:
            return parts[-3].replace("-", " ").title()
        return ""

    def _normalize_transmission(self, value: str) -> str:
        v = value.lower().strip()
        if "auto" in v:
            return "Automatico"
        if "manual" in v:
            return "Manual"
        if "cvt" in v:
            return "CVT"
        return value.strip() or None

    def _normalize_condition(self, value: str) -> str:
        v = value.lower().strip()
        if "nuev" in v:
            return "Nuevo"
        if "usad" in v or "semi" in v:
            return "Usado"
        return value or None
