"""
AutoPulse Panamá — Spider: Encuentra24
URL: encuentra24.com/panama-es/autos

Selectores actualizados según inspección de Abril 2026.
El sitio usa Next.js/React con clases Tailwind prefijadas (card_*).
Los cards están envueltos directamente en <a> tags.

Estrategia:
    1. Abrir página de listado con domcontentloaded (networkidle nunca termina en SPA)
    2. Extraer URLs via JS (los links están en <a href*="/panama-es/autos-">)
    3. Paginar incrementando el parámetro ?page=N
    4. En cada detalle extraer campos desde JSON-LD + CSS
"""
import re
import json
from urllib.parse import urlparse, urlunparse

from playwright.async_api import Page
from scrapy_playwright.page import PageMethod

from .base import AutoPulseSpider


class Encuentra24Spider(AutoPulseSpider):
    name = "encuentra24"
    version = "2.0.0"
    uses_playwright = True

    BASE_URL  = "https://www.encuentra24.com"
    START_URLS = [
        "https://www.encuentra24.com/panama-es/autos-usados",
        "https://www.encuentra24.com/panama-es/autos-nuevos",
    ]

    # Selector real del sitio (clases Tailwind con prefijo card_)
    LISTING_CARD_SELECTOR = "[class*='card_price']"

    custom_settings = {
        "DOWNLOAD_DELAY": 2.0,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 2,
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
                    PageMethod("wait_for_timeout", 6000),
                    PageMethod("evaluate", "window.scrollTo(0, document.body.scrollHeight)"),
                    PageMethod("wait_for_timeout", 2000),
                ]
            )

    async def parse_listing_page(self, response):
        page: Page = response.meta.get("playwright_page")
        page_number = response.meta.get("page_number", 1)
        category_url = response.meta.get("category_url", self.START_URLS[0])

        if page is None:
            self.logger.error(f"Página {page_number}: playwright_page es None, saltando")
            return

        try:
            # Extraer URLs via JS — solo autos (excluye motos y accesorios)
            listing_urls = await page.evaluate("""
                () => {
                    const links = document.querySelectorAll(
                        'a[href*="/panama-es/autos-usados/"], a[href*="/panama-es/autos-nuevos/"]'
                    );
                    const hrefs = Array.from(links).map(a => a.href);
                    return [...new Set(hrefs)].filter(h => /\\/\\d+$/.test(h));
                }
            """)

            self.logger.info(f"[{category_url.split('/')[-1]}] Página {page_number}: {len(listing_urls)} anuncios")

            for url in listing_urls:
                yield self.playwright_request(
                    url=url,
                    callback=self.parse_listing_detail,
                    page_methods=[
                        PageMethod("wait_for_load_state", "domcontentloaded"),
                        PageMethod("wait_for_timeout", 2000),
                    ]
                )

            # Paginación: ?page=N — paginamos desde la URL de categoría
            if listing_urls and page_number < 50:
                next_url = self._build_page_url(category_url, page_number + 1)
                yield self.playwright_request(
                    url=next_url,
                    callback=self.parse_listing_page,
                    meta={"page_number": page_number + 1, "category_url": category_url},
                    page_methods=[
                        PageMethod("wait_for_timeout", 6000),
                        PageMethod("evaluate", "window.scrollTo(0, document.body.scrollHeight)"),
                        PageMethod("wait_for_timeout", 2000),
                    ]
                )
        finally:
            if page:
                await page.close()

    async def parse_listing_detail(self, response):
        page: Page = response.meta.get("playwright_page")

        try:
            url = response.url

            # JSON-LD — fuente principal de datos estructurados
            json_ld = self._extract_json_ld(response)

            # Título (h1)
            title = response.css("h1::text").get("").strip() or json_ld.get("name", "")

            # Precio desde JSON-LD (ya es numérico) o desde CSS
            price_offers = json_ld.get("offers", {})
            price_raw = str(price_offers.get("price", "")) if price_offers.get("price") else (
                response.css("main [class*='price']::text").get("") or ""
            ).strip()

            # Marca desde JSON-LD — puede ser string u objeto {"@type":"Organization","name":"Toyota"}
            manufacturer = json_ld.get("manufacturer", "")
            if isinstance(manufacturer, dict):
                brand_raw = manufacturer.get("name", "")
            else:
                brand_raw = str(manufacturer)
            model_raw = self._extract_model(title, brand_raw)

            # Año desde el título (ej: "BYD SEAL 2024" → "2024")
            year_raw = self._extract_year(title)

            # Transmisión y combustible desde JSON-LD
            transmission = self._normalize_transmission(
                json_ld.get("vehicleTransmission", "")
            )
            engines = json_ld.get("vehicleEngine", [])
            if isinstance(engines, dict):
                engines = [engines]
            fuel_type = next(
                (e.get("fuelType", "") for e in engines if e.get("fuelType")), ""
            )

            # Carrocería y capacidad desde JSON-LD
            body_type = json_ld.get("bodyType", "")

            # Kilometraje — buscar en la página (no está en JSON-LD)
            mileage_raw = self._extract_mileage(response)

            item = self.create_item(
                source_url   = url,
                title        = title,
                price_raw    = price_raw,
                brand_raw    = brand_raw,
                model_raw    = model_raw,
                year_raw     = year_raw,
                mileage_raw  = mileage_raw,
                transmission = transmission,
                body_type    = body_type,
                fuel_type    = fuel_type,
                color        = "",
                condition    = "Usado" if "autos-usados" in url else "Nuevo",
                location_city= self._extract_location(response),
                description  = " ".join(response.css(
                                  "[class*='description']::text"
                               ).getall()).strip(),
                extra_data   = {"json_ld": json_ld},
            )

            self.track_listing()
            yield item

        except Exception as e:
            self._errors_count += 1
            self.logger.error(f"Error parseando {response.url}: {e}")
        finally:
            if page:
                await page.close()

    # ── Helpers ──────────────────────────────────────────────

    def _extract_year(self, title: str) -> str:
        match = re.search(r'\b(19|20)\d{2}\b', title)
        return match.group(0) if match else ""

    def _extract_model(self, title: str, brand: str) -> str:
        model = title
        if brand:
            model = re.sub(re.escape(brand), "", model, flags=re.IGNORECASE)
        model = re.sub(r'\b(19|20)\d{2}\b', "", model)
        return model.strip()

    def _extract_mileage(self, response) -> str:
        texts = response.css("main ::text").getall()
        for text in texts:
            text = text.strip()
            if re.match(r"^\d[\d,.]+ *(km|Km|KM)$", text):
                return text
        return ""

    def _extract_json_ld(self, response) -> dict:
        for script in response.css("script[type='application/ld+json']::text").getall():
            try:
                data = json.loads(script)
                if isinstance(data, list):
                    data = data[0]
                if data.get("@type") in ("Car", "Vehicle", "Product", "Offer"):
                    return data
            except (json.JSONDecodeError, KeyError):
                continue
        return {}

    def _extract_location(self, response) -> str:
        location = response.css(
            "[class*='location']::text, [class*='subtitle']::text"
        ).get("").strip()
        location = re.sub(r"[📍🏢]", "", location).strip()
        return location.split(",")[0].strip() if "," in location else location

    def _normalize_transmission(self, value: str) -> str:
        v = value.lower().strip()
        if any(x in v for x in ["auto", "automát"]):
            return "Automatico"
        if "manual" in v:
            return "Manual"
        if "cvt" in v:
            return "CVT"
        return value.strip() or None

    def _build_page_url(self, base_url: str, page_number: int) -> str:
        if page_number == 1:
            return base_url
        # Strip any prior page suffix (.N) before adding the new one
        clean = re.sub(r'\.\d+$', '', base_url.split('?')[0])
        return f"{clean}?page={page_number}"
