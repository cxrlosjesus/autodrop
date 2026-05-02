"""
AutoPulse Panamá — Spider: Encuentra24 v3 (HTTP puro, sin Playwright)
URL: encuentra24.com/panama-es/autos-usados  |  .../autos-nuevos

El sitio usa Next.js App Router con SSR completo — los listings están en el
HTML estático. No necesita JavaScript ni Playwright.

Estrategia:
    1. GET listing page → extraer URLs via regex
    2. Extraer totalPages del JSON embebido → paginar con formato /categoria.N
    3. Para cada URL nueva (no en DB): GET detail page
    4. Extraer datos desde JSON-LD + CSS selectors
    5. Enrutar a través de proxy residencial si RESIDENTIAL_PROXY_URL está definida
"""
import re
import json
import os

import scrapy

from .base import AutoPulseSpider


class Encuentra24Spider(AutoPulseSpider):
    name = "encuentra24"
    version = "3.0.0"
    uses_playwright = False

    BASE_URL = "https://www.encuentra24.com"
    CATEGORIES = [
        "/panama-es/autos-usados",
        "/panama-es/autos-nuevos",
    ]

    custom_settings = {
        "DOWNLOAD_DELAY": 1.5,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 3,
        "HTTPCACHE_ENABLED": False,
        "CLOSESPIDER_TIMEOUT": 7200,
        "HTTPPROXY_ENABLED": True,
        "RETRY_TIMES": 10,
        "RETRY_HTTP_CODES": [500, 502, 503, 504, 408, 429, 403],
        "RETRY_BACKOFF_BASE": 2.0,
        "RETRY_BACKOFF_MAX": 30.0,
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._proxy_url = os.getenv("RESIDENTIAL_PROXY_URL", "")
        self._known_urls = self._load_known_urls()
        self.logger.info(
            f"Proxy: {'✓ configurado' if self._proxy_url else '✗ sin proxy'} | "
            f"URLs conocidas en DB: {len(self._known_urls)}"
        )

    def _load_known_urls(self) -> set:
        """Carga URLs ya scrapeadas desde la DB para hacer scraping incremental."""
        try:
            from sqlalchemy import create_engine, text
            engine = create_engine(os.getenv(
                "DATABASE_URL",
                "postgresql://autopulse:autopulse_secret@localhost:5432/autopulse"
            ))
            with engine.connect() as conn:
                result = conn.execute(text(
                    "SELECT source_url FROM bronze.raw_listings WHERE source_site = 'encuentra24'"
                ))
                return {row[0] for row in result}
        except Exception as e:
            self.logger.warning(f"No se pudo cargar URLs conocidas (primer run?): {e}")
            return set()

    def _req(self, url: str, callback, **kwargs) -> scrapy.Request:
        """Request con proxy y handle_httpstatus aplicados automáticamente."""
        meta = kwargs.pop("meta", {})
        if self._proxy_url:
            meta["proxy"] = self._proxy_url
        meta.setdefault("handle_httpstatus_list", [403, 429])
        return self.standard_request(url, callback, meta=meta, **kwargs)

    # ──────────────────────────────────────────
    # Flows
    # ──────────────────────────────────────────

    def start_requests(self):
        for path in self.CATEGORIES:
            yield self._req(
                self.BASE_URL + path,
                callback=self.parse_listing_page,
                meta={"category_path": path, "page": 1},
            )

    def parse_listing_page(self, response):
        category_path = response.meta["category_path"]
        page = response.meta["page"]

        if response.status != 200:
            self.logger.warning(f"[{category_path.split('/')[-1]}] p.{page}: status {response.status} — omitiendo")
            return

        # Extraer paths únicos de listings del HTML
        raw_paths = set(re.findall(
            r'/panama-es/autos(?:-usados|-nuevos)/[^"\'<\s]+/\d+',
            response.text
        ))

        new_count = 0
        for path in raw_paths:
            full_url = self.BASE_URL + path
            canonical = self._canonical_url(full_url)
            if canonical not in self._known_urls:
                new_count += 1
                yield self._req(full_url, callback=self.parse_detail)

        self.logger.info(
            f"[{category_path.split('/')[-1]}] p.{page}: "
            f"{len(raw_paths)} listings encontrados | {new_count} nuevos"
        )

        # En página 1 detectamos totalPages y encolamos todas las demás
        if page == 1:
            match = re.search(r'totalPages[^\d]+(\d+)', response.text)
            if match:
                total_pages = int(match.group(1))
                self.logger.info(f"[{category_path.split('/')[-1]}] Total páginas: {total_pages}")
                base = self.BASE_URL + category_path.rstrip("/")
                for p in range(2, total_pages + 1):
                    yield self._req(
                        f"{base}.{p}",
                        callback=self.parse_listing_page,
                        meta={"category_path": category_path, "page": p},
                    )

    @staticmethod
    def _canonical_url(url: str) -> str:
        """Normaliza URL de Encuentra24 al ID numérico canónico.
        /panama-es/autos-usados/hyundai-accent-2019-5-900/32253702
        /panama-es/autos-usados/hyundai-accent-2019-6-900/32253702  <- mismo anuncio
        → https://www.encuentra24.com/item/32253702
        """
        m = re.search(r'/(\d+)$', url)
        return f"https://www.encuentra24.com/item/{m.group(1)}" if m else url

    def parse_detail(self, response):
        url = self._canonical_url(response.url)

        if response.status != 200:
            self.logger.warning(f"status {response.status} en {response.url} — omitiendo")
            return

        try:
            json_ld = self._extract_json_ld(response)

            title = response.css("h1::text").get("").strip() or json_ld.get("name", "")

            price_offers = json_ld.get("offers", {})
            price_raw = (
                str(price_offers.get("price", "")) if price_offers.get("price")
                else (response.css("main [class*='price']::text").get("") or "").strip()
            )

            manufacturer = json_ld.get("manufacturer", "")
            brand_raw = (
                manufacturer.get("name", "") if isinstance(manufacturer, dict)
                else str(manufacturer)
            )
            model_raw = self._extract_model(title, brand_raw)
            year_raw  = self._extract_year(title)

            transmission = self._normalize_transmission(json_ld.get("vehicleTransmission", ""))
            engines = json_ld.get("vehicleEngine", [])
            if isinstance(engines, dict):
                engines = [engines]
            fuel_type = next((e.get("fuelType", "") for e in engines if e.get("fuelType")), "")

            item = self.create_item(
                source_url    = url,
                title         = title,
                price_raw     = price_raw,
                brand_raw     = brand_raw,
                model_raw     = model_raw,
                year_raw      = year_raw,
                mileage_raw   = self._extract_mileage(response),
                transmission  = transmission,
                body_type     = json_ld.get("bodyType", ""),
                fuel_type     = fuel_type,
                color         = "",
                condition     = "Usado" if "autos-usados" in url else "Nuevo",
                location_city = self._extract_location(response),
                description   = " ".join(
                    response.css("[class*='description']::text").getall()
                ).strip(),
                extra_data    = {"json_ld": json_ld},
            )

            self.track_listing()
            yield item

        except Exception as e:
            self._errors_count += 1
            self.logger.error(f"Error parseando {url}: {e}")

    # ── Helpers ──────────────────────────────────────────────────────

    def _extract_year(self, title: str) -> str:
        m = re.search(r'\b(19|20)\d{2}\b', title)
        return m.group(0) if m else ""

    def _extract_model(self, title: str, brand: str) -> str:
        model = title
        if brand:
            model = re.sub(re.escape(brand), "", model, flags=re.IGNORECASE)
        return re.sub(r'\b(19|20)\d{2}\b', "", model).strip()

    def _extract_mileage(self, response) -> str:
        for text in response.css("main ::text").getall():
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
        v = (value or "").lower().strip()
        if any(x in v for x in ["auto", "automát"]): return "Automatico"
        if "manual" in v: return "Manual"
        if "cvt" in v: return "CVT"
        return value.strip() or None
