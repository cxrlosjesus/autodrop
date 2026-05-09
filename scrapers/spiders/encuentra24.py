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

source_url:
    Se guarda response.url (URL real del listing, browseable).
    Para deduplicación en memoria se usa el ID numérico final de la URL.
    Esto evita que la UI muestre enlaces rotos cuando el slug cambia.
"""
import re
import json
import os

import scrapy

from .base import AutoPulseSpider


class Encuentra24Spider(AutoPulseSpider):
    name = "encuentra24"
    version = "3.1.0"
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
        "CLOSESPIDER_TIMEOUT": 1500,   # 25 min: si sigue sin items, algo está mal
        "CLOSESPIDER_ERRORCOUNT": 50,  # Abort si acumula 50 errores HTTP
        "HTTPPROXY_ENABLED": True,
        "RETRY_TIMES": 3,
        "RETRY_HTTP_CODES": [500, 502, 503, 504, 408, 429, 403],
        "RETRY_BACKOFF_BASE": 2.0,
        "RETRY_BACKOFF_MAX": 30.0,
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._proxy_url  = os.getenv("RESIDENTIAL_PROXY_URL", "")
        self._known_ids  = self._load_known_ids()
        self.logger.info(
            f"Proxy: {'✓ configurado' if self._proxy_url else '✗ sin proxy'} | "
            f"IDs conocidos en DB: {len(self._known_ids)}"
        )

    def _load_known_ids(self) -> set:
        """
        Carga los IDs numéricos de listings ya scrapeados para scraping incremental.
        Usa el ID (último segmento numérico de la URL) como clave estable,
        independientemente del slug que tenga la URL en la DB.
        """
        try:
            from sqlalchemy import create_engine, text
            engine = create_engine(os.getenv(
                "DATABASE_URL",
                "postgresql://autopulse:autopulse_secret@localhost:5432/autopulse"
            ))
            with engine.connect() as conn:
                result = conn.execute(text(
                    "SELECT source_url FROM bronze.raw_listings"
                    " WHERE source_site = 'encuentra24'"
                    " AND scraped_at > NOW() - INTERVAL '2 days'"
                ))
                ids = set()
                for (url,) in result:
                    listing_id = self._extract_id(url)
                    if listing_id:
                        ids.add(listing_id)
                return ids
        except Exception as e:
            self.logger.warning(f"No se pudo cargar IDs conocidos (primer run?): {e}")
            return set()

    @staticmethod
    def _extract_id(url: str) -> str | None:
        """Extrae el ID numérico del final de cualquier URL de Encuentra24."""
        m = re.search(r'/(\d+)(?:[/?#].*)?$', url)
        return m.group(1) if m else None

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
        page          = response.meta["page"]

        if response.status != 200:
            self.logger.warning(
                f"[{category_path.split('/')[-1]}] p.{page}: "
                f"status {response.status} — omitiendo"
            )
            return

        # Extraer paths únicos de listings del HTML
        raw_paths = set(re.findall(
            r'/panama-es/autos(?:-usados|-nuevos)/[^"\'<\s]+/\d+',
            response.text
        ))

        new_count = 0
        for path in raw_paths:
            listing_id = self._extract_id(path)
            if listing_id and listing_id not in self._known_ids:
                new_count += 1
                yield self._req(
                    self.BASE_URL + path,
                    callback=self.parse_detail,
                    meta={"listing_path": path},
                )

        self.logger.info(
            f"[{category_path.split('/')[-1]}] p.{page}: "
            f"{len(raw_paths)} listings | {new_count} nuevos"
        )

        # En página 1 detectamos totalPages y encolamos las demás
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

    def parse_detail(self, response):
        source_url = response.url
        # Encuentra24 redirige slug URLs → /item/{id} server-side, pero ese formato
        # no funciona en el browser (redirige al homepage). Usamos el path original
        # extraído del HTML (listing_path), que sí es browseable.
        if '/item/' in source_url:
            original_path = response.meta.get("listing_path")
            if original_path:
                source_url = self.BASE_URL + original_path
            else:
                listing_id = self._extract_id(source_url)
                if listing_id:
                    source_url = f"{self.BASE_URL}/panama-es/autos-usados/{listing_id}"

        if response.status != 200:
            self.logger.warning(f"status {response.status} en {source_url} — omitiendo")
            return

        try:
            json_ld = self._extract_json_ld(response)

            title = response.css("h1::text").get("").strip() or json_ld.get("name", "")

            price_offers = json_ld.get("offers", {})
            price_raw = (
                str(price_offers.get("price", "")) if price_offers.get("price")
                else (response.css("main [class*='price']::text").get("") or "").strip()
            )
            seller_info = price_offers.get("seller", {})
            seller_name = seller_info.get("name", "").strip() if isinstance(seller_info, dict) else ""

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
                source_url    = source_url,
                title         = title,
                price_raw     = price_raw,
                brand_raw     = brand_raw,
                model_raw     = model_raw,
                year_raw      = year_raw,
                mileage_raw   = self._extract_mileage(response, json_ld),
                seller_name   = seller_name or None,
                transmission  = transmission,
                body_type     = json_ld.get("bodyType", ""),
                fuel_type     = fuel_type,
                color         = "",
                condition     = "Usado" if "autos-usados" in response.url else "Nuevo",
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
            self.logger.error(f"Error parseando {source_url}: {e}")

    # ── Helpers ──────────────────────────────────────────────────────

    def _extract_year(self, title: str) -> str:
        m = re.search(r'\b(19|20)\d{2}\b', title)
        return m.group(0) if m else ""

    def _extract_model(self, title: str, brand: str) -> str:
        model = title
        if brand:
            model = re.sub(re.escape(brand), "", model, flags=re.IGNORECASE)
        return re.sub(r'\b(19|20)\d{2}\b', "", model).strip()

    def _extract_mileage(self, response, json_ld: dict) -> str:
        # 1. JSON-LD cuando está disponible
        mileage_ld = json_ld.get("mileageFromOdometer")
        if mileage_ld:
            if isinstance(mileage_ld, dict):
                return f"{mileage_ld.get('value', '')} km"
            return f"{mileage_ld} km"

        # 2. og:title — Encuentra24 siempre incluye el km del anuncio principal aquí,
        #    a diferencia de main::text que también contiene listings relacionados.
        #    Formato: "Marca Modelo Año [N]km Combustible Transmisión en Ciudad | ..."
        og_title = response.css("meta[property='og:title']::attr(content)").get("")
        m = re.search(r"(\d[\d,.]*)\s*km", og_title, re.IGNORECASE)
        if m:
            return f"{m.group(1)} km"

        # 3. Selectores específicos del anuncio (sin riesgo de capturar relacionados)
        for selector in [
            "[data-testid*='mileage']::text",
            "[class*='mileage']::text",
            "[class*='odometer']::text",
        ]:
            for text in response.css(selector).getall():
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
        # og:title tiene el patrón "... en Ciudad | ..." — fuente más confiable
        og_title = response.css("meta[property='og:title']::attr(content)").get("")
        m = re.search(r" en ([^|]+)", og_title)
        if m:
            return m.group(1).strip()

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
