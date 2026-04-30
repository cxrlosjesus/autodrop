"""
AutoPulse Panamá — Spider: AutoMarket Panamá
URL: automarketpanama.com/public/inventario

Estructura del sitio:
    - Sin paginación — todo el inventario en una sola página
    - Filtros por tipo son client-side JS (no crean páginas nuevas)
    - Cada card ya contiene: precio, marca+modelo, km, año, transmisión
    - URL de detalle: /public/detail?placa=XXXXX
    - ~80-120 autos en inventario (dealer, no marketplace)

Estrategia:
    1. Cargar /public/inventario y extraer todos los cards con su data básica
    2. Visitar cada página de detalle para obtener: color, descripción, specs adicionales
    3. Combinar datos del card + detalle en el item final
"""
import re

from playwright.async_api import Page
from scrapy_playwright.page import PageMethod

from .base import AutoPulseSpider


class AutomarketSpider(AutoPulseSpider):
    name = "automarket"
    version = "1.0.0"
    uses_playwright = True

    LISTING_URL = "https://automarketpanama.com/public/inventario"

    custom_settings = {
        "DOWNLOAD_DELAY": 2.0,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 2,
        "PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT": 60000,
        "HTTPCACHE_ENABLED": False,
    }

    def start_requests(self):
        yield self.playwright_request(
            url=self.LISTING_URL,
            callback=self.parse_listing,
            page_methods=[
                PageMethod("wait_for_timeout", 6000),
                PageMethod("evaluate", "window.scrollTo(0, document.body.scrollHeight)"),
                PageMethod("wait_for_timeout", 2000),
            ]
        )

    async def parse_listing(self, response):
        page: Page = response.meta.get("playwright_page")

        if page is None:
            self.logger.error("playwright_page es None en parse_listing")
            return

        try:
            cards = await page.evaluate("""
                () => {
                    const results = [];

                    // Apuntar directamente al link con texto completo dentro de b-goods-f__main
                    // Este es el unico link por card que contiene precio, marca, km y anio
                    document.querySelectorAll('.b-goods-f__main a').forEach(a => {
                        const href = a.href || '';
                        if (!href.includes('placa=')) return;

                        const placa = new URL(href).searchParams.get('placa');
                        if (!placa) return;

                        const text = a.innerText.replace(/\\s+/g, ' ').trim();
                        if (!text) return;

                        const container = a.closest('.b-goods-f');
                        const img = container ? container.querySelector('img') : null;

                        results.push({
                            url: href,
                            placa: placa,
                            card_text: text,
                            img_src: img ? img.src : ''
                        });
                    });

                    return results;
                }
            """)
            self.logger.info(f"[automarket] {len(cards)} vehículos encontrados en listado")

            # Filtrar por si quedó algún card sin texto
            cards_with_data = [c for c in cards if c.get("card_text") and len(c["card_text"]) > 10]

            for card in cards_with_data:
                yield self.playwright_request(
                    url=card["url"],
                    callback=self.parse_detail,
                    meta={"card_data": card},
                    page_methods=[
                        PageMethod("wait_for_load_state", "domcontentloaded"),
                        PageMethod("wait_for_timeout", 3000),
                    ]
                )

        finally:
            if page:
                await page.close()

    async def parse_detail(self, response):
        page: Page = response.meta.get("playwright_page")
        card_data = response.meta.get("card_data", {})
        url = response.url

        try:
            # Datos adicionales desde la página de detalle
            detail = await page.evaluate("""
                () => {
                    // Título principal
                    const h1 = document.querySelector('h1, h2, [class*="title"]');
                    const title = h1 ? h1.innerText.trim() : '';

                    // Precio en detalle (clase b-goods-f__price o similar)
                    let price = '';
                    for (const sel of ['[class*="price"]', '[class*="precio"]', 'b', 'strong']) {
                        const el = document.querySelector(sel);
                        if (el) {
                            const txt = el.innerText.trim();
                            if (/[\\d,\\.]{4,}/.test(txt)) { price = txt; break; }
                        }
                    }

                    // Specs: buscar listas con patrón "Label: Valor"
                    const specs = {};
                    document.querySelectorAll('li, td, [class*="spec"] div').forEach(el => {
                        const txt = el.innerText.replace(/\\s+/g, ' ').trim();
                        const idx = txt.indexOf(':');
                        if (idx > 0 && idx < 40) {
                            const k = txt.slice(0, idx).trim().toLowerCase();
                            const v = txt.slice(idx + 1).trim();
                            if (k && v && v.length < 100) specs[k] = v;
                        }
                    });

                    // Descripción: párrafos largos
                    const desc = Array.from(document.querySelectorAll('p'))
                        .map(p => p.innerText.trim())
                        .filter(t => t.length > 50)
                        .join(' ');

                    return { title, price, specs, description: desc };
                }
            """)

            # Parsear datos del card (fuente más confiable para precio/marca/modelo)
            parsed = self._parse_card_text(card_data.get("card_text", ""))

            title     = detail.get("title") or parsed.get("title", "")
            price_raw = parsed.get("price") or detail.get("price", "")
            specs     = detail.get("specs", {})

            # Color y condición desde specs del detalle
            color     = specs.get("color", "")
            condition = self._normalize_condition(
                specs.get("condición", specs.get("condicion", specs.get("condition", "")))
            )

            item = self.create_item(
                source_url    = url,
                title         = title,
                price_raw     = price_raw,
                brand_raw     = parsed.get("brand", ""),
                model_raw     = parsed.get("model", ""),
                year_raw      = parsed.get("year", ""),
                mileage_raw   = parsed.get("mileage", ""),
                transmission  = parsed.get("transmission", ""),
                body_type     = specs.get("tipo", specs.get("categoría", specs.get("categoria", ""))),
                fuel_type     = specs.get("combustible", specs.get("tipo de combustible", "")),
                color         = color,
                condition     = condition or "Usado",
                location_city = "Ciudad de Panamá",
                description   = detail.get("description", ""),
                extra_data    = {
                    "placa": card_data.get("placa"),
                    "specs": specs,
                    "img_src": card_data.get("img_src", ""),
                },
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
        Parsea el texto del card del listado.
        Formato esperado: "29,998.00 Precio sin impuesto AUDI E-TRON 15876km 2021 AUTOMATICO"
        """
        if not text:
            return {}

        result = {}

        # Precio: primer número con coma/punto que tenga 4+ dígitos
        price_match = re.search(r"[\d]{1,3}(?:[,.][\d]{3})+(?:\.\d{2})?|\d{4,}", text)
        if price_match:
            result["price"] = price_match.group(0)

        # Año: 4 dígitos entre 1990 y 2030
        year_match = re.search(r"\b(19[9]\d|20[0-3]\d)\b", text)
        if year_match:
            result["year"] = year_match.group(0)

        # Kilometraje: número seguido de "km"
        km_match = re.search(r"([\d,\.]+)\s*km", text, re.IGNORECASE)
        if km_match:
            result["mileage"] = km_match.group(0)

        # Transmisión
        if re.search(r"AUTOMATICO|AUTOMÁTICO|AUTOMATIC", text, re.IGNORECASE):
            result["transmission"] = "Automatico"
        elif re.search(r"MANUAL", text, re.IGNORECASE):
            result["transmission"] = "Manual"
        elif re.search(r"CVT", text, re.IGNORECASE):
            result["transmission"] = "CVT"

        # Título/Marca+Modelo: línea que no contiene precio ni km ni año
        # El texto del card con datos viene de b-goods-f__main que tiene el formato:
        # "29,998.00 Precio sin impuesto AUDI E-TRON 15876km 2021 AUTOMATICO"
        # Buscamos la parte de marca+modelo limpiando lo que ya extrajimos
        clean = text
        if result.get("price"):
            clean = clean.replace(result["price"], "")
        clean = re.sub(r"Precio sin impuesto", "", clean, flags=re.IGNORECASE)
        clean = re.sub(r"[\d,\.]+\s*km", "", clean, flags=re.IGNORECASE)
        clean = re.sub(r"\b(19[9]\d|20[0-3]\d)\b", "", clean)
        clean = re.sub(r"\b(AUTOMATICO|AUTOMÁTICO|AUTOMATIC|MANUAL|CVT)\b", "", clean, flags=re.IGNORECASE)
        clean = re.sub(r"\s+", " ", clean).strip()

        if clean:
            result["title"] = clean
            # Primer palabra como marca, resto como modelo
            parts = clean.split()
            if parts:
                result["brand"] = parts[0]
                result["model"] = " ".join(parts[1:]) if len(parts) > 1 else parts[0]

        return result

    def _normalize_condition(self, value: str) -> str:
        if not value:
            return ""
        v = value.lower()
        if "nuev" in v:
            return "Nuevo"
        if "usad" in v or "semi" in v:
            return "Usado"
        return value
