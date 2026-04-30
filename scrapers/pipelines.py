"""
AutoPulse Panamá — Pipelines de Scrapy

Orden de ejecución:
    1. ValidationPipeline    — Descarta items inválidos
    2. NormalizerPipeline    — Normaliza marca, precio, km
    3. DeduplicationPipeline — Detecta duplicados
    4. DatabasePipeline      — Guarda en PostgreSQL Bronze
"""
import re
import hashlib
import json
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

from loguru import logger
from scrapy.exceptions import DropItem
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from items import CarListingItem


# ────────────────────────────────────────────────
# 1. VALIDATION PIPELINE
# ────────────────────────────────────────────────
class ValidationPipeline:
    """
    Descarta items que no tienen la información mínima necesaria.
    Un item sin URL o sin precio no tiene valor para el producto.
    """

    REQUIRED_FIELDS = ["source_url", "source_site"]

    def process_item(self, item, spider):
        if not isinstance(item, CarListingItem):
            return item

        # Verificar campos requeridos
        for field in self.REQUIRED_FIELDS:
            if not item.get(field):
                raise DropItem(f"Campo requerido faltante: {field} | url={item.get('source_url', 'N/A')}")

        # URL debe ser válida
        url = item.get("source_url", "")
        if not url.startswith("http"):
            raise DropItem(f"URL inválida: {url}")

        return item


# ────────────────────────────────────────────────
# 2. NORMALIZER PIPELINE
# ────────────────────────────────────────────────
class NormalizerPipeline:
    """
    Transforma los valores raw a tipos limpios y consistentes.
    No descarta items — si no puede parsear un campo, lo deja en None.
    """

    def __init__(self):
        # Cargar lookup de marcas (se carga desde DB en open_spider)
        self.brand_lookup = {}

    def open_spider(self, spider):
        """Se llama una vez cuando arranca el spider."""
        # Lookup estático de respaldo (la DB tiene más)
        self.brand_lookup = {
            "toyota": "Toyota", "hyundai": "Hyundai", "kia": "Kia",
            "honda": "Honda", "nissan": "Nissan", "mitsubishi": "Mitsubishi",
            "ford": "Ford", "chevrolet": "Chevrolet", "chevy": "Chevrolet",
            "mazda": "Mazda", "volkswagen": "Volkswagen", "vw": "Volkswagen",
            "bmw": "BMW", "mercedes": "Mercedes-Benz", "mercedes-benz": "Mercedes-Benz",
            "audi": "Audi", "jeep": "Jeep", "ram": "Ram", "dodge": "Dodge",
            "subaru": "Subaru", "suzuki": "Suzuki", "isuzu": "Isuzu",
            "lexus": "Lexus", "land rover": "Land Rover", "volvo": "Volvo",
            "byd": "BYD", "chery": "Chery", "jac": "JAC",
        }

    def process_item(self, item, spider):
        if not isinstance(item, CarListingItem):
            return item

        # ── Normalizar marca ──
        brand_raw = str(item.get("brand_raw", "") or "").strip()
        item["brand"] = self._normalize_brand(brand_raw)

        # ── Normalizar modelo ──
        item["model"] = self._clean_text(item.get("model_raw", ""))

        # ── Normalizar año ──
        item["year"] = self._parse_year(item.get("year_raw", ""))

        # ── Normalizar precio ──
        item["price_usd"] = self._parse_price(item.get("price_raw", ""))

        # ── Normalizar kilometraje ──
        item["mileage_km"] = self._parse_mileage(item.get("mileage_raw", ""))

        # ── Normalizar condición ──
        condition = str(item.get("condition", "") or "").lower()
        if "nuev" in condition:
            item["condition"] = "Nuevo"
        elif "usad" in condition or "semi" in condition:
            item["condition"] = "Usado"

        # ── Normalizar descripción (lista → string) ──
        description = item.get("description", "")
        if isinstance(description, list):
            item["description"] = " ".join(str(d) for d in description if d).strip()

        return item

    def _normalize_brand(self, raw: str) -> str | None:
        if not raw:
            return None
        key = raw.lower().strip()
        # Búsqueda exacta primero
        if key in self.brand_lookup:
            return self.brand_lookup[key]
        # Búsqueda parcial
        for k, v in self.brand_lookup.items():
            if k in key or key in k:
                return v
        # Si no encontró, capitalizar la primera letra de cada palabra
        return raw.strip().title()

    def _parse_price(self, raw: str) -> float | None:
        if not raw:
            return None
        # Remover todo excepto dígitos, punto y coma
        clean = re.sub(r"[^\d.,]", "", str(raw))
        if not clean:
            return None
        # Detectar formato: 15,500.00 (US) vs 15.500,00 (EU)
        if "," in clean and "." in clean:
            # Si la coma viene antes del punto → formato US
            if clean.index(",") < clean.index("."):
                clean = clean.replace(",", "")
            else:  # formato EU
                clean = clean.replace(".", "").replace(",", ".")
        elif "," in clean:
            # Solo comas → puede ser separador de miles (15,500)
            if len(clean.split(",")[-1]) == 3:
                clean = clean.replace(",", "")
            else:
                clean = clean.replace(",", ".")
        try:
            value = float(clean)
            # Sanity check para mercado panameño
            if 500 <= value <= 500_000:
                return round(value, 2)
            return None
        except ValueError:
            return None

    def _parse_mileage(self, raw: str) -> int | None:
        if not raw:
            return None
        numbers = re.findall(r"[\d,\.]+", str(raw))
        if not numbers:
            return None
        clean = numbers[0].replace(",", "").replace(".", "")
        try:
            value = int(clean)
            # Sanity check
            if 0 <= value <= 1_000_000:
                return value
            return None
        except ValueError:
            return None

    def _parse_year(self, raw: str) -> int | None:
        if not raw:
            return None
        numbers = re.findall(r"\d{4}", str(raw))
        if numbers:
            year = int(numbers[0])
            if 1990 <= year <= datetime.now().year + 1:
                return year
        # Año de 2 dígitos: '22' → 2022
        short = re.findall(r"\b\d{2}\b", str(raw))
        if short:
            year = int(short[0])
            if 0 <= year <= 30:
                return 2000 + year
        return None

    def _clean_text(self, text: str) -> str | None:
        if not text:
            return None
        return str(text).strip() or None


# ────────────────────────────────────────────────
# 3. DEDUPLICATION PIPELINE
# ────────────────────────────────────────────────
class DeduplicationPipeline:
    """
    Genera el fingerprint para deduplicación cross-site.
    No descarta en este punto — la DB maneja el upsert.
    """

    def process_item(self, item, spider):
        if not isinstance(item, CarListingItem):
            return item

        # Fingerprint basado en campos clave del vehículo
        brand    = str(item.get("brand", "") or "").lower().strip()
        model    = str(item.get("model", "") or "").lower().strip()
        year     = str(item.get("year", "") or "")
        price    = str(item.get("price_usd", "") or "")
        mileage  = str(item.get("mileage_km", "") or "")

        fingerprint_str = f"{brand}|{model}|{year}|{price}|{mileage}"
        item["extra_data"] = item.get("extra_data") or {}
        item["extra_data"]["dedup_hash"] = hashlib.sha256(
            fingerprint_str.encode()
        ).hexdigest()

        return item


# ────────────────────────────────────────────────
# 4. DATABASE PIPELINE
# ────────────────────────────────────────────────
class DatabasePipeline:
    """
    Guarda el item en bronze.raw_listings.
    Usa upsert para manejar re-scraping del mismo anuncio.
    """

    def __init__(self, database_url: str):
        self.database_url = database_url
        self.engine = None
        self.Session = None
        self._batch = []
        self._batch_size = 50   # Insertar en lotes para eficiencia

    @classmethod
    def from_crawler(cls, crawler):
        return cls(database_url=crawler.settings.get("DATABASE_URL"))

    def open_spider(self, spider):
        self.engine = create_engine(
            self.database_url,
            pool_size=5,
            pool_pre_ping=True,
        )
        self.Session = sessionmaker(bind=self.engine)
        logger.info(f"✅ DatabasePipeline: conexión establecida con PostgreSQL")

    def close_spider(self, spider):
        # Flush del batch final
        if self._batch:
            self._flush_batch()
        if self.engine:
            self.engine.dispose()

    def process_item(self, item, spider):
        if not isinstance(item, CarListingItem):
            return item

        self._batch.append(item)

        if len(self._batch) >= self._batch_size:
            self._flush_batch()

        return item

    def _flush_batch(self):
        """Inserta el lote actual en la base de datos."""
        if not self._batch:
            return

        session = self.Session()
        try:
            for item in self._batch:
                # Construir el JSON crudo
                raw_data = {
                    k: v for k, v in dict(item).items()
                    if v is not None and k not in ("scraped_at", "scrape_run_id", "spider_version")
                }

                session.execute(text("""
                    INSERT INTO bronze.raw_listings
                        (source_site, source_url, raw_data, scraped_at, scrape_run_id, spider_version)
                    VALUES
                        (:source_site, :source_url, CAST(:raw_data AS jsonb), :scraped_at, CAST(:run_id AS uuid), :version)
                    ON CONFLICT (source_url, bronze.to_utc_date(scraped_at))
                    DO UPDATE SET
                        raw_data = EXCLUDED.raw_data,
                        scraped_at = EXCLUDED.scraped_at
                """), {
                    "source_site": item.get("source_site"),
                    "source_url":  item.get("source_url"),
                    "raw_data":    json.dumps(raw_data, default=str),
                    "scraped_at":  item.get("scraped_at"),
                    "run_id":      item.get("scrape_run_id"),
                    "version":     item.get("spider_version"),
                })

            session.commit()
            logger.debug(f"💾 Lote insertado: {len(self._batch)} registros en Bronze")
            self._batch = []

        except Exception as e:
            session.rollback()
            logger.error(f"❌ Error insertando lote en DB: {e}")
            raise
        finally:
            session.close()
