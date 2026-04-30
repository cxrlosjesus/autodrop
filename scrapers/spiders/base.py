"""
AutoPulse Panamá — Spider Base

Todos los spiders de AutoPulse heredan de esta clase.
Provee: logging estructurado, manejo de errores, run_id,
        métricas y comportamiento común.
"""
import scrapy
import uuid
from datetime import datetime, timezone
from loguru import logger

from items import CarListingItem


class AutoPulseSpider(scrapy.Spider):
    """
    Clase base para todos los spiders de AutoPulse.
    
    Uso:
        class Encuentra24Spider(AutoPulseSpider):
            name = "encuentra24"
            version = "1.0.0"
            uses_playwright = True
    """
    
    # Subclases deben definir esto
    name = None
    version = "1.0.0"
    uses_playwright = False         # True si el sitio necesita JS rendering
    
    # Métricas del run
    _listings_found = 0
    _errors_count = 0
    _run_id = None
    _started_at = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._run_id = str(uuid.uuid4())
        self._started_at = datetime.now(timezone.utc)
        
        logger.info(
            f"🚀 Spider iniciado: {self.name} v{self.version} | "
            f"run_id={self._run_id}"
        )

    def create_item(self, **kwargs) -> CarListingItem:
        """
        Factory method para crear items con campos comunes pre-llenados.
        Uso: item = self.create_item(brand_raw='Toyota', price_raw='$15,500', ...)
        """
        item = CarListingItem()
        item["source_site"] = self.name
        item["scrape_run_id"] = self._run_id
        item["spider_version"] = self.version
        item["scraped_at"] = datetime.now(timezone.utc).isoformat()
        
        for key, value in kwargs.items():
            if key in item.fields:
                item[key] = value
            else:
                logger.warning(f"Campo desconocido ignorado: {key}")
        
        return item

    def playwright_request(self, url, callback, **kwargs):
        """
        Shortcut para hacer requests con Playwright.
        El timeout de navegación lo toma de custom_settings o del proyecto (no hardcodeado).
        """
        nav_timeout = (
            self.custom_settings.get("PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT")
            if hasattr(self, "custom_settings")
            else None
        ) or 45000
        return scrapy.Request(
            url=url,
            callback=callback,
            meta={
                "playwright": True,
                "playwright_include_page": True,
                "playwright_page_methods": kwargs.get("page_methods", []),
                "playwright_page_goto_kwargs": {
                    "wait_until": "domcontentloaded",
                    "timeout": nav_timeout,
                },
                **kwargs.get("meta", {})
            },
            errback=self.handle_error,
            **{k: v for k, v in kwargs.items() if k not in ("page_methods", "meta")}
        )

    def standard_request(self, url, callback, **kwargs):
        """
        Shortcut para requests HTTP estándar (sin JavaScript).
        """
        return scrapy.Request(
            url=url,
            callback=callback,
            errback=self.handle_error,
            **kwargs
        )

    def handle_error(self, failure):
        """Manejo centralizado de errores de request."""
        self._errors_count += 1
        logger.error(
            f"❌ Error en request | spider={self.name} | "
            f"url={failure.request.url} | "
            f"error={failure.getErrorMessage()}"
        )

    def track_listing(self):
        """Llamar cada vez que se extrae un listing exitosamente."""
        self._listings_found += 1
        if self._listings_found % 50 == 0:
            logger.info(
                f"📊 Progreso: {self._listings_found} listings | "
                f"errores={self._errors_count} | spider={self.name}"
            )

    def closed(self, reason):
        """Se llama automáticamente cuando el spider termina."""
        duration = (datetime.now(timezone.utc) - self._started_at).total_seconds()
        logger.info(
            f"✅ Spider terminado: {self.name} | "
            f"listings={self._listings_found} | "
            f"errores={self._errors_count} | "
            f"duración={duration:.1f}s | "
            f"razón={reason}"
        )
