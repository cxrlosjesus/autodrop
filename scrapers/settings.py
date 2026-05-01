"""
AutoPulse Panamá — Configuración Central de Scrapy
"""
import os
from dotenv import load_dotenv

load_dotenv()

BOT_NAME = "autopulse"
SPIDER_MODULES = ["spiders"]
NEWSPIDER_MODULE = "spiders"

# ──────────────────────────────────────────
# Comportamiento de scraping — respetuoso y sostenible
# ──────────────────────────────────────────

# Delay entre requests (segundos). Scrapy agrega variación aleatoria automáticamente.
DOWNLOAD_DELAY = 2.0
RANDOMIZE_DOWNLOAD_DELAY = True      # Varía entre 0.5x y 1.5x del DOWNLOAD_DELAY

# Concurrencia global
CONCURRENT_REQUESTS = 8              # Máximo requests paralelos
CONCURRENT_REQUESTS_PER_DOMAIN = 2  # Máximo por dominio (no martillar un sitio)

# Timeouts
DOWNLOAD_TIMEOUT = 30

# Reintentos
RETRY_ENABLED = True
RETRY_TIMES = 3
RETRY_HTTP_CODES = [500, 502, 503, 504, 408, 429]

# ──────────────────────────────────────────
# Playwright — Para sitios con JavaScript
# ──────────────────────────────────────────
DOWNLOAD_HANDLERS = {
    "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
    "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
}
TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"

PLAYWRIGHT_BROWSER_TYPE = "chromium"
PLAYWRIGHT_LAUNCH_OPTIONS = {
    "headless": True,
    "args": [
        "--no-sandbox",
        "--disable-setuid-sandbox",
        "--disable-dev-shm-usage",
        "--disable-blink-features=AutomationControlled",
        "--disable-infobars",
        "--window-size=1920,1080",
    ]
}
PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT = 30000   # 30 segundos
PLAYWRIGHT_MAX_PAGES_PER_CONTEXT = 4

# Contexto del browser — imitar un usuario real en Panamá
_BROWSER_CONTEXT_BASE = {
    "user_agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "viewport": {"width": 1920, "height": 1080},
    "locale": "es-PA",
    "timezone_id": "America/Panama",
    "java_script_enabled": True,
    "ignore_https_errors": True,
    "extra_http_headers": {
        "Accept-Language": "es-PA,es;q=0.9,en-US;q=0.8",
        "sec-ch-ua": '"Chromium";v="124", "Google Chrome";v="124"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
    },
}

# Proxy residencial para sitios con Cloudflare (carspot)
# Formato: http://usuario:password@geo.iproyal.com:12321
_PROXY_URL = os.getenv("RESIDENTIAL_PROXY_URL", "")
_proxy_playwright = {}
if _PROXY_URL:
    from urllib.parse import urlparse as _urlparse
    _p = _urlparse(_PROXY_URL)
    _proxy_playwright = {
        "proxy": {
            "server": f"{_p.scheme}://{_p.hostname}:{_p.port}",
            "username": _p.username or "",
            "password": _p.password or "",
        }
    }

PLAYWRIGHT_CONTEXTS = {
    # automarket y champion — sin proxy (no están bloqueados)
    "default": _BROWSER_CONTEXT_BASE,
    # carspot — con proxy residencial para pasar Cloudflare
    "cloudflare_bypass": {**_BROWSER_CONTEXT_BASE, **_proxy_playwright},
}

# Bloquear recursos que no aportan datos: imágenes, videos, fuentes, CSS
# Reduce el tiempo de carga de cada página en ~80% y evita que el spider se congele
def _abort_unnecessary_resources(request):
    return request.resource_type in ("image", "media", "font", "ping")

PLAYWRIGHT_ABORT_REQUEST = _abort_unnecessary_resources

# ──────────────────────────────────────────
# Middlewares
# ──────────────────────────────────────────
DOWNLOADER_MIDDLEWARES = {
    "middlewares.rotating_useragent.RotatingUserAgentMiddleware": 400,
    "scrapy.downloadermiddlewares.retry.RetryMiddleware": 550,
    "scrapy.downloadermiddlewares.httpcompression.HttpCompressionMiddleware": 810,
}

SPIDER_MIDDLEWARES = {
    "middlewares.stats_collector.StatsCollectorMiddleware": 100,
}

# ──────────────────────────────────────────
# Item Pipelines
# ──────────────────────────────────────────
ITEM_PIPELINES = {
    "pipelines.ValidationPipeline": 100,
    "pipelines.NormalizerPipeline": 200,
    "pipelines.DeduplicationPipeline": 300,
    "pipelines.DatabasePipeline": 400,
}

# ──────────────────────────────────────────
# Base de datos
# ──────────────────────────────────────────
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://autopulse:autopulse_secret@localhost:5432/autopulse"
)

# ──────────────────────────────────────────
# Logging
# ──────────────────────────────────────────
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FORMAT = "%(asctime)s [%(name)s] %(levelname)s: %(message)s"

# ──────────────────────────────────────────
# Headers por defecto (parecer un browser real)
# ──────────────────────────────────────────
DEFAULT_REQUEST_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-PA,es;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

# ──────────────────────────────────────────
# Caché de requests (útil en desarrollo para no re-scrapear)
# ──────────────────────────────────────────
HTTPCACHE_ENABLED = os.getenv("ENVIRONMENT", "development") == "development"
HTTPCACHE_EXPIRATION_SECS = 3600        # Cache válido por 1 hora en dev
HTTPCACHE_DIR = ".scrapy_cache"
HTTPCACHE_IGNORE_HTTP_CODES = [404, 429, 500, 502, 503]

# ──────────────────────────────────────────
# Autothrottle — ajusta velocidad automáticamente
# ──────────────────────────────────────────
AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_START_DELAY = 2.0
AUTOTHROTTLE_MAX_DELAY = 15.0
AUTOTHROTTLE_TARGET_CONCURRENCY = 1.5
AUTOTHROTTLE_DEBUG = False

# ──────────────────────────────────────────
# Respetar robots.txt (recomendado legalmente)
# ──────────────────────────────────────────
ROBOTSTXT_OBEY = False   # Muchos sitios de autos lo bloquean incorrectamente
                          # Revisar manualmente los T&C de cada sitio

# ──────────────────────────────────────────
# Feed exports (para debugging)
# ──────────────────────────────────────────
FEEDS = {}   # Se configuran por spider cuando se necesita
