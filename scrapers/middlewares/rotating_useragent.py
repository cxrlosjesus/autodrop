"""
AutoPulse Panamá — Middleware: Rotación de User-Agents

Rota automáticamente entre User-Agents reales de browsers modernos
para evitar detección de scraping.
"""
import random
from loguru import logger


# User-Agents reales de browsers modernos (actualizados Abril 2026)
USER_AGENTS = [
    # Chrome Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    # Chrome Mac
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    # Firefox Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    # Firefox Mac
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.4; rv:125.0) Gecko/20100101 Firefox/125.0",
    # Safari Mac
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Safari/605.1.15",
    # Edge Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0",
    # Chrome Linux
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]


class RotatingUserAgentMiddleware:
    """
    Middleware que rota User-Agents en cada request.
    Se activa automáticamente via DOWNLOADER_MIDDLEWARES en settings.py
    """

    def __init__(self):
        self.user_agents = USER_AGENTS
        logger.info(f"🔄 RotatingUserAgent: {len(self.user_agents)} User-Agents disponibles")

    def process_request(self, request, spider):
        ua = random.choice(self.user_agents)
        request.headers["User-Agent"] = ua
        return None  # Continuar procesando el request
