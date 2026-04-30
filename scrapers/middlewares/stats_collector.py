"""
AutoPulse Panamá — Middleware: Recolección de estadísticas por spider

Captura métricas de cada run (items scrapeados, errores, duración)
y las escribe en bronze.scrape_runs al terminar el spider.
"""
import os
from datetime import datetime, timezone
from loguru import logger


class StatsCollectorMiddleware:

    def process_spider_output(self, response, result, spider):
        for item in result:
            yield item

    def process_spider_exception(self, response, exception, spider):
        spider._errors_count += 1
        logger.error(f"Spider exception en {response.url}: {exception}")
        return None

    @classmethod
    def from_crawler(cls, crawler):
        return cls()
