"""
AutoPulse Panamá — Modelos de Items de Scrapy

Cada spider produce un CarListingItem que luego pasa por
los pipelines de validación, normalización y almacenamiento.
"""
import scrapy
from dataclasses import dataclass, field
from typing import Optional
from datetime import datetime


class CarListingItem(scrapy.Item):
    """
    Item principal. Un registro = un anuncio de auto.
    Todos los campos son opcionales excepto source_site y source_url.
    Los pipelines validan y normalizan antes de guardar.
    """
    # ── Trazabilidad (requeridos) ──
    source_site     = scrapy.Field()   # 'encuentra24', 'clasificar', 'carspot', etc.
    source_url      = scrapy.Field()   # URL exacta del anuncio
    scrape_run_id   = scrapy.Field()   # UUID del run actual (lo inyecta el spider)
    spider_version  = scrapy.Field()   # Versión del spider

    # ── Datos del vehículo (raw, como vienen del sitio) ──
    brand_raw       = scrapy.Field()   # Marca tal como viene: 'TOYOTA', 'toyota', etc.
    model_raw       = scrapy.Field()   # Modelo tal como viene
    year_raw        = scrapy.Field()   # Año como string: '2022', '22', etc.
    price_raw       = scrapy.Field()   # Precio como string: '$15,500', '15.500', etc.
    mileage_raw     = scrapy.Field()   # Km como string: '45,000 km', '45000', etc.

    # ── Datos normalizados (los llenan los pipelines) ──
    brand           = scrapy.Field()   # 'Toyota'
    model           = scrapy.Field()   # 'Hilux'
    year            = scrapy.Field()   # 2022 (int)
    price_usd       = scrapy.Field()   # 15500.00 (float)
    mileage_km      = scrapy.Field()   # 45000 (int)

    # ── Características adicionales ──
    trim            = scrapy.Field()   # 'SR5', 'GLS', 'EX'
    transmission    = scrapy.Field()   # 'Automatico', 'Manual', 'CVT'
    body_type       = scrapy.Field()   # 'SUV', 'Sedan', 'Pickup', 'Hatchback', 'Van'
    fuel_type       = scrapy.Field()   # 'Gasolina', 'Diesel', 'Hibrido', 'Electrico'
    color           = scrapy.Field()
    doors           = scrapy.Field()   # 2, 4 (int)
    condition       = scrapy.Field()   # 'Nuevo', 'Usado'

    # ── Localización ──
    location_city       = scrapy.Field()
    location_province   = scrapy.Field()

    # ── Contenido ──
    title           = scrapy.Field()   # Título del anuncio
    description     = scrapy.Field()   # Descripción completa
    image_urls      = scrapy.Field()   # Lista de URLs de imágenes

    # ── Fechas ──
    published_at    = scrapy.Field()   # Fecha de publicación del vendor (si disponible)
    scraped_at      = scrapy.Field()   # Timestamp del scraping (lo inyecta el spider)

    # ── Datos extra (JSONB en Bronze) ──
    extra_data      = scrapy.Field()   # Cualquier dato adicional específico del sitio


class ScrapeRunItem(scrapy.Item):
    """Metadata del run de scraping. Lo genera el spider al terminar."""
    spider_name     = scrapy.Field()
    started_at      = scrapy.Field()
    finished_at     = scrapy.Field()
    listings_found  = scrapy.Field()
    errors_count    = scrapy.Field()
    error_detail    = scrapy.Field()
