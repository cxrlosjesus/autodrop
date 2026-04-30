# AutoPulse Panamá — Sistema de Inteligencia Competitiva Automotriz

> Inteligencia de precios e inventario del mercado automotriz panameño.

---

## Stack Tecnológico

| Capa | Tecnología | Función |
|------|-----------|---------|
| Extracción | Scrapy + Playwright | Scraping de 23+ sitios |
| Almacenamiento | PostgreSQL (Medallion) | Bronze / Silver / Gold |
| Transformación | dbt-core | Normalización y métricas |
| Orquestación | Prefect | Scheduling y monitoring |
| Infraestructura | Docker Compose | Todo empaquetado |

---

## Quickstart

### Requisitos
- Docker Desktop (o Docker Engine + Compose)
- Git
- Make (incluido en Mac/Linux, en Windows usar Git Bash)

### 1. Clonar y configurar
```bash
git clone <repo>
cd autopulse
cp .env.example .env
# Editar .env con tus passwords
```

### 2. Levantar servicios
```bash
make up
```

### 3. Setup inicial (solo la primera vez)
```bash
make setup
```

### 4. Ejecutar primer scraping
```bash
make scrape s=encuentra24
```

### 5. Transformar datos con dbt
```bash
make dbt-run
make dbt-test
```

### 6. Ver resultados
```bash
make db-status          # Estado general
make db-listings        # Listings por sitio
```

---

## Interfaces Web

| Servicio | URL | Credenciales |
|---------|-----|-------------|
| pgAdmin (DB) | http://localhost:5050 | Ver .env |
| Prefect UI | http://localhost:4200 | Sin auth en dev |

---

## Estructura del Proyecto

```
autopulse/
├── docker-compose.yml       # Stack completo
├── .env.example             # Variables de entorno (copiar a .env)
├── Makefile                 # Comandos frecuentes
│
├── scrapers/                # Proyecto Scrapy
│   ├── spiders/
│   │   ├── base.py          # Clase base de todos los spiders
│   │   ├── encuentra24.py   # Spider principal (más anuncios)
│   │   └── ...
│   ├── pipelines.py         # Validación → Normalización → DB
│   ├── items.py             # Modelo de datos del scraper
│   └── settings.py          # Configuración central
│
├── dbt/                     # Proyecto dbt
│   ├── models/
│   │   ├── bronze/          # Vistas sobre raw data
│   │   ├── silver/          # Datos limpios
│   │   └── gold/            # Métricas de mercado
│   ├── seeds/
│   │   └── brand_lookup.csv # Normalización de marcas
│   └── dbt_project.yml
│
├── flows/                   # Prefect flows
│   └── main_flow.py         # Pipeline diario + health checks
│
└── database/
    └── migrations/
        └── 001_init.sql     # Schema inicial
```

---

## Agregar un Nuevo Spider

1. Crear `scrapers/spiders/nuevo_sitio.py`
2. Heredar de `AutoPulseSpider`
3. Implementar `start_requests()` y `parse_listing_detail()`
4. Agregar al array `SPIDERS` en `flows/main_flow.py`
5. Probar: `make scrape s=nuevo_sitio`

---

## Comandos Frecuentes

```bash
make help           # Ver todos los comandos disponibles
make up             # Levantar servicios
make scrape-all     # Scrapear todos los sitios
make dbt-run        # Transformar datos
make dbt-test       # Validar calidad
make status         # Estado del sistema
make logs           # Ver logs en tiempo real
```

---

*AutoPulse Panamá | Confidencial | Versión 1.0 | Abril 2026*
