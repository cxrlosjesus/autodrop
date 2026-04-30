# ============================================================
# AutoPulse Panamá — Makefile
# Comandos frecuentes para desarrollo y operación
# ============================================================

.PHONY: help up down logs shell-scrapy shell-dbt shell-db \
        scrape-all scrape dbt-run dbt-test dbt-seed \
        setup status clean

# Variables
COMPOSE = docker compose
SCRAPERS = docker compose run --rm scrapy
DBT     = docker compose run --rm dbt
DB      = docker compose exec postgres psql -U autopulse -d autopulse

# ──────────────────────────────────────────
# Ayuda
# ──────────────────────────────────────────
help:
	@echo ""
	@echo "╔══════════════════════════════════════════╗"
	@echo "║       AutoPulse Panamá — Comandos        ║"
	@echo "╚══════════════════════════════════════════╝"
	@echo ""
	@echo "🚀 INFRAESTRUCTURA"
	@echo "  make up          Levantar todos los servicios"
	@echo "  make down        Bajar todos los servicios"
	@echo "  make logs        Ver logs en tiempo real"
	@echo "  make status      Estado de todos los servicios"
	@echo ""
	@echo "🔧 SETUP (primera vez)"
	@echo "  make setup       Inicializar DB + seeds dbt"
	@echo ""
	@echo "🕷️  SCRAPING"
	@echo "  make scrape-all  Ejecutar todos los spiders"
	@echo "  make scrape s=encuentra24    Spider específico"
	@echo ""
	@echo "📊 DBT"
	@echo "  make dbt-seed    Cargar datos de referencia"
	@echo "  make dbt-run     Bronze → Silver → Gold"
	@echo "  make dbt-test    Validar calidad de datos"
	@echo "  make dbt-docs    Generar documentación"
	@echo ""
	@echo "🗄️  BASE DE DATOS"
	@echo "  make db-status   Ver estado del sistema"
	@echo "  make db-shell    Shell de PostgreSQL"
	@echo ""

# ──────────────────────────────────────────
# Infraestructura
# ──────────────────────────────────────────
up:
	@echo "🚀 Levantando AutoPulse..."
	cp -n .env.example .env 2>/dev/null || true
	$(COMPOSE) up -d postgres pgadmin prefect
	@echo "✅ Servicios corriendo:"
	@echo "   PostgreSQL  → localhost:5432"
	@echo "   pgAdmin     → http://localhost:5050"
	@echo "   Prefect UI  → http://localhost:4200"

down:
	$(COMPOSE) down

logs:
	$(COMPOSE) logs -f --tail=100

status:
	$(COMPOSE) ps
	@echo ""
	@echo "📊 Estado de la base de datos:"
	@$(DB) -c "SELECT * FROM public.system_status;" 2>/dev/null || echo "(DB no disponible)"

# ──────────────────────────────────────────
# Setup inicial
# ──────────────────────────────────────────
setup: up
	@echo "⏳ Esperando que PostgreSQL esté listo..."
	sleep 5
	@echo "🔧 Cargando seeds de dbt..."
	$(DBT) dbt seed --profiles-dir .
	@echo "🔧 Verificando modelos Bronze..."
	$(DBT) dbt run --select bronze --profiles-dir .
	@echo ""
	@echo "✅ Setup completado. Ahora puedes ejecutar: make scrape-all"

# ──────────────────────────────────────────
# Scraping
# ──────────────────────────────────────────
scrape-all:
	@echo "🕷️ Ejecutando todos los spiders..."
	$(SCRAPERS) scrapy crawl encuentra24 &
	$(SCRAPERS) scrapy crawl clasificar &
	wait
	@echo "✅ Scraping completado"

scrape:
	@if [ -z "$(s)" ]; then echo "❌ Especifica spider: make scrape s=encuentra24"; exit 1; fi
	@echo "🕷️ Ejecutando spider: $(s)"
	$(SCRAPERS) scrapy crawl $(s)

# ──────────────────────────────────────────
# dbt
# ──────────────────────────────────────────
dbt-seed:
	$(DBT) dbt seed --profiles-dir .

dbt-run:
	$(DBT) dbt run --profiles-dir .

dbt-test:
	$(DBT) dbt test --profiles-dir .

dbt-docs:
	$(DBT) dbt docs generate --profiles-dir .
	$(DBT) dbt docs serve --profiles-dir . --port 8080
	@echo "📚 Docs disponibles en: http://localhost:8080"

# ──────────────────────────────────────────
# Base de datos
# ──────────────────────────────────────────
db-shell:
	$(DB)

db-status:
	$(DB) -c "SELECT * FROM public.system_status;"

db-listings:
	$(DB) -c "SELECT source_site, COUNT(*) as total, MAX(first_seen_at) as ultimo FROM silver.listings GROUP BY source_site ORDER BY total DESC;"

# ──────────────────────────────────────────
# Limpieza
# ──────────────────────────────────────────
clean:
	$(COMPOSE) down -v
	rm -rf .scrapy_cache
	@echo "✅ Limpieza completada (datos de DB eliminados)"
