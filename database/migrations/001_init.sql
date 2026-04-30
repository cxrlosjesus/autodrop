-- ============================================================
-- AutoPulse Panamá — Schema de Base de Datos
-- Arquitectura Medallion: Bronze / Silver / Gold
-- Versión 1.0 | Abril 2026
-- ============================================================

-- ──────────────────────────────────────────
-- SCHEMAS
-- ──────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- ──────────────────────────────────────────
-- EXTENSIONES
-- ──────────────────────────────────────────
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";   -- Para búsquedas fuzzy de marcas/modelos

-- ============================================================
-- BRONZE — Raw data tal como llega del scraper
-- ============================================================

CREATE TABLE IF NOT EXISTS bronze.raw_listings (
    id              BIGSERIAL PRIMARY KEY,
    source_site     VARCHAR(100) NOT NULL,          -- 'encuentra24', 'clasificar', 'carspot', etc.
    source_url      TEXT NOT NULL,                  -- URL exacta del anuncio
    raw_data        JSONB NOT NULL,                 -- Todo el dato crudo como JSON
    scraped_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    scrape_run_id   UUID,                           -- ID del run de Prefect que lo generó
    spider_version  VARCHAR(20),                    -- Versión del spider (para debugging)
    http_status     SMALLINT                        -- HTTP status code del request
);

-- Función inmutable necesaria para el índice expresional (::DATE depende del TimeZone GUC)
CREATE OR REPLACE FUNCTION bronze.to_utc_date(ts TIMESTAMPTZ)
RETURNS DATE LANGUAGE SQL IMMUTABLE AS $$
    SELECT (ts AT TIME ZONE 'UTC')::DATE;
$$;

-- Unique index por URL + día UTC
CREATE UNIQUE INDEX IF NOT EXISTS raw_listings_url_scraped_unique
    ON bronze.raw_listings(source_url, bronze.to_utc_date(scraped_at));

-- Índices Bronze
CREATE INDEX IF NOT EXISTS idx_bronze_source_site ON bronze.raw_listings(source_site);
CREATE INDEX IF NOT EXISTS idx_bronze_scraped_at  ON bronze.raw_listings(scraped_at DESC);
CREATE INDEX IF NOT EXISTS idx_bronze_run_id      ON bronze.raw_listings(scrape_run_id);
CREATE INDEX IF NOT EXISTS idx_bronze_raw_data    ON bronze.raw_listings USING GIN(raw_data);

COMMENT ON TABLE bronze.raw_listings IS 
'Raw data sin transformar. Nunca se modifica una vez insertado. 
 Todo dato tiene trazabilidad completa al scrape_run_id.';

-- ──────────────────────────────────────────
-- Log de runs de scraping
-- ──────────────────────────────────────────
CREATE TABLE IF NOT EXISTS bronze.scrape_runs (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    spider_name     VARCHAR(100) NOT NULL,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at     TIMESTAMPTZ,
    status          VARCHAR(20) DEFAULT 'running',  -- running | success | failed | partial
    listings_found  INTEGER DEFAULT 0,
    listings_new    INTEGER DEFAULT 0,
    errors_count    INTEGER DEFAULT 0,
    error_detail    TEXT,
    metadata        JSONB                           -- Info adicional del run
);

COMMENT ON TABLE bronze.scrape_runs IS
'Auditoría de cada ejecución de spider. Permite detectar cuándo un sitio deja de funcionar.';

-- ============================================================
-- SILVER — Datos limpios, normalizados y deduplicados
-- ============================================================

CREATE TABLE IF NOT EXISTS silver.listings (
    listing_id          UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    
    -- Origen y trazabilidad
    bronze_id           BIGINT REFERENCES bronze.raw_listings(id),
    source_site         VARCHAR(100) NOT NULL,
    source_url          TEXT NOT NULL UNIQUE,
    
    -- Datos del vehículo (normalizados)
    brand               VARCHAR(100),               -- 'Toyota', 'Hyundai', 'Kia' (normalizado)
    model               VARCHAR(200),               -- 'Hilux', 'Accent', 'Sportage'
    trim                VARCHAR(200),               -- 'SR5', 'GLS', 'EX' (cuando disponible)
    year                SMALLINT,                   -- 2018
    price_usd           NUMERIC(10,2),              -- 15500.00
    mileage_km          INTEGER,                    -- 45000
    
    -- Características
    transmission        VARCHAR(50),                -- 'Automatico', 'Manual', 'CVT'
    body_type           VARCHAR(50),                -- 'SUV', 'Sedan', 'Pickup', 'Hatchback'
    fuel_type           VARCHAR(50),                -- 'Gasolina', 'Diesel', 'Hibrido', 'Electrico'
    color               VARCHAR(50),
    doors               SMALLINT,
    condition           VARCHAR(20),                -- 'Nuevo', 'Usado'
    
    -- Localización
    location_city       VARCHAR(100),               -- 'Ciudad de Panamá', 'Colón', etc.
    location_province   VARCHAR(100),
    
    -- Descripción
    title               TEXT,                       -- Título original del anuncio
    description         TEXT,                       -- Descripción completa
    
    -- Métricas de tiempo
    first_seen_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at        TIMESTAMPTZ,                -- Fecha que puso el vendor (si disponible)
    days_on_market      INTEGER,                        -- calculado por dbt: EXTRACT(DAY FROM NOW() - first_seen_at)
    
    -- Estado
    is_active           BOOLEAN DEFAULT TRUE,
    deactivated_at      TIMESTAMPTZ,               -- Cuándo dejó de verse en el sitio
    
    -- Control
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW(),
    
    -- Fingerprint para deduplicación cross-site
    dedup_hash          VARCHAR(64)                 -- SHA256 de brand+model+year+price+km
);

-- Índices Silver (los más importantes para queries del producto)
CREATE INDEX IF NOT EXISTS idx_silver_brand         ON silver.listings(brand);
CREATE INDEX IF NOT EXISTS idx_silver_model         ON silver.listings(model);
CREATE INDEX IF NOT EXISTS idx_silver_brand_model   ON silver.listings(brand, model);
CREATE INDEX IF NOT EXISTS idx_silver_year          ON silver.listings(year);
CREATE INDEX IF NOT EXISTS idx_silver_price         ON silver.listings(price_usd);
CREATE INDEX IF NOT EXISTS idx_silver_active        ON silver.listings(is_active) WHERE is_active = TRUE;
CREATE INDEX IF NOT EXISTS idx_silver_source        ON silver.listings(source_site);
CREATE INDEX IF NOT EXISTS idx_silver_first_seen    ON silver.listings(first_seen_at DESC);
CREATE INDEX IF NOT EXISTS idx_silver_dedup         ON silver.listings(dedup_hash);

COMMENT ON TABLE silver.listings IS
'Datos limpios y normalizados. Un registro por anuncio único.
 La deduplicación cross-site usa dedup_hash.
 days_on_market se calcula automáticamente.';

-- ──────────────────────────────────────────
-- Historial de precios (para análisis de tendencias)
-- ──────────────────────────────────────────
CREATE TABLE IF NOT EXISTS silver.price_history (
    id              BIGSERIAL PRIMARY KEY,
    listing_id      UUID REFERENCES silver.listings(listing_id),
    price_usd       NUMERIC(10,2) NOT NULL,
    recorded_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    change_amount   NUMERIC(10,2),                  -- Diferencia vs precio anterior
    change_pct      NUMERIC(5,2)                    -- Porcentaje de cambio
);

CREATE INDEX IF NOT EXISTS idx_price_history_listing ON silver.price_history(listing_id);
CREATE INDEX IF NOT EXISTS idx_price_history_date    ON silver.price_history(recorded_at DESC);

-- ============================================================
-- GOLD — Métricas calculadas, listas para el producto
-- ============================================================

-- Métricas de mercado por marca/modelo/año (se refresca con dbt)
CREATE TABLE IF NOT EXISTS gold.market_summary (
    id              BIGSERIAL PRIMARY KEY,
    
    -- Dimensiones
    brand           VARCHAR(100) NOT NULL,
    model           VARCHAR(200) NOT NULL,
    year            SMALLINT NOT NULL,
    
    -- Métricas de precio
    avg_price       NUMERIC(10,2),
    median_price    NUMERIC(10,2),
    min_price       NUMERIC(10,2),
    max_price       NUMERIC(10,2),
    price_std_dev   NUMERIC(10,2),
    
    -- Métricas de inventario
    total_listings      INTEGER,
    active_listings     INTEGER,
    
    -- Métricas de tiempo en mercado
    avg_days_on_market  NUMERIC(6,1),
    median_days_market  NUMERIC(6,1),
    
    -- Distribución por sitio
    listings_by_site    JSONB,                      -- {"encuentra24": 12, "clasificar": 5, ...}
    
    -- Control de frescura
    computed_at         TIMESTAMPTZ DEFAULT NOW(),
    data_from           TIMESTAMPTZ,                -- Fecha más antigua de datos incluidos
    data_to             TIMESTAMPTZ,                -- Fecha más reciente
    
    UNIQUE(brand, model, year)
);

CREATE INDEX IF NOT EXISTS idx_gold_brand_model ON gold.market_summary(brand, model);
CREATE INDEX IF NOT EXISTS idx_gold_computed    ON gold.market_summary(computed_at DESC);

-- Comparativa de precios por dealer (para el producto principal)
CREATE TABLE IF NOT EXISTS gold.dealer_comparison (
    id              BIGSERIAL PRIMARY KEY,
    brand           VARCHAR(100) NOT NULL,
    model           VARCHAR(200) NOT NULL,
    year            SMALLINT NOT NULL,
    source_site     VARCHAR(100) NOT NULL,
    
    avg_price       NUMERIC(10,2),
    listing_count   INTEGER,
    avg_days_market NUMERIC(6,1),
    vs_market_avg   NUMERIC(6,2),                   -- % diferencia vs promedio del mercado
    
    computed_at     TIMESTAMPTZ DEFAULT NOW(),
    
    UNIQUE(brand, model, year, source_site)
);

-- ──────────────────────────────────────────
-- FUNCIÓN: Actualizar updated_at automáticamente
-- ──────────────────────────────────────────
CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER listings_updated_at
    BEFORE UPDATE ON silver.listings
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();

-- ──────────────────────────────────────────
-- FUNCIÓN: Registrar cambios de precio
-- ──────────────────────────────────────────
CREATE OR REPLACE FUNCTION record_price_change()
RETURNS TRIGGER AS $$
BEGIN
    IF OLD.price_usd IS DISTINCT FROM NEW.price_usd THEN
        INSERT INTO silver.price_history (listing_id, price_usd, change_amount, change_pct)
        VALUES (
            NEW.listing_id,
            NEW.price_usd,
            NEW.price_usd - OLD.price_usd,
            ROUND(((NEW.price_usd - OLD.price_usd) / OLD.price_usd * 100)::NUMERIC, 2)
        );
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER listings_price_change
    AFTER UPDATE ON silver.listings
    FOR EACH ROW EXECUTE FUNCTION record_price_change();

-- ──────────────────────────────────────────
-- VIEWS útiles para debugging y análisis rápido
-- ──────────────────────────────────────────

-- Vista: resumen rápido del estado del sistema
CREATE OR REPLACE VIEW public.system_status AS
SELECT
    'bronze.raw_listings'    AS tabla,
    COUNT(*)                 AS total_registros,
    MAX(scraped_at)          AS ultimo_registro
FROM bronze.raw_listings
UNION ALL
SELECT
    'silver.listings',
    COUNT(*),
    MAX(last_seen_at)
FROM silver.listings
UNION ALL
SELECT
    'silver.listings (activos)',
    COUNT(*),
    MAX(last_seen_at)
FROM silver.listings WHERE is_active = TRUE
UNION ALL
SELECT
    'gold.market_summary',
    COUNT(*),
    MAX(computed_at)
FROM gold.market_summary;

-- Vista: listings activos con métricas básicas (para queries rápidas)
CREATE OR REPLACE VIEW public.active_listings AS
SELECT
    l.listing_id,
    l.source_site,
    l.brand,
    l.model,
    l.year,
    l.price_usd,
    l.mileage_km,
    l.transmission,
    l.body_type,
    l.condition,
    l.days_on_market,
    l.first_seen_at,
    l.source_url,
    ms.avg_price                                        AS market_avg_price,
    ROUND((l.price_usd - ms.avg_price) / ms.avg_price * 100, 1) AS pct_vs_market
FROM silver.listings l
LEFT JOIN gold.market_summary ms
    ON l.brand = ms.brand AND l.model = ms.model AND l.year = ms.year
WHERE l.is_active = TRUE
ORDER BY l.brand, l.model, l.year, l.price_usd;

COMMENT ON VIEW public.active_listings IS
'Vista principal para análisis. Muestra todos los activos con su posición vs el mercado.';

-- ──────────────────────────────────────────
-- DATOS SEMILLA: Lookup de marcas (normalización)
-- ──────────────────────────────────────────
CREATE TABLE IF NOT EXISTS public.brand_lookup (
    raw_value       VARCHAR(200) PRIMARY KEY,    -- Como viene del sitio
    normalized      VARCHAR(100) NOT NULL        -- Cómo lo guardamos
);

INSERT INTO public.brand_lookup (raw_value, normalized) VALUES
('TOYOTA', 'Toyota'), ('toyota', 'Toyota'), ('Toyotá', 'Toyota'),
('HYUNDAI', 'Hyundai'), ('hyundai', 'Hyundai'), ('Hyundai Motors', 'Hyundai'),
('KIA', 'Kia'), ('kia', 'Kia'), ('Kía', 'Kia'),
('HONDA', 'Honda'), ('honda', 'Honda'),
('NISSAN', 'Nissan'), ('nissan', 'Nissan'),
('MITSUBISHI', 'Mitsubishi'), ('mitsubishi', 'Mitsubishi'),
('FORD', 'Ford'), ('ford', 'Ford'),
('CHEVROLET', 'Chevrolet'), ('chevrolet', 'Chevrolet'), ('CHEVY', 'Chevrolet'),
('MAZDA', 'Mazda'), ('mazda', 'Mazda'),
('VOLKSWAGEN', 'Volkswagen'), ('VW', 'Volkswagen'), ('volkswagen', 'Volkswagen'),
('BMW', 'BMW'), ('bmw', 'BMW'),
('MERCEDES', 'Mercedes-Benz'), ('MERCEDES-BENZ', 'Mercedes-Benz'), ('Mercedes Benz', 'Mercedes-Benz'),
('AUDI', 'Audi'), ('audi', 'Audi'),
('JEEP', 'Jeep'), ('jeep', 'Jeep'),
('RAM', 'Ram'), ('ram', 'Ram'),
('DODGE', 'Dodge'), ('dodge', 'Dodge'),
('SUBARU', 'Subaru'), ('subaru', 'Subaru'),
('SUZUKI', 'Suzuki'), ('suzuki', 'Suzuki'),
('ISUZU', 'Isuzu'), ('isuzu', 'Isuzu'),
('LEXUS', 'Lexus'), ('lexus', 'Lexus'),
('INFINITI', 'Infiniti'), ('infiniti', 'Infiniti'),
('ACURA', 'Acura'), ('acura', 'Acura'),
('LAND ROVER', 'Land Rover'), ('LANDROVER', 'Land Rover'),
('VOLVO', 'Volvo'), ('volvo', 'Volvo'),
('PEUGEOT', 'Peugeot'), ('peugeot', 'Peugeot'),
('RENAULT', 'Renault'), ('renault', 'Renault'),
('BYD', 'BYD'), ('byd', 'BYD'),
('CHERY', 'Chery'), ('chery', 'Chery'),
('JAC', 'JAC'), ('jac', 'JAC'),
('GREAT WALL', 'Great Wall'), ('GWMOTOR', 'Great Wall')
ON CONFLICT (raw_value) DO NOTHING;

-- ──────────────────────────────────────────
-- Confirmar todo creado correctamente
-- ──────────────────────────────────────────
DO $$
BEGIN
    RAISE NOTICE '✅ AutoPulse DB inicializada correctamente';
    RAISE NOTICE '   Schemas: bronze, silver, gold';
    RAISE NOTICE '   Tablas:  bronze.raw_listings, bronze.scrape_runs';
    RAISE NOTICE '            silver.listings, silver.price_history';
    RAISE NOTICE '            gold.market_summary, gold.dealer_comparison';
    RAISE NOTICE '   Views:   public.system_status, public.active_listings';
    RAISE NOTICE '   Lookup:  public.brand_lookup (%  marcas cargadas)', 
                 (SELECT COUNT(*) FROM public.brand_lookup);
END $$;
