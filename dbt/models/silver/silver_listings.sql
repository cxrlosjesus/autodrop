{{
  config(
    materialized     = 'incremental',
    unique_key       = 'source_url',
    on_schema_change = 'append_new_columns',
    post_hook        = [
      """
      UPDATE {{ this }}
      SET
          is_active      = FALSE,
          deactivated_at = NOW()
      WHERE
          is_active  = TRUE
          AND last_seen_at < NOW() - INTERVAL '3 days'
      """
    ]
  )
}}

/*
  AutoPulse — Modelo Silver: listings normalizados

  Transforma bronze.raw_listings en datos limpios y tipados.
  - Normaliza marcas usando brand_lookup
  - Parsea precios, años y kilometrajes
  - Deduplica por source_url
  - Calcula dedup_hash para deduplicación cross-site

  Ciclo de vida de un listing:
    - Aparece en el sitio  → is_active=TRUE,  first_seen_at=ahora
    - Sigue en el sitio    → last_seen_at se actualiza en cada run
    - Desaparece (vendido) → post_hook lo marca is_active=FALSE, deactivated_at=ahora
    - days_on_market       → días entre first_seen_at y deactivated_at (o NOW() si sigue activo)

  En modo incremental: solo procesa registros nuevos desde el último run.
*/

WITH raw AS (

    SELECT
        id                                          AS bronze_id,
        source_site,
        source_url,
        raw_data,
        scraped_at,
        scrape_run_id

    FROM {{ source('bronze', 'raw_listings') }}

    {% if is_incremental() %}
    -- Solo datos nuevos desde el último run
    WHERE scraped_at > (SELECT MAX(last_seen_at) FROM {{ this }})
    {% endif %}

),

-- Preservar first_seen_at de registros que ya existen en Silver
existing AS (

    {% if is_incremental() %}
    SELECT source_url, first_seen_at
    FROM {{ this }}
    {% else %}
    SELECT NULL::TEXT AS source_url, NULL::TIMESTAMPTZ AS first_seen_at
    WHERE FALSE
    {% endif %}

),

-- Extraer campos del JSONB
extracted AS (

    SELECT
        bronze_id,
        source_site,
        source_url,
        scraped_at,
        scrape_run_id,

        -- Texto tal como viene del spider (ya normalizado por el pipeline Python)
        NULLIF(TRIM(raw_data->>'brand'), '')            AS brand_raw,
        NULLIF(TRIM(raw_data->>'model'), '')            AS model_raw,
        NULLIF(TRIM(raw_data->>'title'), '')            AS title,
        NULLIF(TRIM(raw_data->>'description'), '')      AS description,
        NULLIF(TRIM(raw_data->>'transmission'), '')     AS transmission,
        NULLIF(TRIM(raw_data->>'body_type'), '')        AS body_type,
        NULLIF(TRIM(raw_data->>'fuel_type'), '')        AS fuel_type,
        NULLIF(TRIM(raw_data->>'color'), '')            AS color,
        NULLIF(TRIM(raw_data->>'condition'), '')        AS condition,
        NULLIF(TRIM(raw_data->>'location_city'), '')    AS location_city,
        NULLIF(TRIM(raw_data->>'location_province'), '') AS location_province,

        -- Numéricos (ya parseados por el pipeline Python)
        (raw_data->>'price_usd')::NUMERIC(10,2)        AS price_usd,
        (raw_data->>'mileage_km')::INTEGER              AS mileage_km,
        (raw_data->>'year')::SMALLINT                   AS year,
        (raw_data->>'doors')::SMALLINT                  AS doors,

        -- Fechas
        (raw_data->>'published_at')::TIMESTAMPTZ        AS published_at,

        -- Hash para deduplicación cross-site
        raw_data->'extra_data'->>'dedup_hash'           AS dedup_hash

    FROM raw

),

-- Normalizar marca usando lookup table
normalized AS (

    SELECT
        e.*,

        -- Marca normalizada: lookup primero, luego el valor raw
        COALESCE(
            bl.normalized,
            INITCAP(LOWER(TRIM(e.brand_raw)))
        )                                               AS brand,

        -- Modelo: limpiar y capitalizar
        INITCAP(LOWER(TRIM(e.model_raw)))               AS model,

        -- first_seen_at real: preservar el original si el listing ya existía
        COALESCE(ex.first_seen_at, e.scraped_at)        AS real_first_seen_at

    FROM extracted e
    LEFT JOIN {{ ref('brand_lookup') }} bl
        ON LOWER(TRIM(e.brand_raw)) = LOWER(bl.raw_value)
    LEFT JOIN existing ex
        ON ex.source_url = e.source_url

),

-- Deduplicar por source_url dentro del batch actual (por si el mismo URL
-- fue scrapeado múltiples veces): conservar el registro más reciente
normalized_deduped AS (

    SELECT DISTINCT ON (source_url) *
    FROM normalized
    ORDER BY source_url, scraped_at DESC

),

-- Aplicar filtros de calidad y calcular campos derivados
final AS (

    SELECT
        -- Generar UUID estable basado en source_url
        md5(source_url)::uuid                           AS listing_id,
        bronze_id,
        source_site,
        source_url,
        brand,
        model,
        year,
        price_usd,
        mileage_km,
        transmission,
        body_type,
        fuel_type,
        color,
        CASE doors
            WHEN 2 THEN 2
            WHEN 4 THEN 4
            ELSE NULL
        END                                             AS doors,
        condition,
        location_city,
        location_province,
        title,
        description,
        published_at,
        dedup_hash,
        real_first_seen_at                              AS first_seen_at,
        scraped_at                                      AS last_seen_at,
        TRUE                                            AS is_active,
        NULL::TIMESTAMPTZ                               AS deactivated_at,
        EXTRACT(DAY FROM scraped_at - real_first_seen_at)::INTEGER
                                                        AS days_on_market,
        NOW()                                           AS created_at,
        NOW()                                           AS updated_at

    FROM normalized_deduped

    WHERE
        -- Filtros de calidad mínima
        source_url IS NOT NULL
        AND (
            price_usd IS NULL
            OR price_usd BETWEEN {{ var('price_min_usd') }} AND {{ var('price_max_usd') }}
        )
        AND (
            year IS NULL
            OR year BETWEEN {{ var('min_vehicle_year') }} AND EXTRACT(YEAR FROM NOW())::INT + 1
        )
        -- Excluir listings de Venezuela que se cuelan en Encuentra24 (marketplace latinoamericano)
        AND (
            location_city IS NULL
            OR LOWER(location_city) NOT IN (
                'caracas', 'maracaibo', 'valencia', 'barquisimeto', 'maracay',
                'barcelona', 'maturín', 'maturin', 'san cristóbal', 'san cristobal',
                'mérida', 'merida', 'cabimas', 'ciudad bolívar', 'ciudad bolivar',
                'punto fijo', 'los teques', 'guarenas', 'guatire', 'cumaná', 'cumana',
                'puerto la cruz', 'puerto ordaz', 'san tomé', 'san tome',
                'acarigua', 'porlamar', 'la victoria', 'cagua', 'turmero',
                'calabozo', 'el tigre', 'zaraza', 'tucupita', 'carabobo'
            )
        )

)

SELECT * FROM final
