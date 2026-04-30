{{
  config(
    materialized = 'table',
    tags = ['gold', 'daily']
  )
}}

/*
  AutoPulse — Modelo Gold: Resumen de Mercado por Marca/Modelo/Año

  Este es el modelo principal que alimenta:
  - Los reportes PDF para clientes Plan Básico
  - El dashboard web para clientes Pro
  - La API para clientes Enterprise

  Se recalcula completamente en cada run (no incremental)
  porque las métricas del mercado cambian con cada nuevo anuncio.
*/

WITH activos AS (

    SELECT *
    FROM {{ ref('silver_listings') }}
    WHERE
        is_active = TRUE
        AND price_usd IS NOT NULL
        AND price_usd BETWEEN {{ var('price_min_usd') }} AND {{ var('price_max_usd') }}
        -- Considerar "activo" si fue visto en los últimos N días
        AND last_seen_at >= NOW() - INTERVAL '{{ var("active_window_days") }} days'

),

-- Métricas por combinación marca/modelo/año
metricas AS (

    SELECT
        brand,
        model,
        year,

        -- Volumen
        COUNT(*)                                        AS total_listings,
        COUNT(DISTINCT source_site)                     AS sites_count,

        -- Métricas de precio
        ROUND(AVG(price_usd), 0)                        AS avg_price,
        ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP
              (ORDER BY price_usd)::NUMERIC, 0)         AS median_price,
        MIN(price_usd)                                  AS min_price,
        MAX(price_usd)                                  AS max_price,
        ROUND(STDDEV(price_usd), 0)                     AS price_std_dev,

        -- Coeficiente de variación (qué tan dispersos están los precios)
        ROUND(
            CASE WHEN AVG(price_usd) > 0
                 THEN (STDDEV(price_usd) / AVG(price_usd)) * 100
                 ELSE NULL
            END, 1
        )                                               AS price_cv_pct,

        -- Tiempo en mercado
        ROUND(AVG(days_on_market), 1)                   AS avg_days_on_market,
        ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP
              (ORDER BY days_on_market)::NUMERIC, 1)    AS median_days_market,

        -- Distribución por sitio (JSONB)
        jsonb_object_agg(
            source_site,
            site_count
        )                                               AS listings_by_site,

        -- Rango de datos
        MIN(first_seen_at)                              AS data_from,
        MAX(last_seen_at)                               AS data_to,
        NOW()                                           AS computed_at

    FROM (
        SELECT
            *,
            COUNT(*) OVER (PARTITION BY brand, model, year, source_site) AS site_count
        FROM activos
    ) t

    WHERE brand IS NOT NULL AND model IS NOT NULL AND year IS NOT NULL
    GROUP BY brand, model, year
    HAVING COUNT(*) >= 2   -- Mínimo 2 anuncios para calcular métricas confiables

)

SELECT * FROM metricas
ORDER BY brand, model, year DESC
