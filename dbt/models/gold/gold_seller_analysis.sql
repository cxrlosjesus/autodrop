{{
  config(
    materialized = 'table',
    tags = ['gold', 'daily']
  )
}}

/*
  AutoPulse — Modelo Gold: Análisis de Vendedores / Dealers

  Responde:
  - ¿Quiénes son los dealers más activos del mercado?
  - ¿Cómo está su precio promedio vs el mercado?
  - ¿Cuánto tardan en vender sus autos?
  - ¿Qué marcas manejan?

  Clasificación automática:
  - dealer:     >= 5 listings activos
  - individual: < 5 listings activos
*/

WITH activos AS (

    SELECT *
    FROM {{ ref('silver_listings') }}
    WHERE
        is_active     = TRUE
        AND seller_name IS NOT NULL
        AND price_usd IS NOT NULL
        AND last_seen_at >= NOW() - INTERVAL '{{ var("active_window_days") }} days'

),

mercado AS (

    SELECT brand, model, year, avg_price AS market_avg_price
    FROM {{ ref('gold_market_summary') }}

),

por_vendedor AS (

    SELECT
        a.seller_name,
        a.source_site,

        COUNT(*)                                            AS total_listings,
        COUNT(DISTINCT a.brand)                             AS marcas_distintas,
        COUNT(DISTINCT a.model)                             AS modelos_distintos,

        -- Precios
        ROUND(AVG(a.price_usd), 0)                         AS avg_price,
        MIN(a.price_usd)                                    AS min_price,
        MAX(a.price_usd)                                    AS max_price,

        -- Tiempo en mercado
        ROUND(AVG(a.days_on_market), 1)                    AS avg_days_on_market,

        -- Top 3 marcas más frecuentes (para el reporte)
        (
            SELECT STRING_AGG(brand, ', ' ORDER BY cnt DESC)
            FROM (
                SELECT brand, COUNT(*) AS cnt
                FROM activos a2
                WHERE a2.seller_name = a.seller_name
                  AND a2.brand IS NOT NULL
                GROUP BY brand
                LIMIT 3
            ) top
        )                                                   AS top_marcas,

        -- Precio vs mercado: promedio de (precio_listing - market_avg) por listing
        ROUND(AVG(a.price_usd - COALESCE(m.market_avg_price, a.price_usd)), 0)
                                                            AS vs_market_abs,

        ROUND(
            AVG(
                CASE WHEN m.market_avg_price > 0
                     THEN ((a.price_usd - m.market_avg_price) / m.market_avg_price) * 100
                     ELSE NULL
                END
            ), 1
        )                                                   AS vs_market_pct,

        NOW()                                               AS computed_at

    FROM activos a
    LEFT JOIN mercado m
        ON a.brand = m.brand
        AND a.model = m.model
        AND a.year = m.year

    GROUP BY a.seller_name, a.source_site

),

final AS (

    SELECT
        seller_name,
        source_site,
        total_listings,
        marcas_distintas,
        modelos_distintos,
        avg_price,
        min_price,
        max_price,
        avg_days_on_market,
        top_marcas,
        vs_market_abs,
        vs_market_pct,

        -- Clasificación automática
        CASE
            WHEN total_listings >= 5 THEN 'dealer'
            ELSE 'individual'
        END                                                 AS seller_type,

        -- Señal de pricing
        CASE
            WHEN vs_market_pct > 10  THEN 'SOBRE_MERCADO'
            WHEN vs_market_pct < -10 THEN 'BAJO_MERCADO'
            ELSE 'EN_MERCADO'
        END                                                 AS pricing_signal,

        computed_at

    FROM por_vendedor

)

SELECT *
FROM final
ORDER BY total_listings DESC
