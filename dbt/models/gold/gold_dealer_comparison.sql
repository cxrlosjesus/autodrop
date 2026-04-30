{{
  config(
    materialized = 'table',
    tags = ['gold', 'daily']
  )
}}

/*
  AutoPulse — Modelo Gold: Comparativa por Dealer/Sitio

  Responde la pregunta central del producto:
  "¿Cómo está posicionado cada dealer vs el promedio del mercado?"

  Si un dealer tiene sus Kia Sportage 2022 a $18,500 y el mercado
  promedia $16,800, este modelo muestra que está 10.1% sobre el mercado.
*/

WITH activos AS (

    SELECT *
    FROM {{ ref('silver_listings') }}
    WHERE
        is_active = TRUE
        AND price_usd IS NOT NULL
        AND last_seen_at >= NOW() - INTERVAL '{{ var("active_window_days") }} days'

),

mercado AS (

    SELECT *
    FROM {{ ref('gold_market_summary') }}

),

por_dealer AS (

    SELECT
        a.brand,
        a.model,
        a.year,
        a.source_site,

        COUNT(*)                                        AS listing_count,
        ROUND(AVG(a.price_usd), 0)                     AS avg_price,
        MIN(a.price_usd)                                AS min_price,
        MAX(a.price_usd)                                AS max_price,
        ROUND(AVG(a.days_on_market), 1)                AS avg_days_market,

        NOW()                                           AS computed_at

    FROM activos a
    WHERE a.brand IS NOT NULL AND a.model IS NOT NULL AND a.year IS NOT NULL
    GROUP BY a.brand, a.model, a.year, a.source_site

)

SELECT
    d.brand,
    d.model,
    d.year,
    d.source_site,
    d.listing_count,
    d.avg_price,
    d.min_price,
    d.max_price,
    d.avg_days_market,
    m.avg_price                                         AS market_avg_price,
    m.total_listings                                    AS market_total_listings,

    -- Posición relativa vs mercado
    ROUND(d.avg_price - m.avg_price, 0)                AS vs_market_abs,
    ROUND(
        CASE WHEN m.avg_price > 0
             THEN ((d.avg_price - m.avg_price) / m.avg_price) * 100
             ELSE NULL
        END, 1
    )                                                   AS vs_market_pct,

    -- Señal de pricing: si está caro o barato vs el mercado
    CASE
        WHEN d.avg_price > m.avg_price * 1.10 THEN 'SOBRE_MERCADO'
        WHEN d.avg_price < m.avg_price * 0.90 THEN 'BAJO_MERCADO'
        ELSE 'EN_MERCADO'
    END                                                 AS pricing_signal,

    d.computed_at

FROM por_dealer d
INNER JOIN mercado m
    ON d.brand = m.brand
    AND d.model = m.model
    AND d.year = m.year

ORDER BY d.brand, d.model, d.year,
    ABS(ROUND(
        CASE WHEN m.avg_price > 0
             THEN ((d.avg_price - m.avg_price) / m.avg_price) * 100
             ELSE NULL
        END, 1
    )) DESC NULLS LAST
