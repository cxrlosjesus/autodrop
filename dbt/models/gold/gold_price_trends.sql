{{
  config(
    materialized = 'table',
    tags = ['gold', 'daily']
  )
}}

/*
  AutoPulse — Modelo Gold: Tendencias de Precio por Día

  Agrega el precio promedio por marca/modelo/año para cada fecha de scraping.
  Permite graficar la evolución del mercado a lo largo del tiempo.

  Casos de uso:
  - ¿Cómo ha variado el precio del Toyota Corolla 2020 en las últimas semanas?
  - ¿Qué marcas han bajado de precio este mes?
  - Dashboard de tendencias para clientes
*/

WITH daily AS (

    SELECT
        DATE(last_seen_at AT TIME ZONE 'America/Panama')    AS fecha,
        brand,
        model,
        year,
        source_site,

        COUNT(*)                                             AS listings_count,
        ROUND(AVG(price_usd), 0)                            AS avg_price,
        ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP
              (ORDER BY price_usd)::NUMERIC, 0)             AS median_price,
        MIN(price_usd)                                      AS min_price,
        MAX(price_usd)                                      AS max_price,
        ROUND(AVG(mileage_km), 0)                           AS avg_mileage_km

    FROM {{ ref('silver_listings') }}

    WHERE
        price_usd IS NOT NULL
        AND price_usd BETWEEN {{ var('price_min_usd') }} AND {{ var('price_max_usd') }}
        AND brand   IS NOT NULL
        AND model   IS NOT NULL
        AND year    IS NOT NULL
        AND year    BETWEEN {{ var('min_vehicle_year') }} AND EXTRACT(YEAR FROM NOW())::INT + 1

    GROUP BY 1, 2, 3, 4, 5

),

-- Solo incluir combinaciones con suficientes datos para que la métrica sea confiable
filtered AS (

    SELECT *
    FROM daily
    WHERE listings_count >= 2

)

SELECT *
FROM filtered
ORDER BY brand, model, year DESC, fecha DESC
