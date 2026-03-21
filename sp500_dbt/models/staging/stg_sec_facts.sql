-- ============================================================
-- stg_sec_facts.sql
-- Purpose: Standardise the raw SEC EDGAR facts table
-- Source:  raw_sec_facts (loaded by load_to_postgres.py)
-- This pivots the long format (one row per metric)
-- into a wide format (one row per company per year)
-- which is much easier to join and analyse
-- ============================================================

-- First CTE: clean up the raw facts
WITH cleaned AS (
    SELECT
        ticker,
        company_name,
        metric_name,
        -- Extract year from fiscal_year date string
        LEFT(fiscal_year, 4)::INT   AS fiscal_year,
        value,
        currency
    FROM {{ source('public', 'raw_sec_facts') }}
    WHERE value IS NOT NULL
      AND ticker IS NOT NULL
),

-- Second CTE: pivot from long to wide format
-- Each metric becomes its own column
pivoted AS (
    SELECT
        ticker,
        company_name,
        fiscal_year,

        -- Each MAX() picks the value for that metric in that year
        -- We use MAX() because after filtering there's only one
        -- value per ticker/year/metric combination
        MAX(CASE WHEN metric_name = 'NetIncomeLoss'
            THEN value END)         AS net_income,
        MAX(CASE WHEN metric_name = 'Revenues'
            THEN value END)         AS revenue,
        MAX(CASE WHEN metric_name = 'Assets'
            THEN value END)         AS total_assets,
        MAX(CASE WHEN metric_name = 'Liabilities'
            THEN value END)         AS total_liabilities,
        MAX(CASE WHEN metric_name = 'StockholdersEquity'
            THEN value END)         AS stockholders_equity,
        MAX(CASE WHEN metric_name = 'OperatingIncomeLoss'
            THEN value END)         AS operating_income

    FROM cleaned
    GROUP BY ticker, company_name, fiscal_year
)

SELECT * FROM pivoted
ORDER BY ticker, fiscal_year DESC
