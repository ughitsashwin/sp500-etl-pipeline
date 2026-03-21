-- ============================================================
-- stg_financials.sql
-- Purpose: Standardise the raw Kaggle financials table
-- Source:  raw_financials (loaded by load_to_postgres.py)
-- This is a staging model — it only cleans and renames,
-- it never joins or aggregates. One source = one staging model.
-- ============================================================

SELECT
    -- Company identifiers
    symbol                          AS ticker,
    name                            AS company_name,
    sector,

    -- Stock price metrics
    price                           AS stock_price,
    "52_week_low"                   AS week_52_low,
    "52_week_high"                  AS week_52_high,

    -- Valuation ratios (raw from Kaggle)
    price_per_earnings              AS pe_ratio,
    price_per_sales                 AS ps_ratio,
    price_per_book                  AS pb_ratio,

    -- Financial fundamentals
    market_cap,
    ebitda,
    earnings_per_share              AS eps,
    dividend_yield,

    -- SEC filing URL for reference
    sec_filings                     AS sec_filings_url,

    -- Flag companies with complete data for ratio calculations
    CASE
        WHEN market_cap > 0
         AND ebitda IS NOT NULL
         AND price_per_earnings > 0
        THEN TRUE
        ELSE FALSE
    END                             AS has_complete_data

FROM {{ source('public', 'raw_financials') }}

-- Exclude rows with no ticker or company name
WHERE symbol IS NOT NULL
  AND name   IS NOT NULL
