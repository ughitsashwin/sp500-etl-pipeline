-- ============================================================
-- stg_income_statements.sql
-- Updated to work with SEC-derived income statement data
-- ============================================================

SELECT
    ticker,
    "fiscalDateEnding"                          AS fiscal_date,
    LEFT("fiscalDateEnding", 4)::INT            AS fiscal_year,
    CAST(NULLIF("totalRevenue", 'None') AS BIGINT)
                                                AS total_revenue,
    CAST(NULLIF("netIncome", 'None') AS BIGINT)
                                                AS net_income,
    CAST(NULLIF("operatingIncome", 'None') AS BIGINT)
                                                AS operating_income,
    CAST(NULLIF("stockholdersEquity", 'None') AS BIGINT)
                                                AS stockholders_equity,
    NULL::BIGINT                                AS gross_profit,
    NULL::BIGINT                                AS ebitda,
    NULL::BIGINT                                AS rd_expense,
    COALESCE("reportedCurrency", 'USD')         AS currency

FROM {{ source('public', 'raw_income_statements') }}

WHERE ticker IS NOT NULL
  AND "fiscalDateEnding" IS NOT NULL
