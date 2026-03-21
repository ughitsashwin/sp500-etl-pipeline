-- ============================================================
-- stg_income_statements.sql
-- Purpose: Standardise the raw Alpha Vantage income statements
-- Source:  raw_income_statements (loaded by load_to_postgres.py)
-- Casts all financial values from text to numeric —
-- Alpha Vantage returns everything as strings in their API
-- ============================================================

SELECT
    -- Company and time identifiers
    ticker,
    "fiscalDateEnding"              AS fiscal_date,

    -- Extract the year for easier grouping and joining
    EXTRACT(YEAR FROM CAST("fiscalDateEnding" AS DATE))::INT
                                    AS fiscal_year,

    -- Income statement line items
    -- NULLIF handles "None" strings returned by the API
    -- CAST converts the string to a proper number
    CAST(NULLIF("totalRevenue", 'None') AS BIGINT)
                                    AS total_revenue,
    CAST(NULLIF("grossProfit", 'None') AS BIGINT)
                                    AS gross_profit,
    CAST(NULLIF("operatingIncome", 'None') AS BIGINT)
                                    AS operating_income,
    CAST(NULLIF("netIncome", 'None') AS BIGINT)
                                    AS net_income,
    CAST(NULLIF("ebitda", 'None') AS BIGINT)
                                    AS ebitda,
    CAST(NULLIF("researchAndDevelopment", 'None') AS BIGINT)
                                    AS rd_expense,
    CAST(NULLIF("operatingExpenses", 'None') AS BIGINT)
                                    AS operating_expenses,

    -- Currency of the reported figures
    "reportedCurrency"              AS currency

FROM {{ source('public', 'raw_income_statements') }}

-- Only keep rows with a valid ticker and date
WHERE ticker IS NOT NULL
  AND "fiscalDateEnding" IS NOT NULL
