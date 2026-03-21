-- ============================================================
-- mart_financials.sql
-- Purpose: Final wide analytical table combining all sources
--          and calculating key financial ratios
-- This is the table that analysts, dashboards, and reports
-- will query directly. It's the end product of our pipeline.
-- ============================================================

WITH

-- Pull in the Kaggle company data (505 companies, market data)
kaggle AS (
    SELECT * FROM {{ ref('stg_financials') }}
),

-- Pull in the Alpha Vantage income statements (5 tickers, multi-year)
income AS (
    SELECT * FROM {{ ref('stg_income_statements') }}
),

-- Pull in the SEC EDGAR facts (5 tickers, multi-year, pivoted)
sec AS (
    SELECT * FROM {{ ref('stg_sec_facts') }}
),

-- Join income statements with SEC facts on ticker and year
combined_financials AS (
    SELECT
        i.ticker,
        i.fiscal_year,
        i.total_revenue,
        i.gross_profit,
        i.net_income                            AS api_net_income,
        i.ebitda                                AS api_ebitda,
        i.operating_income                      AS api_operating_income,
        i.rd_expense,
        s.net_income                            AS sec_net_income,
        s.total_assets,
        s.total_liabilities,
        s.stockholders_equity,
        s.revenue                               AS sec_revenue
    FROM income i
    LEFT JOIN sec s
           ON i.ticker      = s.ticker
          AND i.fiscal_year = s.fiscal_year
),

-- Calculate financial ratios
ratios AS (
    SELECT
        ticker,
        fiscal_year,
        total_revenue,
        gross_profit,
        api_net_income                          AS net_income,
        api_ebitda                              AS ebitda,
        total_assets,
        total_liabilities,
        stockholders_equity,

        -- Gross Margin: % of revenue left after cost of goods
        ROUND(gross_profit::NUMERIC
            / NULLIF(total_revenue, 0) * 100, 2) AS gross_margin_pct,

        -- Net Profit Margin: % of revenue that becomes profit
        ROUND(api_net_income::NUMERIC
            / NULLIF(total_revenue, 0) * 100, 2) AS net_profit_margin_pct,

        -- Return on Assets: how efficiently assets generate profit
        ROUND(api_net_income::NUMERIC
            / NULLIF(total_assets, 0) * 100, 2)  AS return_on_assets_pct,

        -- Return on Equity: profit per dollar of shareholder equity
        ROUND(api_net_income::NUMERIC
            / NULLIF(stockholders_equity, 0) * 100, 2) AS return_on_equity_pct,

        -- Debt Ratio: proportion of assets financed by debt
        ROUND(total_liabilities::NUMERIC
            / NULLIF(total_assets, 0) * 100, 2)  AS debt_ratio_pct

    FROM combined_financials
    WHERE total_revenue IS NOT NULL
      AND total_revenue > 0
),

-- Join ratios with Kaggle market data to add sector and market cap
final AS (
    SELECT
        r.ticker,
        k.company_name,
        k.sector,
        r.fiscal_year,

        -- Market data from Kaggle
        k.stock_price,
        k.market_cap,
        k.pe_ratio,
        k.ps_ratio,
        k.pb_ratio,
        k.eps,
        k.week_52_low,
        k.week_52_high,

        -- Income statement figures
        r.total_revenue,
        r.gross_profit,
        r.net_income,
        r.ebitda,
        r.total_assets,
        r.total_liabilities,
        r.stockholders_equity,

        -- Calculated financial ratios
        r.gross_margin_pct,
        r.net_profit_margin_pct,
        r.return_on_assets_pct,
        r.return_on_equity_pct,
        r.debt_ratio_pct

    FROM ratios r
    LEFT JOIN kaggle k ON r.ticker = k.ticker
)

SELECT * FROM final
ORDER BY ticker, fiscal_year DESC
