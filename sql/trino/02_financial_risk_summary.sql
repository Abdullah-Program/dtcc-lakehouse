-- Financial Risk Analytics Query on Gold Active Trades
-- 1. Aggregates active notional exposure by asset class in Gold
SELECT
    coalesce(asset_class, 'UNKNOWN') AS asset_class,
    count(*) AS active_trade_count,
    round(sum(notional_amount_leg_1) / 1e9, 2) AS total_notional_billions,
    round(avg(notional_amount_leg_1) / 1e6, 2) AS avg_notional_millions,
    round(min(notional_amount_leg_1) / 1e6, 2) AS min_notional_millions,
    round(max(notional_amount_leg_1) / 1e6, 2) AS max_notional_millions
FROM polaris.dtcc.gold_active_trades
WHERE lifecycle_status = 'ACTIVE'
GROUP BY asset_class
ORDER BY total_notional_billions DESC;

-- 2. Trade Lifecycle Distribution in Gold
SELECT
    lifecycle_status,
    count(*) AS trade_count,
    round(100.0 * count(*) / sum(count(*)) OVER (), 2) AS percentage_of_portfolio
FROM polaris.dtcc.gold_active_trades
GROUP BY lifecycle_status
ORDER BY trade_count DESC;

-- 3. Currency Risk Exposure from Silver (117-column dataset)
SELECT
    coalesce(notional_currency_leg_1, 'UNKNOWN') AS currency,
    count(*) AS total_events,
    round(sum(notional_amount_leg_1) / 1e9, 2) AS total_notional_billions
FROM polaris.dtcc.silver_rates
GROUP BY notional_currency_leg_1
ORDER BY total_notional_billions DESC
LIMIT 10;
