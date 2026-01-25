SELECT DISTINCT
    symbol,
    last_price,
    implied_volatility_index_rank,
    implied_volatility_percentile,
    iv_hv_30_day_difference,
    liquidity_rating,
    beta,
    CASE
        WHEN earnings_expected_report_date BETWEEN
             created_date AND created_date + INTERVAL '25 days'
        THEN 'AVOID'
        ELSE 'CLEAR'
    END as earnings_status
FROM equity_data
WHERE liquidity_rating >= 3.0
  AND implied_volatility_index_rank > .50
  AND implied_volatility_percentile > .75
  AND last_price >= 10
  --AND ABS(beta) < 2.0
ORDER BY implied_volatility_index_rank DESC, liquidity_rating DESC;