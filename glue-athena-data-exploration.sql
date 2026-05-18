1. Period Performance — Total & Daily Returns
Cumulative return over the full period (best first)


    SELECT
        symbol,
        MIN(trade_date)                                          AS start_date,
        MAX(trade_date)                                          AS end_date,
        ROUND(MIN_BY(close, trade_date), 2)                      AS start_price,
        ROUND(MAX_BY(close, trade_date), 2)                      AS end_price,
        ROUND((MAX_BY(close, trade_date) - MIN_BY(close, trade_date))
            / MIN_BY(close, trade_date) * 100, 2)              AS pct_return
    FROM stocks_fact
    GROUP BY symbol
    ORDER BY pct_return DESC;

    Daily returns per stock


    SELECT
        symbol,
        trade_date,
        close,
        LAG(close) OVER (PARTITION BY symbol ORDER BY trade_date)          AS prev_close,
        ROUND((close - LAG(close) OVER (PARTITION BY symbol ORDER BY trade_date))
            / LAG(close) OVER (PARTITION BY symbol ORDER BY trade_date) * 100, 2) AS daily_return_pct
    FROM stocks_fact
    ORDER BY symbol, trade_date;


2. Volatility — How Risky Is Each Stock?
Average daily price range (intraday volatility)


    SELECT
        symbol,
        ROUND(AVG(high - low), 2)              AS avg_daily_range,
        ROUND(AVG((high - low) / close * 100), 2) AS avg_daily_range_pct,
        ROUND(MAX(high - low), 2)              AS max_single_day_swing
    FROM stocks_fact
    GROUP BY symbol
    ORDER BY avg_daily_range_pct DESC;

    Standard deviation of daily returns (classic volatility measure)


    WITH daily_returns AS (
        SELECT
            symbol,
            trade_date,
            (close - LAG(close) OVER (PARTITION BY symbol ORDER BY trade_date))
            / LAG(close) OVER (PARTITION BY symbol ORDER BY trade_date) AS ret
        FROM stocks_fact
    )
    SELECT
        symbol,
        ROUND(STDDEV(ret) * 100, 4) AS daily_volatility_pct,
        ROUND(STDDEV(ret) * SQRT(252) * 100, 2) AS annualized_volatility_pct
    FROM daily_returns
    WHERE ret IS NOT NULL
    GROUP BY symbol
    ORDER BY annualized_volatility_pct DESC;


3. Moving Averages — Trend Direction
5-day and 20-day simple moving averages


    SELECT
        symbol,
        trade_date,
        close,
        ROUND(AVG(close) OVER (PARTITION BY symbol ORDER BY trade_date
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW), 2)  AS sma_5,
        ROUND(AVG(close) OVER (PARTITION BY symbol ORDER BY trade_date
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW), 2) AS sma_20
    FROM stocks_fact
    ORDER BY symbol, trade_date;


    Golden/Death cross signal — SMA5 vs SMA20 crossover


    WITH mas AS (
        SELECT
            symbol, trade_date, close,
            AVG(close) OVER (PARTITION BY symbol ORDER BY trade_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW)  AS sma_5,
            AVG(close) OVER (PARTITION BY symbol ORDER BY trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS sma_20
        FROM stocks_fact
    )
    SELECT
        symbol, trade_date, ROUND(sma_5,2) AS sma_5, ROUND(sma_20,2) AS sma_20,
        CASE WHEN sma_5 > sma_20 THEN 'Bullish (SMA5 > SMA20)'
            ELSE 'Bearish (SMA5 < SMA20)' END AS trend_signal
    FROM mas
    WHERE sma_20 IS NOT NULL
    ORDER BY symbol, trade_date;



4. Volume Analysis — Market Conviction
Average volume and days with abnormal volume spikes


    WITH avg_vol AS (
        SELECT symbol, AVG(volume) AS avg_volume
        FROM stocks_fact
        GROUP BY symbol
    )
    SELECT
        f.symbol,
        f.trade_date,
        f.volume,
        ROUND(a.avg_volume, 0)                      AS avg_volume,
        ROUND(f.volume / a.avg_volume * 100, 1)     AS pct_of_avg_volume
    FROM stocks_fact f
    JOIN avg_vol a on a.symbol = f.symbol
    WHERE f.volume > a.avg_volume * 1.5             -- 50% above average
    ORDER BY pct_of_avg_volume DESC;

5. Highs & Lows — Price Extremes
Period high, low, and where current price sits in that range


    SELECT
        symbol,
        ROUND(MAX(high), 2)                                       AS period_high,
        ROUND(MIN(low), 2)                                        AS period_low,
        ROUND(MAX_BY(close, trade_date), 2)                       AS latest_close,
        ROUND((MAX_BY(close, trade_date) - MIN(low))
            / (MAX(high) - MIN(low)) * 100, 1)                  AS position_in_range_pct
    FROM stocks_fact
    GROUP BY symbol
    ORDER BY symbol;



6. Best & Worst Days

    WITH daily_returns AS (
        SELECT
            symbol, trade_date, close,
            ROUND((close - LAG(close) OVER (PARTITION BY symbol ORDER BY trade_date))
                / LAG(close) OVER (PARTITION BY symbol ORDER BY trade_date) * 100, 2) AS daily_return_pct
        FROM stocks_fact
    )
    SELECT * FROM daily_returns
    WHERE daily_return_pct IS NOT NULL
    ORDER BY daily_return_pct DESC
    LIMIT 10;  -- change to ASC for worst days



7. Cross-Stock Comparison Summary

    SELECT
        f.symbol,
        d.exchange,
        d.currency,
        ROUND(MIN_BY(f.close, f.trade_date), 2)                              AS start_price,
        ROUND(MAX_BY(f.close, f.trade_date), 2)                              AS end_price,
        ROUND((MAX_BY(f.close, f.trade_date) - MIN_BY(f.close, f.trade_date))
            / MIN_BY(f.close, f.trade_date) * 100, 2)                      AS total_return_pct,
        ROUND(AVG(f.volume), 0)                                              AS avg_daily_volume,
        ROUND(AVG(f.high - f.low), 2)                                        AS avg_daily_range
    FROM stocks_fact f
    JOIN stocks_dim d on d.symbol = f.symbol
    GROUP BY f.symbol, d.exchange, d.currency
    ORDER BY total_return_pct DESC;
