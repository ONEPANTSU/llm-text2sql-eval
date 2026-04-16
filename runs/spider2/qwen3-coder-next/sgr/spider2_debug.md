# spider2 debug

## Top mismatches
- question: Can you calculate the 5-day symmetric moving average of predicted toy sales for December 5 to 8, 2018, using daily sales data from January 1, 2017, to August 29, 2018, with a simple linear regression model? Finally provide the sum of those four 5-day moving averages?
  gold_sql: ``
  pred_sql: `WITH daily_sales AS (
    SELECT 
        DATE(o.order_purchase_timestamp) AS sale_date,
        SUM(oi.price) AS daily_sales_value
    FROM order_items oi
    JOIN orders o ON oi.order_id = o.order_id
    JOIN products p ON oi.product_id = p.product_id
    WHERE p.product_category_name = 'toys'
      AND DATE(o.order_purchase_timestamp) BETWEEN '2017-01-01' AND '2018-08-29'
    GROUP BY DATE(o.order_purchase_timestamp)
),
regression_params AS (
    SELECT 
        AVG(sale_date) AS mean_date,
        AVG(daily_sales_value) AS mean_sales,
        SUM((strftime('%J', sale_date) - (SELECT AVG(strftime('%J', sale_date)) FROM daily_sales)) * (daily_sales_value - (SELECT AVG(daily_sales_value) FROM daily_sales))) /
        NULLIF(SUM(POWER((strftime('%J', sale_date) - (SELECT AVG(strftime('%J', sale_date)) FROM daily_sales)), 2)), 0) AS slope,
        (SELECT AVG(daily_sales_value) FROM daily_sales) -
        ((SELECT AVG(strftime('%J', sale_date)) FROM daily_sales) *
         (SUM((strftime('%J', sale_date) - (SELECT AVG(strftime('%J', sale_date)) FROM daily_sales)) * (daily_sales_value - (SELECT AVG(daily_sales_value) FROM daily_sales))) /
          NULLIF(SUM(POWER((strftime('%J', sale_date) - (SELECT AVG(strftime('%J', sale_date)) FROM daily_sales)), 2)), 0))) AS intercept
    FROM daily_sales
),
predicted_sales AS (
    SELECT 
        date_series.sale_date,
        (SELECT slope FROM regression_params) * strftime('%J', date_series.sale_date) + 
        (SELECT intercept FROM regression_params) AS predicted_sales
    FROM (
        SELECT '2018-12-05' AS sale_date UNION ALL
        SELECT '2018-12-06' UNION ALL
        SELECT '2018-12-07' UNION ALL
        SELECT '2018-12-08'
    ) AS date_series
),
moving_averages AS (
    SELECT 
        sale_date,
        AVG(predicted_sales) OVER (
            ORDER BY sale_date
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) AS symmetric_ma_5
    FROM predicted_sales
)
SELECT SUM(symmetric_ma_5) AS total_ma_sum
FROM moving_averages
WHERE sale_date BETWEEN '2018-12-05' AND '2018-12-08';`
  reason: row_count_mismatch
  gold_rows: []
  pred_rows: [[None]]
- question: Could you help me calculate the average single career span value in years for all baseball players? Please precise the result as a float number. First, calculate the difference in years, months, and days between the debut and final game dates. For each player, the career span is computed as the sum of the absolute number of years, plus the absolute number of months divided by 12, plus the absolute number of days divided by 365. Round each part to two decimal places before summing. Finally, average the career spans and round the result to a float number.
  gold_sql: ``
  pred_sql: `SELECT ROUND(AVG(CAST(ABS(julianday(final_game) - julianday(debut)) / 365.0 AS REAL)), 2) AS avg_career_span FROM player WHERE debut IS NOT NULL AND final_game IS NOT NULL`
  reason: row_count_mismatch
  gold_rows: []
  pred_rows: [[None]]
- question: What is the distance of the longest route where Abakan is either the departure or destination city (in kilometers)?
  gold_sql: ``
  pred_sql: `SELECT MAX(aircrafts_data.range) FROM flights JOIN airports_data AS dep_airport ON flights.departure_airport = dep_airport.airport_code JOIN airports_data AS arr_airport ON flights.arrival_airport = arr_airport.airport_code JOIN aircrafts_data ON flights.aircraft_code = aircrafts_data.aircraft_code WHERE dep_airport.city = 'Abakan' OR arr_airport.city = 'Abakan'`
  reason: row_count_mismatch
  gold_rows: []
  pred_rows: [[None]]
- question: Distribute all the unique city pairs into the distance ranges 0, 1000, 2000, 3000, 4000, 5000, and 6000+, based on their average distance of all routes between them. Then how many pairs are there in the distance range with the fewest unique city paires?
  gold_sql: ``
  pred_sql: `WITH city_pairs AS (
    SELECT 
        LEAST(a1.city, a2.city) AS city1,
        GREATEST(a1.city, a2.city) AS city2
    FROM flights f
    JOIN airports_data a1 ON f.departure_airport = a1.airport_code
    JOIN airports_data a2 ON f.arrival_airport = a2.airport_code
    WHERE a1.city != a2.city
),
pair_distances AS (
    SELECT 
        city1,
        city2,
        COUNT(*) AS num_routes
    FROM city_pairs
    GROUP BY city1, city2
),
distance_ranges AS (
    SELECT 
        CASE 
            WHEN num_routes < 1000 THEN '0-999'
            WHEN num_routes < 2000 THEN '1000-1999'
            WHEN num_routes < 3000 THEN '2000-2999'
            WHEN num_routes < 4000 THEN '3000-3999'
            WHEN num_routes < 5000 THEN '4000-4999'
            WHEN num_routes < 6000 THEN '5000-5999'
            ELSE '6000+'
        END AS distance_range,
        COUNT(*) AS num_pairs
    FROM pair_distances
    GROUP BY distance_range
)
SELECT MIN(num_pairs)
FROM distance_ranges;`
  reason: pred_exec_fail
  gold_rows: []
- question: For the NXT title that had the shortest match (excluding titles with "title change"), what were the names of the two wrestlers involved?
  gold_sql: `WITH MatchDetails AS (
    SELECT
        b.name AS titles,
        m.duration AS match_duration,
        w1.name || ' vs ' || w2.name AS matches,
        m.win_type AS win_type,
        l.name AS location,
        e.name AS event,
        ROW_NUMBER() OVER (PARTITION BY b.name ORDER BY m.duration ASC) AS rank
    FROM 
        Belts b
    INNER JOIN Matches m ON m.title_id = b.id
    INNER JOIN Wrestlers w1 ON w1.id = m.winner_id
    INNER JOIN Wrestlers w2 ON w2.id = m.loser_id
    INNER JOIN Cards c ON c.id = m.card_id
    INNER JOIN Locations l ON l.id = c.location_id
    INNER JOIN Events e ON e.id = c.event_id
    INNER JOIN Promotions p ON p.id = c.promotion_id
    WHERE
        p.name = 'NXT'
        AND m.duration <> ''
        AND b.name <> ''
        AND b.name NOT IN (
            SELECT name 
            FROM Belts 
            WHERE name LIKE '%title change%'
        )
),
Rank1 AS (
SELECT 
    titles,
    match_duration,
    matches,
    win_type,
    location,
    event
FROM 
    MatchDetails
WHERE 
    rank = 1
)
SELECT
    SUBSTR(matches, 1, INSTR(matches, ' vs ') - 1) AS wrestler1,
    SUBSTR(matches, INSTR(matches, ' vs ') + 4) AS wrestler2
FROM
Rank1
ORDER BY match_duration 
LIMIT 1`
  pred_sql: `SELECT name FROM Promotions ORDER BY id LIMIT 1`
  reason: gold_exec_fail
  pred_rows: []

## Error types
- row_count_mismatch: 19
- pred_exec_fail: 13
- gold_exec_fail: 5
- task_timeout: 2
- pred_generation_fail: 1