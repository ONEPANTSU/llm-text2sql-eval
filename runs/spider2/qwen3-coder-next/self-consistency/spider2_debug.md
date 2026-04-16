# spider2 debug

## Top mismatches
- question: Can you calculate the 5-day symmetric moving average of predicted toy sales for December 5 to 8, 2018, using daily sales data from January 1, 2017, to August 29, 2018, with a simple linear regression model? Finally provide the sum of those four 5-day moving averages?
  gold_sql: ``
  pred_sql: `WITH daily_sales AS (
    SELECT 
        DATE(o.order_purchase_timestamp) AS sale_date,
        SUM(oi.price) AS daily_sales_amount
    FROM order_items oi
    JOIN orders o ON oi.order_id = o.order_id
    WHERE DATE(o.order_purchase_timestamp) BETWEEN '2017-01-01' AND '2018-08-29'
    GROUP BY DATE(o.order_purchase_timestamp)
),
toy_sales AS (
    SELECT 
        ds.sale_date,
        ds.daily_sales_amount AS predicted_sales
    FROM daily_sales ds
    -- Assuming toys are a subset; since no clear mapping, use all sales as proxy
    -- In real scenario would filter products where product_category_name LIKE '%toy%'
),
date_range AS (
    SELECT '2018-12-05' AS date UNION ALL
    SELECT '2018-12-06' UNION ALL
    SELECT '2018-12-07' UNION ALL
    SELECT '2018-12-08'
),
regression_params AS (
    SELECT 
        COUNT(*) AS n,
        SUM(DATE(sale_date)) AS sum_x,
        SUM(predicted_sales) AS sum_y,
        SUM(DATE(sale_date) * predicted_sales) AS sum_xy,
        SUM(DATE(sale_date) * DATE(sale_date)) AS sum_xx
    FROM toy_sales
),
model AS (
    SELECT 
        (n * sum_xy - sum_x * sum_y) / (n * sum_xx - sum_x * sum_x) AS slope,
        (sum_y - (sum_x * (n * sum_xy - sum_x * sum_y) / (n * sum_xx - sum_x * sum_x))) / n AS intercept
    FROM regression_params
),
predicted_daily AS (
    SELECT 
        dr.date,
        m.intercept + m.slope * DATE(dr.date) AS predicted_sales
    FROM date_range dr, model m
),
moving_avg AS (
    SELECT 
        date,
        AVG(predicted_sales) OVER (
            ORDER BY date 
            ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING
        ) AS five_day_ma
    FROM predicted_daily
)
SELECT 
    SUM(five_day_ma) AS total_moving_averages
FROM moving_avg
WHERE date BETWEEN '2018-12-05' AND '2018-12-08';`
  reason: row_count_mismatch
  gold_rows: []
  pred_rows: [[None]]
- question: 
  gold_sql: `WITH CustomerData AS (
    SELECT
        customer_unique_id,
        COUNT(DISTINCT orders.order_id) AS order_count,
        SUM(payment_value) AS total_payment,
        JULIANDAY(MIN(order_purchase_timestamp)) AS first_order_day,
        JULIANDAY(MAX(order_purchase_timestamp)) AS last_order_day
    FROM customers
        JOIN orders USING (customer_id)
        JOIN order_payments USING (order_id)
    GROUP BY customer_unique_id
)
SELECT
    customer_unique_id,
    order_count AS PF,
    ROUND(total_payment / order_count, 2) AS AOV,
    CASE
        WHEN (last_order_day - first_order_day) < 7 THEN
            1
        ELSE
            (last_order_day - first_order_day) / 7
        END AS ACL
FROM CustomerData
ORDER BY AOV DESC
LIMIT 3`
  pred_sql: ``
  reason: task_timeout
- question: Could you help me calculate the average single career span value in years for all baseball players? Please precise the result as a float number. First, calculate the difference in years, months, and days between the debut and final game dates. For each player, the career span is computed as the sum of the absolute number of years, plus the absolute number of months divided by 12, plus the absolute number of days divided by 365. Round each part to two decimal places before summing. Finally, average the career spans and round the result to a float number.
  gold_sql: ``
  pred_sql: `SELECT 
    ROUND(
        AVG(
            ROUND(
                ABS(
                    CAST(
                        (julianday(final_game) - julianday(debut)) / 365.0 
                        AS INTEGER
                    )
                ) + 
                ROUND(
                    ABS(
                        (
                            (julianday(final_game) - julianday(debut)) % 365.0
                        ) / 30.4167
                    ) / 12.0
                , 2) + 
                ROUND(
                    ABS(
                        (julianday(final_game) - julianday(debut)) % 30.4167
                    ) / 365.0
                , 2)
            , 2)
        ) 
    , 2) AS avg_career_span
FROM player
WHERE debut IS NOT NULL AND final_game IS NOT NULL;`
  reason: row_count_mismatch
  gold_rows: []
  pred_rows: [[None]]
- question: What is the distance of the longest route where Abakan is either the departure or destination city (in kilometers)?
  gold_sql: ``
  pred_sql: `SELECT MAX(range)
FROM aircrafts_data a
JOIN flights f ON a.aircraft_code = f.aircraft_code
WHERE f.departure_airport IN (
    SELECT airport_code
    FROM airports_data
    WHERE city = 'Abakan'
)
OR f.arrival_airport IN (
    SELECT airport_code
    FROM airports_data
    WHERE city = 'Abakan'
);`
  reason: row_count_mismatch
  gold_rows: []
  pred_rows: [[None]]
- question: Distribute all the unique city pairs into the distance ranges 0, 1000, 2000, 3000, 4000, 5000, and 6000+, based on their average distance of all routes between them. Then how many pairs are there in the distance range with the fewest unique city paires?
  gold_sql: ``
  pred_sql: `WITH city_pairs AS (
    SELECT DISTINCT
        f.departure_airport,
        f.arrival_airport,
        1 AS pair_id
    FROM flights f
),
distances AS (
    SELECT 
        departure_airport,
        arrival_airport,
        COUNT(*) AS route_count
    FROM flights
    GROUP BY departure_airport, arrival_airport
),
pair_distances AS (
    SELECT 
        departure_airport,
        arrival_airport,
        AVG(route_count) AS avg_distance
    FROM distances
    GROUP BY departure_airport, arrival_airport
),
binned AS (
    SELECT 
        departure_airport,
        arrival_airport,
        CASE 
            WHEN avg_distance < 1000 THEN '0-999'
            WHEN avg_distance < 2000 THEN '1000-1999'
            WHEN avg_distance < 3000 THEN '2000-2999'
            WHEN avg_distance < 4000 THEN '3000-3999'
            WHEN avg_distance < 5000 THEN '4000-4999'
            WHEN avg_distance < 6000 THEN '5000-5999'
            ELSE '6000+'
        END AS distance_range
    FROM pair_distances
),
range_counts AS (
    SELECT 
        distance_range,
        COUNT(*) AS num_pairs
    FROM binned
    GROUP BY distance_range
)
SELECT MIN(num_pairs) 
FROM range_counts;`
  reason: row_count_mismatch
  gold_rows: []
  pred_rows: [[None]]

## Error types
- row_count_mismatch: 19
- pred_exec_fail: 10
- task_timeout: 9
- gold_exec_fail: 5