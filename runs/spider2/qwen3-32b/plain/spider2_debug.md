# spider2 debug

## Top mismatches
- question: Can you calculate the 5-day symmetric moving average of predicted toy sales for December 5 to 8, 2018, using daily sales data from January 1, 2017, to August 29, 2018, with a simple linear regression model? Finally provide the sum of those four 5-day moving averages?
  gold_sql: ``
  pred_sql: `SELECT 
    SUM(avg_sales) AS total_moving_average_sum
FROM (
    SELECT 
        AVG(predicted_sales) AS avg_sales
    FROM (
        SELECT 
            DATE(o.order_purchase_timestamp) AS sale_date,
            SUM(oi.price) AS actual_sales,
            CAST(STRFTIME('%Y', o.order_purchase_timestamp) AS INTEGER) * 365 + 
            CAST(STRFTIME('%J', o.order_purchase_timestamp) AS INTEGER) AS day_index,
            CAST(STRFTIME('%Y', o.order_purchase_timestamp) AS INTEGER) AS year,
            CAST(STRFTIME('%m', o.order_purchase_timestamp) AS INTEGER) AS month,
            CAST(STRFTIME('%d', o.order_purchase_timestamp) AS INTEGER) AS day
        FROM orders o
        JOIN order_items oi ON o.order_id = oi.order_id
        JOIN products p ON oi.product_id = p.product_id
        JOIN product_category_name_translation t ON p.product_category_name = t.product_category_name
        WHERE t.product_category_name_english = 'toys'
          AND o.order_purchase_timestamp >= '2017-01-01'
          AND o.order_purchase_timestamp <= '2018-08-29'
        GROUP BY DATE(o.order_purchase_timestamp)
    ) AS daily_sales
    CROSS JOIN (
        SELECT 
            CAST(STRFTIME('%Y', '2018-12-05') AS INTEGER) * 365 + 
            CAST(STRFTIME('%J', '2018-12-05') AS INTEGER) AS day_index
    ) AS target_start
    CROSS JOIN (
        SELECT 
            CAST(STRFTIME('%Y', '2018-12-08') AS INTEGER) * 365 + 
            CAST(STRFTIME('%J', '2018-12-08') AS INTEGER) AS day_index
    ) AS target_end
    WHERE day_index BETWEEN target_start.day_index - 2 AND target_end.day_index + 2
    GROUP BY sale_date
    ORDER BY sale_date
    LIMIT 4
) AS moving_averages;`
  reason: pred_exec_fail
  gold_rows: []
- question: According to the RFM definition document, calculate the average sales per order for each customer within distinct RFM segments, considering only 'delivered' orders. Use the customer unique identifier. Clearly define how to calculate Recency based on the latest purchase timestamp and specify the criteria for classifying RFM segments. The average sales should be computed as the total spend divided by the total number of orders. Please analyze and report the differences in average sales across the RFM segments
  gold_sql: `WITH RecencyScore AS (
    SELECT customer_unique_id,
           MAX(order_purchase_timestamp) AS last_purchase,
           NTILE(5) OVER (ORDER BY MAX(order_purchase_timestamp) DESC) AS recency
    FROM orders
        JOIN customers USING (customer_id)
    WHERE order_status = 'delivered'
    GROUP BY customer_unique_id
),
FrequencyScore AS (
    SELECT customer_unique_id,
           COUNT(order_id) AS total_orders,
           NTILE(5) OVER (ORDER BY COUNT(order_id) DESC) AS frequency
    FROM orders
        JOIN customers USING (customer_id)
    WHERE order_status = 'delivered'
    GROUP BY customer_unique_id
),
MonetaryScore AS (
    SELECT customer_unique_id,
           SUM(price) AS total_spent,
           NTILE(5) OVER (ORDER BY SUM(price) DESC) AS monetary
    FROM orders
        JOIN order_items USING (order_id)
        JOIN customers USING (customer_id)
    WHERE order_status = 'delivered'
    GROUP BY customer_unique_id
),

-- 2. Assign each customer to a group
RFM AS (
    SELECT last_purchase, total_orders, total_spent,
        CASE
            WHEN recency = 1 AND frequency + monetary IN (1, 2, 3, 4) THEN "Champions"
            WHEN recency IN (4, 5) AND frequency + monetary IN (1, 2) THEN "Can't Lose Them"
            WHEN recency IN (4, 5) AND frequency + monetary IN (3, 4, 5, 6) THEN "Hibernating"
            WHEN recency IN (4, 5) AND frequency + monetary IN (7, 8, 9, 10) THEN "Lost"
            WHEN recency IN (2, 3) AND frequency + monetary IN (1, 2, 3, 4) THEN "Loyal Customers"
            WHEN recency = 3 AND frequency + monetary IN (5, 6) THEN "Needs Attention"
            WHEN recency = 1 AND frequency + monetary IN (7, 8) THEN "Recent Users"
            WHEN recency = 1 AND frequency + monetary IN (5, 6) OR
                recency = 2 AND frequency + monetary IN (5, 6, 7, 8) THEN "Potentital Loyalists"
            WHEN recency = 1 AND frequency + monetary IN (9, 10) THEN "Price Sensitive"
            WHEN recency = 2 AND frequency + monetary IN (9, 10) THEN "Promising"
            WHEN recency = 3 AND frequency + monetary IN (7, 8, 9, 10) THEN "About to Sleep"
        END AS RFM_Bucket
    FROM RecencyScore
        JOIN FrequencyScore USING (customer_unique_id)
        JOIN MonetaryScore USING (customer_unique_id)
)

SELECT RFM_Bucket, 
       AVG(total_spent / total_orders) AS avg_sales_per_customer
FROM RFM
GROUP BY RFM_Bucket`
  pred_sql: `WITH customer_orders AS (
    SELECT 
        o.customer_id,
        o.order_id,
        o.order_purchase_timestamp,
        SUM(oi.price + oi.freight_value) AS total_order_value
    FROM orders o
    INNER JOIN order_items oi ON o.order_id = oi.order_id
    WHERE o.order_status = 'delivered'
    GROUP BY o.customer_id, o.order_id, o.order_purchase_timestamp
),
customer_rfm AS (
    SELECT 
        customer_id,
        COUNT(order_id) AS total_orders,
        SUM(total_order_value) AS total_spend,
        MAX(CAST(order_purchase_timestamp AS INTEGER)) AS last_purchase_timestamp,
        AVG(total_order_value) AS avg_sales_per_order
    FROM customer_orders
    GROUP BY customer_id
),
rfm_segments AS (
    SELECT 
        customer_id,
        total_orders,
        total_spend,
        last_purchase_timestamp,
        avg_sales_per_order,
        -- Recency: Days since last purchase (using current date as reference)
        CAST(julianday('now') - julianday(last_purchase_timestamp)) AS recency_days,
        -- Frequency: Total number of orders
        total_orders AS frequency,
        -- Monetary: Total spend
        total_spend AS monetary
    FROM customer_rfm
),
rfm_binned AS (
    SELECT 
        customer_id,
        recency_days,
        frequency,
        monetary,
        avg_sales_per_order,
        -- RFM Segmentation: Quartile-based binning
        CASE 
            WHEN recency_days <= 30 THEN 'R1' 
            WHEN recency_days <= 60 THEN 'R2' 
            WHEN recency_days <= 90 THEN 'R3' 
            ELSE 'R4' 
        END AS recency_segment,
        CASE 
            WHEN frequency >= 10 THEN 'F1' 
            WHEN frequency >= 5 THEN 'F2' 
            WHEN frequency >= 2 THEN 'F3' 
            ELSE 'F4' 
        END AS frequency_segment,
        CASE 
            WHEN monetary >= 500 THEN 'M1' 
            WHEN monetary >= 200 THEN 'M2' 
            WHEN monetary >= 100 THEN 'M3' 
            ELSE 'M4' 
        END AS monetary_segment
    FROM rfm_segments
)
SELECT 
    recency_segment,
    frequency_segment,
    monetary_segment,
    AVG(avg_sales_per_order) AS avg_sales_per_order_across_segment
FROM rfm_binned
GROUP BY recency_segment, frequency_segment, monetary_segment
ORDER BY recency_segment, frequency_segment, monetary_segment;`
  reason: pred_exec_fail
  gold_rows: []
- question: Could you help me calculate the average single career span value in years for all baseball players? Please precise the result as a float number. First, calculate the difference in years, months, and days between the debut and final game dates. For each player, the career span is computed as the sum of the absolute number of years, plus the absolute number of months divided by 12, plus the absolute number of days divided by 365. Round each part to two decimal places before summing. Finally, average the career spans and round the result to a float number.
  gold_sql: ``
  pred_sql: `SELECT 
    AVG(
        ROUND(
            (strftime('%Y', final_game) - strftime('%Y', debut)) + 
            (strftime('%m', final_game) - strftime('%m', debut)) / 12.0 + 
            (strftime('%d', final_game) - strftime('%d', debut)) / 365.0, 
            2
        )
    ) AS avg_career_span
FROM player
WHERE debut IS NOT NULL AND final_game IS NOT NULL;`
  reason: row_count_mismatch
  gold_rows: []
  pred_rows: [[None]]
- question: What is the distance of the longest route where Abakan is either the departure or destination city (in kilometers)?
  gold_sql: ``
  pred_sql: `SELECT MAX(distance) AS max_distance
FROM (
    SELECT 
        CASE 
            WHEN f.departure_airport = a1.airport_code AND a1.city->>'en' = 'Abakan' THEN a2.coordinates <-> a1.coordinates
            WHEN f.arrival_airport = a1.airport_code AND a1.city->>'en' = 'Abakan' THEN a2.coordinates <-> a1.coordinates
            ELSE NULL
        END AS distance
    FROM flights f
    JOIN airports_data a1 ON f.departure_airport = a1.airport_code
    JOIN airports_data a2 ON f.arrival_airport = a2.airport_code
    WHERE a1.city->>'en' = 'Abakan' OR a2.city->>'en' = 'Abakan'
) AS distances
WHERE distance IS NOT NULL;`
  reason: pred_exec_fail
  gold_rows: []
- question: Distribute all the unique city pairs into the distance ranges 0, 1000, 2000, 3000, 4000, 5000, and 6000+, based on their average distance of all routes between them. Then how many pairs are there in the distance range with the fewest unique city paires?
  gold_sql: ``
  pred_sql: `SELECT 
    CASE 
        WHEN avg_distance <= 1000 THEN '0-1000'
        WHEN avg_distance <= 2000 THEN '1001-2000'
        WHEN avg_distance <= 3000 THEN '2001-3000'
        WHEN avg_distance <= 4000 THEN '3001-4000'
        WHEN avg_distance <= 5000 THEN '4001-5000'
        WHEN avg_distance <= 6000 THEN '5001-6000'
        ELSE '6001+'
    END AS distance_range,
    COUNT(*) AS pair_count
FROM (
    SELECT 
        f.departure_airport,
        f.arrival_airport,
        AVG(ST_Distance(
            (SELECT coordinates FROM airports_data WHERE airport_code = f.departure_airport),
            (SELECT coordinates FROM airports_data WHERE airport_code = f.arrival_airport)
        )) AS avg_distance
    FROM flights f
    WHERE f.status = 'Arrived'
    GROUP BY f.departure_airport, f.arrival_airport
) AS city_pairs
GROUP BY distance_range
ORDER BY pair_count ASC
LIMIT 1;`
  reason: pred_exec_fail
  gold_rows: []

## Error types
- pred_exec_fail: 64
- row_count_mismatch: 14
- gold_exec_fail: 4
- pred_generation_fail: 3
- task_timeout: 2