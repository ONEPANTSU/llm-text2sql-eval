# spider2 debug

## Top mismatches
- question: Can you calculate the 5-day symmetric moving average of predicted toy sales for December 5 to 8, 2018, using daily sales data from January 1, 2017, to August 29, 2018, with a simple linear regression model? Finally provide the sum of those four 5-day moving averages?
  gold_sql: ``
  pred_sql: ``
  reason: pred_generation_fail
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
  pred_sql: ``
  reason: pred_generation_fail
- question: Could you tell me the number of orders, average payment per order and customer lifespan in weeks of the 3 custumers with the highest average payment per order, where the lifespan is calculated by subtracting the earliest purchase date from the latest purchase date in days, dividing by seven, and if the result is less than seven days, setting it to 1.0?
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
  reason: pred_generation_fail
- question: Could you help me calculate the average single career span value in years for all baseball players? Please precise the result as a float number. First, calculate the difference in years, months, and days between the debut and final game dates. For each player, the career span is computed as the sum of the absolute number of years, plus the absolute number of months divided by 12, plus the absolute number of days divided by 365. Round each part to two decimal places before summing. Finally, average the career spans and round the result to a float number.
  gold_sql: ``
  pred_sql: ``
  reason: pred_generation_fail
- question: I would like to know the given names of baseball players who have achieved the highest value of games played, runs, hits, and home runs, with their corresponding score values.
  gold_sql: `WITH player_stats AS (
    SELECT
        b.player_id,
        p.name_given AS player_name,
        SUM(b.g) AS games_played,
        SUM(b.r) AS runs,
        SUM(b.h) AS hits,
        SUM(b.hr) AS home_runs
    FROM player p
    JOIN batting b ON p.player_id = b.player_id
    GROUP BY b.player_id, p.name_given
)

SELECT 'Games Played' AS Category, player_name AS Player_Name, games_played AS Batting_Table_Topper
FROM player_stats
WHERE games_played = (SELECT MAX(games_played) FROM player_stats)

UNION ALL

SELECT 'Runs' AS Category, player_name AS Player_Name, runs AS Batting_Table_Topper
FROM player_stats
WHERE runs = (SELECT MAX(runs) FROM player_stats)

UNION ALL

SELECT 'Hits' AS Category, player_name AS Player_Name, hits AS Batting_Table_Topper
FROM player_stats
WHERE hits = (SELECT MAX(hits) FROM player_stats)

UNION ALL

SELECT 'Home Runs' AS Category, player_name AS Player_Name, home_runs AS Batting_Table_Topper
FROM player_stats
WHERE home_runs = (SELECT MAX(home_runs) FROM player_stats);`
  pred_sql: ``
  reason: pred_generation_fail

## Error types
- pred_generation_fail: 97
- pred_exec_fail: 15
- row_count_mismatch: 2
- gold_exec_fail: 1