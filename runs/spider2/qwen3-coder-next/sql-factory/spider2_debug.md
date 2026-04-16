# spider2 debug

## Top mismatches
- question: Can you calculate the 5-day symmetric moving average of predicted toy sales for December 5 to 8, 2018, using daily sales data from January 1, 2017, to August 29, 2018, with a simple linear regression model? Finally provide the sum of those four 5-day moving averages?
  gold_sql: ``
  pred_sql: `WITH daily_sales AS (
    SELECT 
        DATE(o.order_purchase_timestamp) AS sale_date,
        COUNT(oi.order_id) AS daily_sales_count
    FROM order_items oi
    JOIN orders o ON oi.order_id = o.order_id
    WHERE DATE(o.order_purchase_timestamp) BETWEEN '2017-01-01' AND '2018-08-29'
    GROUP BY DATE(o.order_purchase_timestamp)
),
regression AS (
    SELECT 
        AVG(sale_date) AS avg_date,
        AVG(daily_sales_count) AS avg_sales,
        SUM((strftime('%s', sale_date) - (SELECT AVG(strftime('%s', sale_date)) FROM daily_sales)) * 
            (daily_sales_count - (SELECT AVG(daily_sales_count) FROM daily_sales))) / 
            NULLIF(SUM(POW((strftime('%s', sale_date) - (SELECT AVG(strftime('%s', sale_date)) FROM daily_sales)), 2)), 0) AS slope,
        (SELECT AVG(daily_sales_count) FROM daily_sales) - 
            (SUM((strftime('%s', sale_date) - (SELECT AVG(strftime('%s', sale_date)) FROM daily_sales)) * 
            (daily_sales_count - (SELECT AVG(daily_sales_count) FROM daily_sales))) / 
            NULLIF(SUM(POW((strftime('%s', sale_date) - (SELECT AVG(strftime('%s', sale_date)) FROM daily_sales)), 2)), 0)) * 
            (SELECT AVG(strftime('%s', sale_date)) FROM daily_sales) AS intercept
    FROM daily_sales
),
predicted_sales AS (
    SELECT 
        '2018-12-05' AS sale_date,
        (SELECT slope FROM regression) * strftime('%s', '2018-12-05') + (SELECT intercept FROM regression) AS predicted_sales
    UNION ALL
    SELECT 
        '2018-12-06' AS sale_date,
        (SELECT slope FROM regression) * strftime('%s', '2018-12-06') + (SELECT intercept FROM regression) AS predicted_sales
    UNION ALL
    SELECT 
        '2018-12-07' AS sale_date,
        (SELECT slope FROM regression) * strftime('%s', '2018-12-07') + (SELECT intercept FROM regression) AS predicted_sales
    UNION ALL
    SELECT 
        '2018-12-08' AS sale_date,
        (SELECT slope FROM regression) * strftime('%s', '2018-12-08') + (SELECT intercept FROM regression) AS predicted_sales
),
symmetric_ma AS (
    SELECT 
        sale_date,
        (LAG(predicted_sales, 2) OVER (ORDER BY sale_date) +
         LAG(predicted_sales, 1) OVER (ORDER BY sale_date) +
         predicted_sales +
         LEAD(predicted_sales, 1) OVER (ORDER BY sale_date) +
         LEAD(predicted_sales, 2) OVER (ORDER BY sale_date)) / 5.0 AS symmetric_ma
    FROM predicted_sales
)
SELECT 
    SUM(symmetric_ma) AS total_symmetric_ma
FROM symmetric_ma;`
  reason: row_count_mismatch
  gold_rows: []
  pred_rows: [[None]]
- question: Could you help me calculate the average single career span value in years for all baseball players? Please precise the result as a float number. First, calculate the difference in years, months, and days between the debut and final game dates. For each player, the career span is computed as the sum of the absolute number of years, plus the absolute number of months divided by 12, plus the absolute number of days divided by 365. Round each part to two decimal places before summing. Finally, average the career spans and round the result to a float number.
  gold_sql: ``
  pred_sql: `SELECT ROUND(AVG(
    CAST(ABS(JULIANDAY(final_game) - JULIANDAY(debut)) AS REAL) / 365.0
), 2) AS avg_career_span_years
FROM player
WHERE debut IS NOT NULL AND final_game IS NOT NULL;`
  reason: row_count_mismatch
  gold_rows: []
  pred_rows: [[None]]
- question: What is the distance of the longest route where Abakan is either the departure or destination city (in kilometers)?
  gold_sql: ``
  pred_sql: `SELECT MAX(a.distance) AS longest_route_distance_km
FROM (
    SELECT 
        6371 * 2 * ASIN(SQRT(
            POWER(SIN((radians(a2.coordinates[1]) - radians(a1.coordinates[1])) / 2), 2) +
            COS(radians(a1.coordinates[1])) * COS(radians(a2.coordinates[1])) *
            POWER(SIN((radians(a2.coordinates[0]) - radians(a1.coordinates[0])) / 2), 2)
        )) AS distance
    FROM flights f
    JOIN airports_data a1 ON f.departure_airport = a1.airport_code
    JOIN airports_data a2 ON f.arrival_airport = a2.airport_code
    WHERE a1.city = 'Abakan' OR a2.city = 'Abakan'
) a`
  reason: pred_exec_fail
  gold_rows: []
- question: Distribute all the unique city pairs into the distance ranges 0, 1000, 2000, 3000, 4000, 5000, and 6000+, based on their average distance of all routes between them. Then how many pairs are there in the distance range with the fewest unique city paires?
  gold_sql: ``
  pred_sql: `WITH city_pairs AS (
    SELECT 
        f.departure_airport,
        f.arrival_airport,
        AVG(
            6371 * 2 * ASIN(
                SQRT(
                    POWER(SIN((r2.coordinates[1] - r1.coordinates[1]) * 3.14159265358979 / 360), 2) +
                    COS(r1.coordinates[1] * 3.14159265358979 / 180) * 
                    COS(r2.coordinates[1] * 3.14159265358979 / 180) * 
                    POWER(SIN((r2.coordinates[0] - r1.coordinates[0]) * 3.14159265358979 / 360), 2)
                )
            )
        ) AS avg_distance
    FROM flights f
    JOIN airports_data r1 ON f.departure_airport = r1.airport_code
    JOIN airports_data r2 ON f.arrival_airport = r2.airport_code
    WHERE r1.coordinates IS NOT NULL AND r2.coordinates IS NOT NULL
    GROUP BY f.departure_airport, f.arrival_airport
),
distance_ranges AS (
    SELECT 
        CASE 
            WHEN avg_distance < 1000 THEN '0-999'
            WHEN avg_distance < 2000 THEN '1000-1999'
            WHEN avg_distance < 3000 THEN '2000-2999'
            WHEN avg_distance < 4000 THEN '3000-3999'
            WHEN avg_distance < 5000 THEN '4000-4999'
            WHEN avg_distance < 6000 THEN '5000-5999'
            ELSE '6000+'
        END AS distance_range
    FROM city_pairs
)
SELECT MIN(cnt) 
FROM (
    SELECT distance_range, COUNT(*) AS cnt
    FROM distance_ranges
    GROUP BY distance_range
);`
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
  pred_sql: `SELECT 
    w1.name AS wrestler1, 
    w2.name AS wrestler2
FROM 
    Matches m
JOIN 
    Wrestlers w1 ON m.wrestler1_id = w1.id
JOIN 
    Wrestlers w2 ON m.wrestler2_id = w2.id
JOIN 
    Titles t ON m.title_id = t.id
WHERE 
    t.name = 'NXT' 
    AND m.title_change = 0
ORDER BY 
    m.duration ASC
LIMIT 1;`
  reason: gold_exec_fail

## Error types
- pred_exec_fail: 25
- row_count_mismatch: 20
- gold_exec_fail: 5
- task_timeout: 2