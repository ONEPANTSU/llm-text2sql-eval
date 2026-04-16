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
    WHERE o.order_purchase_timestamp >= '2017-01-01' 
      AND o.order_purchase_timestamp <= '2018-08-29'
    GROUP BY DATE(o.order_purchase_timestamp)
),
regression AS (
    SELECT 
        AVG(sale_date) AS avg_date,
        AVG(daily_sales_amount) AS avg_sales,
        SUM((sale_date - (SELECT AVG(sale_date) FROM daily_sales)) * 
            (daily_sales_amount - (SELECT AVG(daily_sales_amount) FROM daily_sales))) /
        NULLIF(SUM((sale_date - (SELECT AVG(sale_date) FROM daily_sales)) * 
                   (sale_date - (SELECT AVG(sale_date) FROM daily_sales))), 0) AS slope,
        (SELECT AVG(daily_sales_amount) FROM daily_sales) - 
        (SUM((sale_date - (SELECT AVG(sale_date) FROM daily_sales)) * 
             (daily_sales_amount - (SELECT AVG(daily_sales_amount) FROM daily_sales))) /
         NULLIF(SUM((sale_date - (SELECT AVG(sale_date) FROM daily_sales)) * 
                    (sale_date - (SELECT AVG(sale_date) FROM daily_sales))), 0) *
         (SELECT AVG(sale_date) FROM daily_sales)) AS intercept
    FROM daily_sales
),
predicted_sales AS (
    SELECT 
        '2018-12-05' AS sale_date, 
        (SELECT slope FROM regression) * julianday('2018-12-05') + (SELECT intercept FROM regression) AS predicted_sales
    UNION ALL
    SELECT 
        '2018-12-06', 
        (SELECT slope FROM regression) * julianday('2018-12-06') + (SELECT intercept FROM regression)
    UNION ALL
    SELECT 
        '2018-12-07', 
        (SELECT slope FROM regression) * julianday('2018-12-07') + (SELECT intercept FROM regression)
    UNION ALL
    SELECT 
        '2018-12-08', 
        (SELECT slope FROM regression) * julianday('2018-12-08') + (SELECT intercept FROM regression)
),
moving_avg AS (
    SELECT 
        sale_date,
        predicted_sales,
        AVG(predicted_sales) OVER (
            ORDER BY sale_date 
            ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING
        ) AS symmetric_5day_ma
    FROM predicted_sales
    WHERE sale_date BETWEEN '2018-12-05' AND '2018-12-08'
)
SELECT 
    SUM(symmetric_5day_ma) AS total_symmetric_ma
FROM moving_avg;`
  reason: row_count_mismatch
  gold_rows: []
  pred_rows: [[None]]
- question: Could you help me calculate the average single career span value in years for all baseball players? Please precise the result as a float number. First, calculate the difference in years, months, and days between the debut and final game dates. For each player, the career span is computed as the sum of the absolute number of years, plus the absolute number of months divided by 12, plus the absolute number of days divided by 365. Round each part to two decimal places before summing. Finally, average the career spans and round the result to a float number.
  gold_sql: ``
  pred_sql: `SELECT ROUND(AVG(
    CAST(
        ABS(JULIANDAY(final_game) - JULIANDAY(debut)) / 365.0
    AS FLOAT)
), 2) AS avg_career_span
FROM player
WHERE debut IS NOT NULL AND final_game IS NOT NULL;`
  reason: row_count_mismatch
  gold_rows: []
  pred_rows: [[None]]
- question: What is the distance of the longest route where Abakan is either the departure or destination city (in kilometers)?
  gold_sql: ``
  pred_sql: `SELECT MAX(a1.coordinates) - MAX(a2.coordinates) AS distance_km
FROM flights f
JOIN airports_data a1 ON f.departure_airport = a1.airport_code
JOIN airports_data a2 ON f.arrival_airport = a2.airport_code
WHERE (a1.city = 'Abakan' OR a2.city = 'Abakan');`
  reason: row_count_mismatch
  gold_rows: []
  pred_rows: [[None]]
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
  pred_sql: `SELECT NULL, NULL;`
  reason: gold_exec_fail
  pred_rows: [[None, None]]
- question: For each match, considering every innings, please combine runs from both batsman scored and extra runs for each over, then identify the single over with the highest total runs, retrieve the bowler for that over from the ball by ball table, and calculate the average of these highest over totals across all matches, ensuring that all runs and bowler details are accurately reflected.
  gold_sql: ``
  pred_sql: `WITH over_runs AS (
    SELECT 
        bb.match_id,
        bb.over_id,
        bb.innings_no,
        bb.bowler,
        COALESCE(SUM(bs.runs_scored), 0) + COALESCE(SUM(er.extra_runs), 0) AS total_runs
    FROM ball_by_ball bb
    LEFT JOIN batsman_scored bs 
        ON bb.match_id = bs.match_id 
        AND bb.over_id = bs.over_id 
        AND bb.innings_no = bs.innings_no
    LEFT JOIN extra_runs er 
        ON bb.match_id = er.match_id 
        AND bb.over_id = er.over_id 
        AND bb.innings_no = er.innings_no
    GROUP BY bb.match_id, bb.over_id, bb.innings_no, bb.bowler
),
highest_over_per_match AS (
    SELECT 
        match_id,
        over_id,
        innings_no,
        bowler,
        total_runs,
        ROW_NUMBER() OVER (PARTITION BY match_id ORDER BY total_runs DESC) AS rn
    FROM over_runs
)
SELECT 
    AVG(total_runs) AS avg_highest_over_runs
FROM highest_over_per_match
WHERE rn = 1;`
  reason: row_count_mismatch
  gold_rows: []
  pred_rows: [[None]]

## Error types
- row_count_mismatch: 22
- gold_exec_fail: 5
- pred_generation_fail: 5
- task_timeout: 2
- row_mismatch_0: 1