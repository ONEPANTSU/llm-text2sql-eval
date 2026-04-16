# tpcds debug

## Top mismatches
- question: Task:
Compare total web and catalog sales revenue by day of week across two consecutive years, per calendar week.

Details:
- Data sources: web_sales and catalog_sales (UNION ALL); join date_dim for week and day.
- Metric: sum of ext_sales_price (web: ws_ext_sales_price, catalog: cs_ext_sales_price) by d_week_seq and day name (Sunday–Saturday).
- Compare year 2001 vs year 2002: align weeks with d_week_seq1 = d_week_seq2 - 53.
- Return d_week_seq and ratios (round(sun_sales1/sun_sales2, 2), etc.) for each day.
- Order by d_week_seq.
  gold_sql: `WITH wscs AS
  (SELECT sold_date_sk,
          sales_price
   FROM
     (SELECT ws_sold_date_sk sold_date_sk,
             ws_ext_sales_price sales_price
      FROM web_sales
      UNION ALL SELECT cs_sold_date_sk sold_date_sk,
                       cs_ext_sales_price sales_price
      FROM catalog_sales) sq1),
     wswscs AS
  (SELECT d_week_seq,
          sum(CASE
                  WHEN (d_day_name='Sunday') THEN sales_price
                  ELSE NULL
              END) sun_sales,
          sum(CASE
                  WHEN (d_day_name='Monday') THEN sales_price
                  ELSE NULL
              END) mon_sales,
          sum(CASE
                  WHEN (d_day_name='Tuesday') THEN sales_price
                  ELSE NULL
              END) tue_sales,
          sum(CASE
                  WHEN (d_day_name='Wednesday') THEN sales_price
                  ELSE NULL
              END) wed_sales,
          sum(CASE
                  WHEN (d_day_name='Thursday') THEN sales_price
                  ELSE NULL
              END) thu_sales,
          sum(CASE
                  WHEN (d_day_name='Friday') THEN sales_price
                  ELSE NULL
              END) fri_sales,
          sum(CASE
                  WHEN (d_day_name='Saturday') THEN sales_price
                  ELSE NULL
              END) sat_sales
   FROM wscs,
        date_dim
   WHERE d_date_sk = sold_date_sk
   GROUP BY d_week_seq)
SELECT d_week_seq1,
       round(sun_sales1/sun_sales2, 2) r1,
       round(mon_sales1/mon_sales2, 2) r2,
       round(tue_sales1/tue_sales2, 2) r3,
       round(wed_sales1/wed_sales2, 2) r4,
       round(thu_sales1/thu_sales2, 2) r5,
       round(fri_sales1/fri_sales2, 2) r6,
       round(sat_sales1/sat_sales2, 2)
FROM
  (SELECT wswscs.d_week_seq d_week_seq1,
          sun_sales sun_sales1,
          mon_sales mon_sales1,
          tue_sales tue_sales1,
          wed_sales wed_sales1,
          thu_sales thu_sales1,
          fri_sales fri_sales1,
          sat_sales sat_sales1
   FROM wswscs,
        date_dim
   WHERE date_dim.d_week_seq = wswscs.d_week_seq
     AND d_year = 2001) y,
  (SELECT wswscs.d_week_seq d_week_seq2,
          sun_sales sun_sales2,
          mon_sales mon_sales2,
          tue_sales tue_sales2,
          wed_sales wed_sales2,
          thu_sales thu_sales2,
          fri_sales fri_sales2,
          sat_sales sat_sales2
   FROM wswscs,
        date_dim
   WHERE date_dim.d_week_seq = wswscs.d_week_seq
     AND d_year = 2001+1) z
WHERE d_week_seq1 = d_week_seq2-53
ORDER BY d_week_seq1 NULLS FIRST;
`
  pred_sql: ``
  reason: pred_generation_fail
- question: Task:
Total store sales revenue by brand and year for a given manufacturer.

Details:
- Use store_sales, date_dim, item.
- Filter: item.i_manufact_id = 128, date_dim.d_moy = 11.
- Metric: sum(ss_ext_sales_price).
- Group by d_year, i_brand, i_brand_id.
- Return d_year, brand_id, brand, sum_agg.
- Order by d_year, sum_agg DESC, brand_id. Limit 100.
  gold_sql: `SELECT dt.d_year,
       item.i_brand_id brand_id,
       item.i_brand brand,
       sum(ss_ext_sales_price) sum_agg
FROM date_dim dt,
     store_sales,
     item
WHERE dt.d_date_sk = store_sales.ss_sold_date_sk
  AND store_sales.ss_item_sk = item.i_item_sk
  AND item.i_manufact_id = 128
  AND dt.d_moy=11
GROUP BY dt.d_year,
         item.i_brand,
         item.i_brand_id
ORDER BY dt.d_year,
         sum_agg DESC,
         brand_id
LIMIT 100;
`
  pred_sql: ``
  reason: pred_generation_fail
- question: Task:
Customers with high combined sales across store, catalog, and web in two consecutive years; profile attributes.

Details:
- Data: store_sales, catalog_sales, web_sales, customer, date_dim. Year total = (ext_list_price - ext_wholesale_cost - ext_discount_amt + ext_sales_price)/2 per channel.
- Same customer must have sales in all three channels in year 2001 and 2002; filter by year_total > 0 and catalog/year ratio > store/year ratio and catalog/year ratio > web/year ratio.
- Return c_customer_id, c_first_name, c_last_name, c_preferred_cust_flag.
- Order by customer_id, first_name, last_name, preferred_cust_flag. Limit 100.
  gold_sql: `WITH year_total AS
  (SELECT c_customer_id customer_id,
          c_first_name customer_first_name,
          c_last_name customer_last_name,
          c_preferred_cust_flag customer_preferred_cust_flag,
          c_birth_country customer_birth_country,
          c_login customer_login,
          c_email_address customer_email_address,
          d_year dyear,
          sum(((ss_ext_list_price-ss_ext_wholesale_cost-ss_ext_discount_amt)+ss_ext_sales_price)/2) year_total,
          's' sale_type
   FROM customer,
        store_sales,
        date_dim
   WHERE c_customer_sk = ss_customer_sk
     AND ss_sold_date_sk = d_date_sk
   GROUP BY c_customer_id,
            c_first_name,
            c_last_name,
            c_preferred_cust_flag,
            c_birth_country,
            c_login,
            c_email_address,
            d_year
   UNION ALL SELECT c_customer_id customer_id,
                    c_first_name customer_first_name,
                    c_last_name customer_last_name,
                    c_preferred_cust_flag customer_preferred_cust_flag,
                    c_birth_country customer_birth_country,
                    c_login customer_login,
                    c_email_address customer_email_address,
                    d_year dyear,
                    sum((((cs_ext_list_price-cs_ext_wholesale_cost-cs_ext_discount_amt)+cs_ext_sales_price)/2)) year_total,
                    'c' sale_type
   FROM customer,
        catalog_sales,
        date_dim
   WHERE c_customer_sk = cs_bill_customer_sk
     AND cs_sold_date_sk = d_date_sk
   GROUP BY c_customer_id,
            c_first_name,
            c_last_name,
            c_preferred_cust_flag,
            c_birth_country,
            c_login,
            c_email_address,
            d_year
   UNION ALL SELECT c_customer_id customer_id,
                    c_first_name customer_first_name,
                    c_last_name customer_last_name,
                    c_preferred_cust_flag customer_preferred_cust_flag,
                    c_birth_country customer_birth_country,
                    c_login customer_login,
                    c_email_address customer_email_address,
                    d_year dyear,
                    sum((((ws_ext_list_price-ws_ext_wholesale_cost-ws_ext_discount_amt)+ws_ext_sales_price)/2)) year_total,
                    'w' sale_type
   FROM customer,
        web_sales,
        date_dim
   WHERE c_customer_sk = ws_bill_customer_sk
     AND ws_sold_date_sk = d_date_sk
   GROUP BY c_customer_id,
            c_first_name,
            c_last_name,
            c_preferred_cust_flag,
            c_birth_country,
            c_login,
            c_email_address,
            d_year)
SELECT t_s_secyear.customer_id,
       t_s_secyear.customer_first_name,
       t_s_secyear.customer_last_name,
       t_s_secyear.customer_preferred_cust_flag
FROM year_total t_s_firstyear,
     year_total t_s_secyear,
     year_total t_c_firstyear,
     year_total t_c_secyear,
     year_total t_w_firstyear,
     year_total t_w_secyear
WHERE t_s_secyear.customer_id = t_s_firstyear.customer_id
  AND t_s_firstyear.customer_id = t_c_secyear.customer_id
  AND t_s_firstyear.customer_id = t_c_firstyear.customer_id
  AND t_s_firstyear.customer_id = t_w_firstyear.customer_id
  AND t_s_firstyear.customer_id = t_w_secyear.customer_id
  AND t_s_firstyear.sale_type = 's'
  AND t_c_firstyear.sale_type = 'c'
  AND t_w_firstyear.sale_type = 'w'
  AND t_s_secyear.sale_type = 's'
  AND t_c_secyear.sale_type = 'c'
  AND t_w_secyear.sale_type = 'w'
  AND t_s_firstyear.dyear = 2001
  AND t_s_secyear.dyear = 2001+1
  AND t_c_firstyear.dyear = 2001
  AND t_c_secyear.dyear = 2001+1
  AND t_w_firstyear.dyear = 2001
  AND t_w_secyear.dyear = 2001+1
  AND t_s_firstyear.year_total > 0
  AND t_c_firstyear.year_total > 0
  AND t_w_firstyear.year_total > 0
  AND CASE
          WHEN t_c_firstyear.year_total > 0 THEN t_c_secyear.year_total / t_c_firstyear.year_total
          ELSE NULL
      END > CASE
                WHEN t_s_firstyear.year_total > 0 THEN t_s_secyear.year_total / t_s_firstyear.year_total
                ELSE NULL
            END
  AND CASE
          WHEN t_c_firstyear.year_total > 0 THEN t_c_secyear.year_total / t_c_firstyear.year_total
          ELSE NULL
      END > CASE
                WHEN t_w_firstyear.year_total > 0 THEN t_w_secyear.year_total / t_w_firstyear.year_total
                ELSE NULL
            END
ORDER BY t_s_secyear.customer_id NULLS FIRST,
         t_s_secyear.customer_first_name NULLS FIRST,
         t_s_secyear.customer_last_name NULLS FIRST,
         t_s_secyear.customer_preferred_cust_flag NULLS FIRST
LIMIT 100;
`
  pred_sql: `WITH store_agg AS (
    SELECT
        ss_customer_sk AS c_customer_id,
        d_year AS year,
        SUM((ss_ext_list_price - ss_ext_wholesale_cost - ss_ext_discount_amt + ss_ext_sales_price) / 2.0) AS store_total
    FROM store_sales
    JOIN date_dim ON ss_sold_date_sk = d_date_sk
    GROUP BY ss_customer_sk, d_year
),
catalog_agg AS (
    SELECT
        cs_customer_sk AS c_customer_id,
        d_year AS year,
        SUM((cs_ext_list_price - cs_ext_wholesale_cost - cs_ext_discount_amt + cs_ext_sales_price) / 2.0) AS catalog_total
    FROM catalog_sales
    JOIN date_dim ON cs_sold_date_sk = d_date_sk
    GROUP BY cs_customer_sk, d_year
),
web_agg AS (
    SELECT
        ws_customer_sk AS c_customer_id,
        d_year AS year,
        SUM((ws_ext_list_price - ws_ext_wholesale_cost - ws_ext_discount_amt + ws_ext_sales_price) / 2.0) AS web_total
    FROM web_sales
    JOIN date_dim ON ws_sold_date_sk = d_date_sk
    GROUP BY ws_customer_sk, d_year
),
qualified AS (
    SELECT
        s.c_customer_id,
        s.year
    FROM store_agg s
    JOIN catalog_agg c ON s.c_customer_id = c.c_customer_id AND s.year = c.year
    JOIN web_agg w ON s.c_customer_id = w.c_customer_id AND s.year = w.year
    WHERE s.year IN (2001, 2002)
      AND s.store_total > 0
      AND c.catalog_total > 0
      AND w.web_total > 0
      AND c.catalog_total > s.store_total
      AND c.catalog_total > w.web_total
)
SELECT
    cust.c_customer_id,
    cust.c_first_name,
    cust.c_last_name,
    cust.c_preferred_cust_flag
FROM qualified q
JOIN customer cust ON q.c_customer_id = cust.c_customer_sk
GROUP BY cust.c_customer_id, cust.c_first_name, cust.c_last_name, cust.c_preferred_cust_flag
HAVING COUNT(DISTINCT q.year) = 2
ORDER BY cust.c_customer_id, cust.c_first_name, cust.c_last_name, cust.c_preferred_cust_flag
LIMIT 100;`
  reason: pred_bind_fail
  gold_rows: []
- question: Task:
Channel-level sales, returns, and profit with ROLLUP for a date range.

Details:
- Data: store_sales + store_returns (UNION), catalog_sales + catalog_returns (UNION), web_sales + web_returns; date_dim; store, catalog_page, web_site.
- Time: d_date between '2000-08-23' and '2000-09-06'.
- Aggregate per channel and id (store_id, catalog_page_id, web_site_id): sum(sales), sum(returns_), sum(profit).
- Return channel, id, sales, returns_, profit. GROUP BY ROLLUP(channel, id).
- Order by channel, id. Limit 100.
  gold_sql: `WITH ssr AS
  (SELECT s_store_id,
          sum(sales_price) AS sales,
          sum(profit) AS profit,
          sum(return_amt) AS returns_,
          sum(net_loss) AS profit_loss
   FROM
     (SELECT ss_store_sk AS store_sk,
             ss_sold_date_sk AS date_sk,
             ss_ext_sales_price AS sales_price,
             ss_net_profit AS profit,
             cast(0 AS decimal(7,2)) AS return_amt,
             cast(0 AS decimal(7,2)) AS net_loss
      FROM store_sales
      UNION ALL SELECT sr_store_sk AS store_sk,
                       sr_returned_date_sk AS date_sk,
                       cast(0 AS decimal(7,2)) AS sales_price,
                       cast(0 AS decimal(7,2)) AS profit,
                       sr_return_amt AS return_amt,
                       sr_net_loss AS net_loss
      FROM store_returns ) salesreturns,
        date_dim,
        store
   WHERE date_sk = d_date_sk
     AND d_date BETWEEN cast('2000-08-23' AS date) AND cast('2000-09-06' AS date)
     AND store_sk = s_store_sk
   GROUP BY s_store_id) ,
     csr AS
  (SELECT cp_catalog_page_id,
          sum(sales_price) AS sales,
          sum(profit) AS profit,
          sum(return_amt) AS returns_,
          sum(net_loss) AS profit_loss
   FROM
     (SELECT cs_catalog_page_sk AS page_sk,
             cs_sold_date_sk AS date_sk,
             cs_ext_sales_price AS sales_price,
             cs_net_profit AS profit,
             cast(0 AS decimal(7,2)) AS return_amt,
             cast(0 AS decimal(7,2)) AS net_loss
      FROM catalog_sales
      UNION ALL SELECT cr_catalog_page_sk AS page_sk,
                       cr_returned_date_sk AS date_sk,
                       cast(0 AS decimal(7,2)) AS sales_price,
                       cast(0 AS decimal(7,2)) AS profit,
                       cr_return_amount AS return_amt,
                       cr_net_loss AS net_loss
      FROM catalog_returns ) salesreturns,
        date_dim,
        catalog_page
   WHERE date_sk = d_date_sk
     AND d_date BETWEEN cast('2000-08-23' AS date) AND cast('2000-09-06' AS date)
     AND page_sk = cp_catalog_page_sk
   GROUP BY cp_catalog_page_id) ,
     wsr AS
  (SELECT web_site_id,
          sum(sales_price) AS sales,
          sum(profit) AS profit,
          sum(return_amt) AS returns_,
          sum(net_loss) AS profit_loss
   FROM
     (SELECT ws_web_site_sk AS wsr_web_site_sk,
             ws_sold_date_sk AS date_sk,
             ws_ext_sales_price AS sales_price,
             ws_net_profit AS profit,
             cast(0 AS decimal(7,2)) AS return_amt,
             cast(0 AS decimal(7,2)) AS net_loss
      FROM web_sales
      UNION ALL SELECT ws_web_site_sk AS wsr_web_site_sk,
                       wr_returned_date_sk AS date_sk,
                       cast(0 AS decimal(7,2)) AS sales_price,
                       cast(0 AS decimal(7,2)) AS profit,
                       wr_return_amt AS return_amt,
                       wr_net_loss AS net_loss
      FROM web_returns
      LEFT OUTER JOIN web_sales ON (wr_item_sk = ws_item_sk
                                    AND wr_order_number = ws_order_number) ) salesreturns,
        date_dim,
        web_site
   WHERE date_sk = d_date_sk
     AND d_date BETWEEN cast('2000-08-23' AS date) AND cast('2000-09-06' AS date)
     AND wsr_web_site_sk = web_site_sk
   GROUP BY web_site_id)
SELECT channel ,
       id ,
       sum(sales) AS sales ,
       sum(returns_) AS returns_ ,
       sum(profit) AS profit
FROM
  (SELECT 'store channel' AS channel ,
          concat('store', s_store_id) AS id ,
          sales ,
          returns_ ,
          (profit - profit_loss) AS profit
   FROM ssr
   UNION ALL SELECT 'catalog channel' AS channel ,
                    concat('catalog_page', cp_catalog_page_id) AS id ,
                    sales ,
                    returns_ ,
                    (profit - profit_loss) AS profit
   FROM csr
   UNION ALL SELECT 'web channel' AS channel ,
                    concat('web_site', web_site_id) AS id ,
                    sales ,
                    returns_ ,
                    (profit - profit_loss) AS profit
   FROM wsr ) x
GROUP BY ROLLUP (channel,
                 id)
ORDER BY channel NULLS FIRST,
         id NULLS FIRST
LIMIT 100;
`
  pred_sql: ``
  reason: pred_generation_fail
- question: Task:
Count of customers by state where items have current price above 1.2× category average, for a given month.

Details:
- Use customer_address, customer, store_sales, date_dim, item.
- Time: d_month_seq from (d_year=2001, d_moy=1).
- Filter: i_current_price > 1.2 * avg(i_current_price) per i_category.
- Group by ca_state. HAVING count(*) >= 10.
- Return state (ca_state), cnt. Order by cnt, ca_state. Limit 100.
  gold_sql: `SELECT a.ca_state state,
       count(*) cnt
FROM customer_address a ,
     customer c ,
     store_sales s ,
     date_dim d ,
     item i
WHERE a.ca_address_sk = c.c_current_addr_sk
  AND c.c_customer_sk = s.ss_customer_sk
  AND s.ss_sold_date_sk = d.d_date_sk
  AND s.ss_item_sk = i.i_item_sk
  AND d.d_month_seq =
    (SELECT DISTINCT (d_month_seq)
     FROM date_dim
     WHERE d_year = 2001
       AND d_moy = 1 )
  AND i.i_current_price > 1.2 *
    (SELECT avg(j.i_current_price)
     FROM item j
     WHERE j.i_category = i.i_category)
GROUP BY a.ca_state
HAVING count(*) >= 10
ORDER BY cnt NULLS FIRST,
         a.ca_state NULLS FIRST
LIMIT 100;
`
  pred_sql: ``
  reason: pred_generation_fail

## Error types
- pred_generation_fail: 53
- row_count_mismatch: 13
- row_mismatch_0: 11
- pred_bind_fail: 9
- pred_parse_fail: 4
- pred_runtime_fail: 2
- pred_invalid_sql: 1