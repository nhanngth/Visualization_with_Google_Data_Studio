-- Data comes from 2 tables: 'data-to-insights.ecommerce.all_sessions' and `data-to-insights.ecommerce.categories`

-- Get info about the number of sessions, visits, quantity sold and total revenue; other columns are needed for cross-filtering
SELECT t1.visit_id,
       t1.fullvisitorid,
       t1.session,
       t1.country,
       t1.date,
       t1.transaction_id,
       t1.channel,
       COALESCE(t2.category, 'No Category') AS category,
       t1.productSKU,
       t1.total_quantity,
       t1.total_sales
FROM
  (SELECT visitId AS visit_id,
          fullvisitorid,
          CONCAT(CAST(fullvisitorid AS string), CAST(visitid AS string),date) AS session, -- create unique ids to identify sessions
          country,
          PARSE_DATE('%Y%m%d', date) AS date -- convert dates (in STRING format) to DATE format
          transactionId AS transaction_id,
          channelGrouping AS channel,
          productSKU,
          SUM(CASE WHEN eCommerceAction_type = '6' THEN productQuantity ELSE 0 END) AS total_quantity, -- only '6' action type records have quantity
          SUM(CASE WHEN eCommerceAction_type = '6' THEN productRevenue ELSE 0 END) AS total_sales -- only '6' action type records have revenue
   FROM `data-to-insights.ecommerce.all_sessions`
   GROUP BY visitId, fullvisitorid, session, country, date, transactionId, channel, productSKU) t1
LEFT JOIN
  (SELECT productSKU, category
   FROM
     (SELECT productSKU,
             category,
             ROW_NUMBER() OVER(PARTITION BY productSKU ORDER BY (LENGTH(category)-LENGTH(REPLACE(category, '/', '')))) AS rnk -- get unique category for each productSKU
      FROM `data-to-insights.ecommerce.categories`
      ORDER BY productSKU, rnk)
   WHERE rnk = 1 ) t2 -- get productSKU and category
ON t1.productSKU = t2.productSKU




--Get data for conversion funnel
SELECT t1.channel,
       t1.country,
       t1.date,
       t1.session,
       COALESCE(t2.category, 'No Category') AS category,
       t1.productSKU,
       COUNT(DISTINCT t1.step1) AS step1,
       COUNT(DISTINCT t1.step2) AS step2,
       COUNT(DISTINCT t1.step3) AS step3,
       COUNT(DISTINCT t1.step5) AS step5,
       COUNT(DISTINCT t1.step6) AS step6
FROM
  (SELECT channelGrouping AS channel,
          country,
          PARSE_DATE('%Y%m%d', date) AS date,
          CONCAT(CAST(fullVisitorId AS string), CAST(visitId AS string), date) AS session,
          productSKU,
          CASE
              WHEN eCommerceAction_type = '1' THEN CONCAT(CAST(fullVisitorId AS string), CAST(visitId AS string), date, '1') -- Get unique ids for each type of actions
              ELSE NULL
          END AS step1,
          CASE
              WHEN eCommerceAction_type = '2' THEN CONCAT(CAST(fullVisitorId AS string), CAST(visitId AS string), date, '2')
              ELSE NULL
          END AS step2,
          CASE
              WHEN eCommerceAction_type = '3' THEN CONCAT(CAST(fullVisitorId AS string), CAST(visitId AS string), date, '3')
              ELSE NULL
          END AS step3,
          CASE
              WHEN eCommerceAction_type = '5' THEN CONCAT(CAST(fullVisitorId AS string), CAST(visitId AS string), date, '5')
              ELSE NULL
          END AS step5,
          CASE
              WHEN eCommerceAction_type = '6' THEN CONCAT(CAST(fullVisitorId AS string), CAST(visitId AS string), date, '6')
              ELSE NULL
          END AS step6
   FROM `data-to-insights.ecommerce.all_sessions`) t1
LEFT JOIN
  (SELECT productSKU, category
   FROM
     (SELECT productSKU,
             category,
             ROW_NUMBER() OVER(PARTITION BY productSKU ORDER BY (LENGTH(category)-LENGTH(REPLACE(category, '/', '')))) AS rnk  -- get unique category for each productSKU
      FROM `data-to-insights.ecommerce.categories`
      ORDER BY productSKU, rnk)
   WHERE rnk = 1 ) t2 -- get productSKU and category
ON t1.productSKU = t2.productSKU
GROUP BY t1.channel, t1.country, t1.date, t1.session, category, t1.productSKU
ORDER BY session, channel, country, date




-- Calculate times that a product is abandoned and the abbandonment rate for each of them
SELECT a.channelGrouping AS channel,
       a.country,
       a.date,
       COALESCE(c.category, 'No Category') AS category,  -- mark NULL values and replace them by a phrase as no category
       a.productSKU AS SKU,
       a.v2ProductName AS product_name,
       a.total_add_to_cart,
       b.total_purchase
FROM
  (SELECT channelGrouping,
          country,
          parse_date('%Y%m%d', date) AS date,
          productSKU,
          v2ProductName,
          CONCAT(CAST(fullVisitorId AS string), CAST(visitId AS string), date) AS session,
          SUM(productQuantity) AS total_add_to_cart
   FROM `data-to-insights.ecommerce.all_sessions`
   WHERE eCommerceAction_type = '3'
   GROUP BY channelGrouping, country, date, productSKU, v2ProductName, session) a -- Get products that have been added to cart
LEFT JOIN
  (SELECT channelGrouping,
          country,
          parse_date('%Y%m%d', date) AS date,
          productSKU,
          v2ProductName,
          CONCAT(CAST(fullVisitorId AS string), CAST(visitId AS string), date) AS session,
          SUM(productQuantity) AS total_purchase
   FROM `data-to-insights.ecommerce.all_sessions`
   WHERE eCommerceAction_type = '6'
   GROUP BY channelGrouping, country, date, productSKU, v2ProductName, session) b -- Get products that have been bought
   ON a.session = b.session AND a.v2ProductName = b.v2ProductName AND a.productSKU = b.productSKU
LEFT JOIN
  (SELECT productSKU, category
   FROM
     (SELECT productSKU,
             category,
             ROW_NUMBER() OVER(PARTITION BY productSKU ORDER BY (LENGTH(category)-LENGTH(REPLACE(category, '/', '')))) AS rnk
      FROM `data-to-insights.ecommerce.categories`
      ORDER BY productSKU, rnk)
   WHERE rnk = 1 ) c -- Get category info
   ON a.productSKU = c.productSKU
WHERE total_add_to_cart IS NOT NULL




--Get data to calculate the average period between add-to-cart and purchase completion
WITH t1 AS
  (SELECT channelGrouping,
          country,
          fullVisitorId,
          CAST(CONCAT(PARSE_DATE('%Y%m%d', DATE), ' ', FORMAT_TIMESTAMP('%T',TIMESTAMP_MILLIS(time))) AS TIMESTAMP) AS add, -- Consolidate the timestamp of action by concatnating date and time
          productsku,
          v2ProductName AS product_name,
          ROW_NUMBER() OVER(PARTITION BY fullvisitorid, v2ProductName, productsku 
          ORDER BY CAST(CONCAT(PARSE_DATE('%Y%m%d', DATE), ' ', FORMAT_TIMESTAMP('%T',TIMESTAMP_MILLIS(time))) AS TIMESTAMP)) AS rnk -- Get the order of products added to cart
   FROM `data-to-insights.ecommerce.all_sessions`
   WHERE eCommerceAction_type = '3'),
     t2 AS
  (SELECT channelGrouping,
          country,
          fullVisitorId,
          CAST(CONCAT(PARSE_DATE('%Y%m%d', DATE), ' ', FORMAT_TIMESTAMP('%T',TIMESTAMP_MILLIS(time))) AS TIMESTAMP) AS buy, -- Consolidate the timestamp of action by concatnating date and time
          productsku,
          v2ProductName AS product_name,
          ROW_NUMBER() OVER(PARTITION BY fullvisitorid, v2ProductName, productsku 
          ORDER BY CAST(CONCAT(PARSE_DATE('%Y%m%d', DATE), ' ', FORMAT_TIMESTAMP('%T',TIMESTAMP_MILLIS(time))) AS TIMESTAMP)) AS rnk -- Get the order of products purchased
   FROM `data-to-insights.ecommerce.all_sessions`
   WHERE eCommerceAction_type = '6')
SELECT t2.channelGrouping as channel,
       t2.country,
       COALESCE(t3.category,'Unknown Category') AS category,
       t1.product_name,
       FORMAT_TIMESTAMP('%Y-%m-%d', t2.buy) AS buy_action,
       TIMESTAMP_DIFF(t2.buy, t1.add, SECOND) AS time_between_add_and_buy -- Get the difference between when the product was added to cart and when the product was bought
FROM t1
JOIN t2 ON t1.fullvisitorid = t2.fullvisitorid
AND t1.product_name = t2.product_name
AND t1.productsku = t2.productsku
AND t1.rnk = t2.rnk
AND t1.add < t2.buy
LEFT JOIN (
  SELECT
    productSKU,
    category
  FROM (
    SELECT
      productSKU,
      category,
      ROW_NUMBER() OVER(PARTITION BY productSKU ORDER BY (LENGTH(category)-LENGTH(REPLACE(category,'/','')))) AS rnk
    FROM `data-to-insights.ecommerce.categories`
    ORDER BY productSKU, rnk )
  WHERE rnk = 1 ) t3 -- Get categories
ON t1.productSKU = t3.productSKU 




-- Find pairs of products that are usually bought together to generate some ideas for cross-selling campaign
WITH
  t1 AS (
  SELECT
    channelGrouping,
    country,
    parse_DATE('%Y%m%d', date) AS date,
    transactionId,
    productSKU,
    v2ProductName,
    SUM(productRevenue/1000000) AS revenue
  FROM `data-to-insights.ecommerce.all_sessions`
  WHERE eCommerceAction_type = '6'
  GROUP BY channelGrouping, country, date, transactionId, productSKU, v2ProductName) -- Get all products purchased
SELECT
  a.channelGrouping AS channel,
  a.country,
  a.date,
  COALESCE(c.category,'Unknown Category') AS category,
  a.v2ProductName AS original_product,
  b.v2ProductName AS product_bought_with,
  COUNT(*) AS times_bought_together,
  SUM(a.revenue + b.revenue) AS total_revenue
FROM t1 a INNER JOIN t1 b
ON a.transactionId = b.transactionId -- Only keep products that have been purchased in an order
  AND a.v2ProductName < b.v2ProductName -- Avoid duplications results
LEFT JOIN (
  SELECT
    productSKU,
    category
  FROM (
    SELECT
      productSKU,
      category,
      ROW_NUMBER() OVER(PARTITION BY productSKU ORDER BY (LENGTH(category)-LENGTH(REPLACE(category,'/','')))) AS rnk
    FROM `data-to-insights.ecommerce.categories`
    ORDER BY productSKU, rnk )
  WHERE rnk = 1 ) c -- Get categories
on a.productSKU = c.productSKU
GROUP BY a.channelGrouping, a.country, a.date, category, a.v2ProductName, b.v2ProductName