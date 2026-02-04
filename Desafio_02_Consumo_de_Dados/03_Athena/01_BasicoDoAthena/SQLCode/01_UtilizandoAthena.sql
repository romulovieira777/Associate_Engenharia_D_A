SELECT
  COUNT(DISTINCT order_id) AS sales
 , media_source       AS dt
FROM
  processed-data.orders-full
GROUP BY
  media_source
ORDER BY
  sales DESC;

SELECT
  *
FROM
  raw-data.orders AS t1
INNER JOIN
  raw-data.orders-items AS t2
ON
  t1.order_id = t2.order_id
LIMIT 10;

SELECT
  COUNT(order_id) AS sales
  , DATE(CAST(order_date AS TIMESTAMP)) AS dt
FROM
  orders
GROUP BY
  DATE(CAST(order_date AS TIMESTAMP))
ORDER BY
  sales DESC;

SELECT
  COUNT(DISTINCT order_id) AS sales
 , media_source       AS dt
FROM
  processed-data.orders-full
GROUP BY
  media_source
ORDER BY
  sales DESC;