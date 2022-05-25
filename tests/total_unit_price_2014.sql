
  SELECT EXTRACT( YEAR FROM order_date ) AS year,
         SUM( unit_price ) AS unit_price_total
    FROM {{ ref('fact_sales') }}
   WHERE EXTRACT( YEAR FROM order_date ) = 2014
GROUP BY 1
  HAVING unit_price_total <> 16620751.5803