
    WITH staging_sales_order_header AS
 (SELECT sales_order_id,
         status
    FROM {{ ref('stg_sales_order_header') }}),

         staging_sales_order_header_sales_reason AS
 (SELECT sales_order_id,
         sales_reason_id
    FROM {{ ref('stg_sales_order_header_sales_reason') }}),

         staging_sales_reason AS
 (SELECT sales_reason_id,
         reason_type
    FROM {{ ref('stg_sales_reason') }}),

         staging AS
 (SELECT DISTINCT
         ssoh.sales_order_id,
         ssoh.status,
         CASE WHEN ssoh.status = 1 THEN 'In process'
              WHEN ssoh.status = 2 THEN 'Approved'
              WHEN ssoh.status = 3 THEN 'Backordered'
              WHEN ssoh.status = 4 THEN 'Rejected'
              WHEN ssoh.status = 5 THEN 'Shipped'
              WHEN ssoh.status = 6 THEN 'Cancelled'
               END AS status_desc,
         ssr.reason_type
    FROM staging_sales_order_header AS ssoh
         INNER JOIN staging_sales_order_header_sales_reason AS ssohsr
         ON ssoh.sales_order_id = ssohsr.sales_order_id

         INNER JOIN staging_sales_reason AS ssr
         ON ssohsr.sales_reason_id = ssr.sales_reason_id),

         transformed AS
 (SELECT ROW_NUMBER() OVER(ORDER BY sales_order_id) AS sales_order_sk,
         sales_order_id,
         status,
         status_desc,
         reason_type
    FROM staging)

  SELECT *
    FROM transformed