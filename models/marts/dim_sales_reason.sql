
    WITH staging_sales_reason AS
 (SELECT sales_reason_id,
         name,
         reason_type
    FROM {{ ref('stg_sales_reason') }}),

         staging_sales_order_header_sales_reason AS
 (SELECT sales_order_id,
         sales_reason_id
    FROM {{ ref('stg_sales_order_header_sales_reason') }}),

         transformed AS
 (SELECT ROW_NUMBER() OVER(ORDER BY ssr.sales_reason_id) AS sales_reason_sk,
         ssr.sales_reason_id,
         ssohsr.sales_order_id,
         ssr.name,
         ssr.reason_type
    FROM staging_sales_reason AS ssr
         LEFT JOIN staging_sales_order_header_sales_reason AS ssohsr
         ON ssr.sales_reason_id = ssohsr.sales_reason_id)

  SELECT *
    FROM transformed