
    WITH staging AS
 (SELECT product_id,
         name
    FROM {{ ref('stg_product') }}),

         transformed AS
 (SELECT ROW_NUMBER() OVER(ORDER BY product_id) AS product_sk,
         product_id,
         name
    FROM staging)

  SELECT *
     FROM transformed
