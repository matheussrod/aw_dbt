
    WITH dim_customer AS
 (SELECT customer_sk,
         customer_id
    FROM {{ ref('dim_customer') }}),

         dim_product AS
 (SELECT product_sk,
         product_id
    FROM {{ ref('dim_product') }}),

         dim_sales_reason AS
 (SELECT sales_order_id,
         sales_reason_sk
    FROM {{ ref('dim_sales_reason') }}),

         dim_territory AS
 (SELECT address_sk,
         address_id
    FROM {{ ref('dim_territory') }}),

         dim_credit_card AS
 (SELECT credit_card_sk,
         credit_card_id
    FROM {{ ref('dim_credit_card') }}),

         dim_sales_information AS
 (SELECT sales_order_sk,
         sales_order_id
    FROM {{ ref('dim_sales_information') }}),

         staging_sales_order_header AS
 (SELECT sales_order_id,
         customer_id,
         ship_to_address_id,
         credit_card_id,
         order_date,
         due_date,
         ship_date
    FROM {{ ref('stg_sales_order_header') }}),

         sales_with_sk AS
 (SELECT ssod.sales_order_id,
         dp.product_sk,
         dt.address_sk,
         dcc.credit_card_sk,
         dc.customer_sk,
         dsr.sales_reason_sk,
         dsi.sales_order_sk,
         ssoh.order_date,
         ssoh.due_date,
         ssoh.ship_date,
         ssod.order_qty,
         ssod.unit_price,
         ssod.unit_price_discount
    FROM {{ ref('stg_sales_order_detail') }} AS ssod
         LEFT JOIN staging_sales_order_header AS ssoh
         ON ssod.sales_order_id = ssoh.sales_order_id

         LEFT JOIN dim_product AS dp
         ON ssod.product_id = dp.product_id

         LEFT JOIN dim_territory AS dt
         ON ssoh.ship_to_address_id = dt.address_id

         LEFT JOIN dim_credit_card AS dcc
         ON ssoh.credit_card_id = dcc.credit_card_id

         LEFT JOIN dim_customer AS dc
         ON ssoh.customer_id = dc.customer_id

         LEFT JOIN dim_sales_reason AS dsr
         ON ssoh.sales_order_id = dsr.sales_order_id
         
         LEFT JOIN dim_sales_information AS dsi
         ON ssod.sales_order_id = dsi.sales_order_id)

 SELECT *
   FROM sales_with_sk