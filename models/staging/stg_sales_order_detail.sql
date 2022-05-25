
    WITH source_data AS
 (SELECT salesorderid AS sales_order_id,
         salesorderdetailid AS sales_order_detail_id,
         productid AS product_id,
         specialofferid AS specialoffer_id,
         carriertrackingnumber AS carrier_tracking_number,
         orderqty AS order_qty,
         unitprice AS unit_price,
         unitpricediscount AS unit_price_discount
         --modifieddate,
         --rowguid,
         --_sdc_table_version,
         --_sdc_received_at,
         --_sdc_sequence,
         --_sdc_batched_at,
    FROM {{ source('aw_postgre', 'sales_salesorderdetail') }})

  SELECT *
    FROM source_data