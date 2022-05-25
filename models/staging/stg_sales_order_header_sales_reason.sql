
    WITH source_data AS
 (SELECT salesorderid AS sales_order_id,
         salesreasonid AS sales_reason_id
         --modifieddate,
         --_sdc_sequence,
         --_sdc_table_version,
         --_sdc_received_at,
         --_sdc_batched_at,
    FROM {{ source('aw_postgre', 'sales_salesorderheadersalesreason') }})

  SELECT *
    FROM source_data