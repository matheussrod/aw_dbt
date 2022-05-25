
    WITH source_data AS
 (SELECT salesreasonid AS sales_reason_id,
         reasontype AS reason_type,         
         name
         --modifieddate AS modified_date,
         --_sdc_sequence,
         --_sdc_table_version,
         --_sdc_received_at,
         --_sdc_batched_at,
    FROM {{ source('aw_postgre', 'sales_salesreason') }})

  SELECT *
    FROM source_data