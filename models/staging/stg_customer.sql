
    WITH source_data AS
 (SELECT customerid AS customer_id,
         personid AS person_id,
         storeid AS store_id,
         territoryid AS territory_id
         --modifieddate,
         --rowguid,
         --_sdc_table_version,
         --_sdc_received_at,
         --_sdc_sequence,
         --_sdc_batched_at,
    FROM {{ source('aw_postgre', 'sales_customer') }})

  SELECT *
    FROM source_data