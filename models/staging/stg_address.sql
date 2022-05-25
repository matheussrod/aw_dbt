
    WITH source_data AS
 (SELECT addressid AS address_id,
         stateprovinceid AS state_province_id,
         city,
         addressline1 AS address_line1,
         addressline2 AS address_line2,
         postalcode AS postal_code,
         spatiallocation AS spatial_location
         --modifieddate,
         --rowguid,
         --_sdc_received_at,
         --_sdc_sequence,
         --_sdc_batched_at,
         --_sdc_table_version
    FROM {{ source('aw_postgre', 'person_address') }})

  SELECT *
    FROM source_data