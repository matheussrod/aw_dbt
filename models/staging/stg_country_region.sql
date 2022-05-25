
    WITH source_data AS 
 (SELECT countryregioncode AS country_region_code,
         name
         --modifieddate,
         --_sdc_sequence,
         --_sdc_table_version,
         --_sdc_received_at,
         --_sdc_batched_at
    FROM {{ source('aw_postgre', 'person_countryregion') }})

  SELECT *
    FROM source_data

