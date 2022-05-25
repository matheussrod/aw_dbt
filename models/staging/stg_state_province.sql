
    WITH source_data AS
 (SELECT stateprovinceid AS state_province_id,
         stateprovincecode AS state_province_code,
         countryregioncode AS country_region_code,
         isonlystateprovinceflag AS is_only_state_province_flag,
         name
         --modifieddate,
         --rowguid,
         --_sdc_table_version,
         --territoryid,
         --_sdc_received_at,
         --_sdc_sequence,
         --_sdc_batched_at,
    FROM {{ source('aw_postgre', 'person_stateprovince') }})

  SELECT *
    FROM source_data