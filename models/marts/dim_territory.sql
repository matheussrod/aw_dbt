
    WITH staging_country_region AS
 (SELECT country_region_code,
         name
    FROM {{ ref('stg_country_region') }}),

         staging_state_province AS
 (SELECT state_province_id,
         state_province_code,
         country_region_code,
         name
    FROM {{ ref('stg_state_province') }}),

         staging_address AS
 (SELECT address_id,
         state_province_id,
         city,
         address_line1
    FROM {{ ref('stg_address') }}),

         staging AS
 (SELECT sa.address_id,
         scr.country_region_code,
         ssp.state_province_code,
         scr.name AS country,
         ssp.name AS state,
         sa.city,
         sa.address_line1 AS address
    FROM staging_country_region AS scr
         INNER JOIN staging_state_province AS ssp
         ON scr.country_region_code = ssp.country_region_code

         INNER JOIN staging_address AS sa
         ON ssp.state_province_id = sa.state_province_id),

         transformed AS 
 (SELECT ROW_NUMBER() OVER(ORDER BY address_id) AS address_sk,
         address_id,
         country_region_code,
         state_province_code,
         country,
         state,
         city,
         address
    FROM staging)

  SELECT *
    FROM transformed