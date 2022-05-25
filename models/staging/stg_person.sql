
    WITH source_data AS 
 (SELECT businessentityid AS business_entity_id,
         persontype AS person_type,
         namestyle AS name_style,
         title,
         firstname AS first_name,
         middlename AS middle_name,
         lastname AS last_name,
         suffix,
         emailpromotion AS email_promotion
         --modifieddate,
         --rowguid,
         --_sdc_table_version,
         --_sdc_received_at,
         --_sdc_sequence,
         --_sdc_batched_at,
    FROM {{ source('aw_postgre', 'person_person') }})

  SELECT *
    FROM source_data