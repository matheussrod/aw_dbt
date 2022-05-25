
    WITH staging_customer AS
 (SELECT customer_id,
         person_id,
         store_id
    FROM {{ ref('stg_customer') }}),

         staging_person AS
 (SELECT business_entity_id,
         person_type,
         title,
         first_name,
         middle_name,
         last_name,
         suffix
    FROM {{ ref('stg_person') }}),

         staging AS
 (SELECT sc.customer_id,
         --sp.person_type AS customer_type,
         --CASE WHEN sp.person_type = 'SC' THEN 'Store Contact'
         --     WHEN sp.person_type = 'IN' THEN 'Individual (retail)'
         --     WHEN sp.person_type = 'SP' THEN 'Sales person'
         --     WHEN sp.person_type = 'EM' THEN 'Employee (non-sales)'
         --     WHEN sp.person_type = 'VC' THEN 'Vendor contact'
         --     WHEN sp.person_type = 'GC' THEN 'General contact'
         --      END AS customer_person_type_desc,
         TRIM(CONCAT(sp.title, ' ', sp.first_name, ' ', sp.middle_name, ' ', sp.last_name, ' ', sp.suffix)) AS customer_person
    FROM staging_customer AS sc
         LEFT JOIN staging_person AS sp
         ON sc.person_id = sp.business_entity_id),

         transformed AS
 (SELECT ROW_NUMBER() OVER(ORDER BY customer_id) AS customer_sk,
         customer_id,
         customer_person
    FROM staging
   WHERE customer_person IS NOT NULL)

  SELECT *
    FROM transformed