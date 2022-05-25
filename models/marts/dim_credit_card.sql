
    WITH staging AS
 (SELECT credit_card_id,
         card_type
    FROM {{ ref('stg_credit_card') }}),

         transformed AS
 (SELECT ROW_NUMBER() OVER(ORDER BY credit_card_id) AS credit_card_sk,
         credit_card_id,
         card_type
    FROM staging)

  SELECT *
    FROM transformed