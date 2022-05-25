
   WITH source_data AS
 (SELECT creditcardid AS credit_card_id,
         cardtype AS card_type,
         cardnumber AS card_number,
         expmonth AS exp_month,
         expyear AS exp_year
         --modifieddate,
         --_sdc_table_version,
         --_sdc_received_at,
         --_sdc_sequence,
         --_sdc_batched_at,
    FROM {{ source('aw_postgre', 'sales_creditcard') }})

  SELECT *
    FROM source_data