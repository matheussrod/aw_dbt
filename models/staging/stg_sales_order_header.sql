
    WITH source_data AS
 (SELECT salesorderid AS sales_order_id,
         customerid AS customer_id,
         salespersonid AS sales_person_id,
         territoryid AS territory_id,
         billtoaddressid AS bill_to_address_id,
         shiptoaddressid AS ship_to_address_id,
         shipmethodid AS ship_method_id,
         creditcardid AS credit_card_id,
         currencyrateid AS currency_rate_id,
         revisionnumber AS revision_number,
         orderdate AS order_date,
         duedate AS due_date,
         shipdate AS ship_date,
         status,
         onlineorderflag AS online_order_flag,
         purchaseordernumber AS purchase_order_number,
         accountnumber AS account_number,
         creditcardapprovalcode AS credit_card_approval_code,
         subtotal AS sub_total,
         taxamt,
         freight,
         totaldue AS total_due
         --modifieddate,
         --rowguid,
         --_sdc_table_version,
         --_sdc_received_at,
         --_sdc_sequence,
         --_sdc_batched_at,
    FROM {{ source('aw_postgre', 'sales_salesorderheader') }})

  SELECT *
    FROM source_data