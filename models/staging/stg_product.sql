
    WITH source_data AS 
 (SELECT productid AS product_id,
         name,
         productnumber AS product_number,
         makeflag AS make_flag,
         finishedgoodsflag AS finished_goods_flag,
         color,
         safetystocklevel AS safety_stock_level,
         reorderpoint AS reorder_point,
         standardcost AS standard_cost,
         listprice AS list_price,
         size,
         sizeunitmeasurecode AS size_unit_measure_code,
         weightunitmeasurecode AS weight_unit_measure_code,
         weight,
         daystomanufacture AS days_to_manufacture,
         productline AS product_line,
         class,
         style,
         productsubcategoryid AS product_subcategory_id,
         productmodelid AS product_model_id,
         sellstartdate AS sell_start_date,
         sellenddate AS sell_end_date
         --modifieddate,
         --rowguid,
         --_sdc_table_version,
         --_sdc_received_at,
         --_sdc_sequence,
         --_sdc_batched_at,
    FROM {{ source('aw_postgre', 'production_product') }})

  SELECT *
    FROM source_data