-- back compat for old kwarg name
  
  begin;
    
        
            
                
                
            
                
                
            
                
                
            
        
    

    

    merge into ECOMMERCE.silver.status_silver as DBT_INTERNAL_DEST
        using ECOMMERCE.silver.status_silver__dbt_tmp as DBT_INTERNAL_SOURCE
        on (
                    DBT_INTERNAL_SOURCE.order_id = DBT_INTERNAL_DEST.order_id
                ) and (
                    DBT_INTERNAL_SOURCE.order_status = DBT_INTERNAL_DEST.order_status
                ) and (
                    DBT_INTERNAL_SOURCE.status_timestamp = DBT_INTERNAL_DEST.status_timestamp
                )

    
    when matched then update set
        "EVENT_ID" = DBT_INTERNAL_SOURCE."EVENT_ID","ORDER_ID" = DBT_INTERNAL_SOURCE."ORDER_ID","CUSTOMER_ID" = DBT_INTERNAL_SOURCE."CUSTOMER_ID","EVENT_TIMESTAMP" = DBT_INTERNAL_SOURCE."EVENT_TIMESTAMP","INGESTION_TIMESTAMP" = DBT_INTERNAL_SOURCE."INGESTION_TIMESTAMP","ORDER_STATUS" = DBT_INTERNAL_SOURCE."ORDER_STATUS","PAYMENT_STATUS" = DBT_INTERNAL_SOURCE."PAYMENT_STATUS","STATUS_TIMESTAMP" = DBT_INTERNAL_SOURCE."STATUS_TIMESTAMP"
    

    when not matched then insert
        ("EVENT_ID", "ORDER_ID", "CUSTOMER_ID", "EVENT_TIMESTAMP", "INGESTION_TIMESTAMP", "ORDER_STATUS", "PAYMENT_STATUS", "STATUS_TIMESTAMP")
    values
        ("EVENT_ID", "ORDER_ID", "CUSTOMER_ID", "EVENT_TIMESTAMP", "INGESTION_TIMESTAMP", "ORDER_STATUS", "PAYMENT_STATUS", "STATUS_TIMESTAMP")

;
    commit;