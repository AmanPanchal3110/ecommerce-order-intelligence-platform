



select
    1
from ECOMMERCE.dbt.payment_bronze

where not(event_timestamp event_timestamp <= current_timestamp())

