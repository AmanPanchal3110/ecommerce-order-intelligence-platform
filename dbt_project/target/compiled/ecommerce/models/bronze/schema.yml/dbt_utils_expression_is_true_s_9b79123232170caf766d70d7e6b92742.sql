



select
    1
from ECOMMERCE.dbt.status_bronze

where not(event_timestamp event_timestamp <= current_timestamp())

