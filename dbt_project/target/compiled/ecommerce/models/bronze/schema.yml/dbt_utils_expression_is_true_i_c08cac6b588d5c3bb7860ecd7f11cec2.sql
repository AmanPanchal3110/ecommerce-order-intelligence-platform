



select
    1
from ECOMMERCE.dbt.items_bronze

where not(event_timestamp <= current_timestamp())

