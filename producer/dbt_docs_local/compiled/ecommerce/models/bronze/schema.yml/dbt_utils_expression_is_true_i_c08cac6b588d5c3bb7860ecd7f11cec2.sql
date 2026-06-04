



select
    1
from ECOMMERCE.bronze.items_bronze

where not(event_timestamp <= current_timestamp())

