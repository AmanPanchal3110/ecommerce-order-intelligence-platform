



select
    1
from ECOMMERCE.bronze.status_bronze

where not(event_timestamp <= current_timestamp())

