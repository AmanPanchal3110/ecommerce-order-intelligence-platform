



select
    1
from ECOMMERCE.bronze.return_bronze

where not(event_timestamp <= current_timestamp())

