



select
    1
from ECOMMERCE.bronze.payment_bronze

where not(event_timestamp <= current_timestamp())

