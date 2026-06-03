



select
    1
from ECOMMERCE.dbt.return_bronze

where not(event_timestamp <= current_timestamp())

