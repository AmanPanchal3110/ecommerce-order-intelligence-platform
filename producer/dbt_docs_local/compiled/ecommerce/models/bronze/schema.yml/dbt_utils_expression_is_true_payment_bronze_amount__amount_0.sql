



select
    1
from ECOMMERCE.dbt.payment_bronze

where not(amount amount > 0)

