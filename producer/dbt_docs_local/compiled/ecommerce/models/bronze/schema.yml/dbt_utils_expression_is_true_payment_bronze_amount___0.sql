



select
    1
from ECOMMERCE.bronze.payment_bronze

where not(amount > 0)

