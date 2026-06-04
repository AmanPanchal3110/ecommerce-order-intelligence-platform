



select
    1
from ECOMMERCE.bronze.return_bronze

where not(refund_amount > 0)

