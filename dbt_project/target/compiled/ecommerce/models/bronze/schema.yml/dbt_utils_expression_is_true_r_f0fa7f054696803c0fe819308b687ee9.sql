



select
    1
from ECOMMERCE.dbt.return_bronze

where not(refund_amount refund_amount > 0)

