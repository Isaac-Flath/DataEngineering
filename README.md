# DataEngineering
Repository where I keep stuff related to learning.


```sql
select 'customer_landing' as tbl, count(1) as record_count from customer_landing
union all
select 'accelerometer_landing' as tbl, count(1) as record_count from accelerometer_landing
union all
select 'step_trainer_landing' as tbl, count(1) as record_count from step_trainer_landing
union all
select 'customer_trusted' as tbl, count(1) as record_count from customer_trusted
union all
select 'accelerometer_trusted' as tbl, count(1) as record_count from accelerometer_trusted
union all
select 'step_trainer_trusted' as tbl, count(1) as record_count from step_trainer_trusted
union all
select 'customer_curated' as tbl, count(1) as record_count from customer_curated
union all
select 'machine_learning_curated' as tbl, count(1) as record_count from machine_learning_curated

select count(1) as record_count
from customer_landing
where sharewithresearchasofdate is null
```