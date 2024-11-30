Here's an example of a possible query language that is inspired by SQL but just changes the order of elements slightly to be more composable.

```sql
from table
where quantity < 10
group by store_id get (
  with profit as quantity * price - cost
  select count(*), sum(profit), median(profit)
)

-- Top 5 products with highest profit per store
from table
with profit as quantity * price - cost
where quantity < 10
group by store_id get (
  group by product_id get sum(profit) as profit
  order by profit desc
  first 5
)
augment with (
  group by store_id get sum(profit) as store_profit
) as total_sales on store_id = total_sales.store_id
order by store_profit desc
select store_id, product_id, profit

-- stores ordered by profit
from table
with profit as quantity * price - cost
group by store_id get sum(profit) as profit
order by profit desc
select store_id, profit

-- multiple tables in response
with profit_tbl as (
  from table
  with profit as quantity * price - cost
)

with tbl_a as (from table)
with tbl_b as (from table)

return tbl_a, tbl_b
```
