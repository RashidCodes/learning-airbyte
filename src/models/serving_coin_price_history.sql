drop table if exists {{ target_table }};

create table {{ target_table }} as 
	with price_movements as (

		select 
			*,
			coalesce(lag(price) over (partition by id order by "Date"), price) as prev_price 
		from public.staging_coin_price_history
	) 

	select *,
		concat(cast(round(((price - prev_price) / nullif(prev_price, 0)) * 100, 3) as varchar(10)), '%') as percent_increase
	from price_movements 