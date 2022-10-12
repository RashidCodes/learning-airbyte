drop table if exists {{ target_table }};

create table {{ target_table }} as 
	select 
		_airbyte_ab_id,
        _airbyte_data ->> 'ID' as ID,
		_airbyte_data ->> 'Age' as Age,
		_airbyte_data ->>  'Gender' as Gender,
		_airbyte_data ->>  'Cleanliness' as Cleanliness
    from public._airbyte_raw_happiness
	limit 10;