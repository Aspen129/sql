with daily_cost as (
	select campaign_id, date::date as day, cost, div0(sum(cost),sum(clicks)) as cpc,
	avg(sum(cost)) over(order by campaign_id, date::date rows 2 preceding) as avg_3_rows,
	avg(sum(cost)) over(order by campaign_id, date::date rows 6 preceding) as avg_7_rows,
	stddev(sum(cost)) over(order by campaign_id, date::date rows 6 preceding) as stdv_7_rows,
	avg(sum(cost)) over(order by campaign_id, date::date rows 13 preceding) as avg_14_rows,
	stddev(sum(cost)) over(order by campaign_id, date::date rows 13 preceding) as stdv_14_rows,
	avg(sum(cost)) over(order by campaign_id, date::date rows 29 preceding) as avg_30_rows,
	stddev(sum(cost)) over(order by campaign_id, date::date rows 29 preceding) as stdv_30_rows,
	avg(div0(sum(cost),sum(clicks))) over(order by campaign_id, date::date rows 2 preceding) as cpc_avg_3_rows,
	avg(div0(sum(cost),sum(clicks))) over(order by campaign_id, date::date rows 6 preceding) as cpc_avg_7_rows,
	stddev(div0(sum(cost),sum(clicks))) over(order by campaign_id, date::date rows 6 preceding) as cpc_stdv_7_rows,
	avg(div0(sum(cost),sum(clicks))) over(order by campaign_id, date::date rows 13 preceding) as cpc_avg_14_rows,
	stddev(div0(sum(cost),sum(clicks))) over(order by campaign_id, date::date rows 13 preceding) as cpc_stdv_14_rows,
	avg(div0(sum(cost),sum(clicks))) over(order by campaign_id, date::date rows 29 preceding) as cpc_avg_30_rows,
	stddev(div0(sum(cost),sum(clicks))) over(order by campaign_id, date::date rows 29 preceding) as cpc_stdv_30_rows,
	row_number() over (partition by campaign_id order by  date::date) as row_number
	from dbt.campaign_performance
	where year(date::date) >= 2021 and date::date between current_date -90 and current_date
	group by 1,2,3
),
moving_avg as (
	select campaign_id, day, cost, cpc,
	lag(avg_3_rows,1) over(partition by campaign_id order by day asc) as cost_3_day_avg,
	lag(avg_7_rows,1) over(partition by campaign_id order by day asc) as cost_7_day_avg,
	lag(stdv_7_rows,1) over(partition by campaign_id order by day asc) as cost_7_day_stdv,
	lag(avg_14_rows,1) over(partition by campaign_id order by day asc) as cost_14_day_avg,
	lag(stdv_14_rows,1) over(partition by campaign_id order by day asc) as cost_14_day_stdv,
	lag(avg_30_rows,1) over(partition by campaign_id order by day asc) as cost_30_day_avg,
	lag(stdv_30_rows,1) over(partition by campaign_id order by day asc) as cost_30_day_stdv,
	lag(cpc_avg_3_rows,1) over(partition by campaign_id order by day asc) as cpc_3_day_avg,
	lag(cpc_avg_7_rows,1) over(partition by campaign_id order by day asc) as cpc_7_day_avg,
	lag(cpc_stdv_7_rows,1) over(partition by campaign_id order by day asc) as cpc_7_day_stdv,
	lag(cpc_avg_14_rows,1) over(partition by campaign_id order by day asc) as cpc_14_day_avg,
	lag(cpc_stdv_14_rows,1) over(partition by campaign_id order by day asc) as cpc_14_day_stdv,
	lag(cpc_avg_30_rows,1) over(partition by campaign_id order by day asc) as cpc_30_day_avg,
	lag(cpc_stdv_30_rows,1) over(partition by campaign_id order by day asc) as cpc_30_day_stdv,
	row_number
	from daily_cost
)
select campaign_id, name, day, week(day) week, cost, cpc, cost_3_day_avg, cost_7_day_avg, cost_7_day_stdv, cost_14_day_avg, cost_14_day_stdv, cost_30_day_avg, cost_30_day_stdv,
	   cpc_3_day_avg, cpc_7_day_avg, cpc_7_day_stdv, cpc_14_day_avg, cpc_14_day_stdv, cpc_30_day_avg, cpc_30_day_stdv,
	   abs(div0((cost_3_day_avg - cost_30_day_avg),cost_30_day_stdv)) cost_z_score, abs(div0((cpc_3_day_avg - cpc_30_day_avg),cpc_30_day_stdv)) cpc_z_score,
		(case
		 when abs(div0((cost_3_day_avg - cost_30_day_avg),cost_30_day_stdv)) > 1.9 then 'watch'
		 else 'in_range' end) as cost_outliers,
		(case
		 when abs(div0((cpc_3_day_avg - cpc_30_day_avg),cpc_30_day_stdv)) > 1.9 then 'watch'
		 else 'in_range' end) as cpc_outliers
from moving_avg
join dbt.campaign_names on moving_avg.campaign_id = campaign_names.id
where row_number > 30