



Create table alex_sr.sr_vin_summary_latest1
(vin_number, period_flg, period, 
miles, miles_delta, night_time, night_time_delta, 
accel_count, accel_count_delta, hard_break_count, hard_break_count_delta
)
row format delimited fields terminated by ','
as
with 
vin_day_summary as
(
select 
vin_number, year(trip_start_timestamp) as yr, month(trip_start_timestamp) as mnth, day(trip_start_timestamp) as dy,
sum(miles_driven) as miles, sum(night_drive_min) as night_time, 
sum(fast_accel_count) as accel_count, sum(hard_break_count) as hard_break_count
from alex_sr.sr_trip_info 
group by vin_number, year(trip_start_timestamp), month(trip_start_timestamp), day(trip_start_timestamp)
) ,
vin_month_summary as
(
select 
vin_number, year(trip_start_timestamp) as yr, month(trip_start_timestamp) as mnth,
sum(miles_driven) as miles, sum(night_drive_min) as night_time, 
sum(fast_accel_count) as accel_count, sum(hard_break_count) as hard_break_count
from alex_sr.sr_trip_info 
group by vin_number, year(trip_start_timestamp), month(trip_start_timestamp)
) ,
vin_total_summary as
(
select 
vin_number,
sum(miles_driven) as miles, sum(night_drive_min) as night_time, 
sum(fast_accel_count) as accel_count, sum(hard_break_count) as hard_break_count
from alex_sr.sr_trip_info 
group by vin_number
) ,
vin_all_summary as
(
select vin_number, 'D' as period_flg, concat(concat(cast(yr as char(4)), cast(mnth as char(2))), cast(dy as char(2))) as period,   
miles, night_time, accel_count, hard_break_count
from vin_day_summary
union
select vin_number, 'M' as period_flg, concat(cast(yr as char(4)), cast(mnth as char(2))) as period,   
miles, night_time, accel_count, hard_break_count
from vin_month_summary
union
select vin_number, 'T' as period_flg, 'Total' as period,   
miles, night_time, accel_count, hard_break_count
from vin_total_summary
) ,
vin_all_consolidated as
(
select 
if(l.vin_number is NULL, c.vin_number, l.vin_number) as vin_number,
if(l.period_flg is NULL, c.period_flg, l.period_flg) as period_flg,
if(l.period is NULL, c.period, l.period) as period,
(if(l.miles is null, 0, l.miles) + if(c.miles is null, 0, c.miles)) as total_miles,
(if(l.night_time is null, 0, l.night_time) + if(c.night_time is null, 0, c.night_time)) as total_night_time,
(if(l.accel_count is null, 0, l.accel_count) + if(c.accel_count is null, 0, c.accel_count)) as total_accel_count,if(l.accel_count is null, 0, l.accel_count) as accel_count_delta,
(if(l.hard_break_count is null, 0, l.hard_break_count) + if(c.hard_break_count is null, 0, c.hard_break_count)) as total_hard_break_count
from vin_all_summary as l full outer join alex_sr.sr_vin_summary1 c
on 
l.vin_number = c.vin_number
and l.period_flg = c.period_flg
and l.period = c.period
)
select 
vin_number, period_flg, period, 
total_miles, (total_miles - lag(total_miles, 1) OVER (PARTITION BY vin_number order by period_flg, period)) as miles_delta,
total_night_time, (total_night_time - lag(total_night_time, 1) OVER (PARTITION BY vin_number order by period_flg, period)) as total_night_time_delta,
total_accel_count, (total_accel_count - lag(total_accel_count, 1) OVER (PARTITION BY vin_number order by period_flg, period)) as total_accel_count_delta,
total_hard_break_count, (total_hard_break_count - lag(total_hard_break_count, 1) OVER (PARTITION BY vin_number order by period_flg, period)) as total_hard_break_count_delta
from vin_all_consolidated
;

drop table alex_sr.sr_vin_summary1;

alter table alex_sr.sr_vin_summary_latest1 rename to alex_sr.sr_vin_summary1;

