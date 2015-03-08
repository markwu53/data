select count(*) from MarkSR.ext_nw_ims_raw;
select top 100 * from MarkSR.ext_nw_ims_raw;
select count(*) from MarkSR.sr_scrubbed_latest;
select top 100 * from MarkSR.sr_scrubbed_latest;
select count(*) from MarkSR.sr_canonical_latest;
select top 100 * from MarkSR.sr_canonical_latest;

select top 100 * from marksr.sre_trip_info_hbase 
where trip_start_timestamp not like '2014-07-08%'
and trip_start_timestamp not like '2014-07-07%'
order by trip_start_timestamp;

select substr(trip_start_timestamp, 1, 10) dd, count(*) cc 
from marksr.sre_trip_info_hbase
where substr(trip_start_timestamp, 1, 10) < '2014-07-01'
group by substr(trip_start_timestamp, 1, 10)
order by substr(trip_start_timestamp, 1, 10) desc;

select trip_vin from marksr.sre_trip_info_hbase
group by trip_vin;

select top 10 * from marksr.sre_trip_info_hbase;

with cte as (
select 
    substr(trip_start_timestamp, 1, 7) trip_month, 
    sum(trip_length_miles) miles_driven,
    sum(trip_hard_break_count) hard_break,
    sum(trip_fast_accel_count) fast_accel,
    sum(trip_night_time_drive_min) night_time
from marksr.sre_trip_info_hbase
where trip_vin='0000004'
group by substr(trip_start_timestamp, 1, 7)
)
select * from cte
where trip_month='2014-07'
;

select *
from marksr.sre_trip_info
;

with cte as (
select 
    substr(trip_start_timestamp, 1, 7) trip_month, 
    sum(trip_length_miles) miles_driven,
    sum(trip_hard_break_count) hard_break,
    sum(trip_fast_accel_count) fast_accel,
    sum(trip_night_time_drive_min) night_time
from marksr.sre_trip_info_hbase
where trip_vin='0000004'
group by substr(trip_start_timestamp, 1, 7)
)
select * from cte
where trip_month < '2014-08'
;

select * from (
select 
    substr(trip_start_timestamp, 1, 7) trip_month, 
    sum(trip_length_miles) miles_driven
from marksr.sre_trip_info_hbase
where trip_vin='0000004'
group by substr(trip_start_timestamp, 1, 7)
) cte
where trip_month='2014-07'
;

select substr(trip_vin, 6, 2), sum(trip_length_miles) miles_driven
from marksr.sre_trip_info_hbase
group by trip_vin
having trip_vin='0000004'
;

select substr(trip_start_timestamp, 1, 7) previous_month
from marksr.sre_trip_info_hbase
where trip_vin='0000004'
and substr(trip_start_timestamp, 1, 7)<'2014-08'
group by substr(trip_start_timestamp, 1, 7)
order by substr(trip_start_timestamp, 1, 7)
;

select top 100 * from marksr.sre_trip_info_hbase
;

with cte as (
                        select 
                                sum(trip_length_miles) miles_driven, 
                                sum(trip_hard_break_count) hard_break, 
                                sum(trip_fast_accel_count) fast_accel, 
                                sum(trip_night_time_drive_min) night_time 
                        from marksr.sre_trip_info_hbase 
                        where trip_vin = '0000004' 
                ) 
                select * from cte where trip_month = ?
;

select * from marksr.sre_vin_summary_hbase
;
                select top 1
                        miles as miles_driven,
                        hard_break_count as hard_break,
                        accel_count as fast_accel,
                        night_time as night_time
                from marksr.sre_vin_summary_hbase 
                where vin_number = '0000004'
                        and period_flg='T'
;
select *
           from marksr.sre_vin_summary_hbase
           where
            --vin_number = ?
                   --and 
                   period_flg = 'M'
;                
select *
           from marksr.sre_trip_info_hbase
           where trip_vin = '0000004'
           order by trip_start_timestamp desc
;              
select * from marksr.sre_trip_info_hbase
where trip_start_timestamp is null
;
select top 1
                        miles as miles_driven,
                        hard_break_count as hard_break,
                        accel_count as fast_accel,
                        night_time as night_time
                from marksr.sre_vin_summary_hbase 
                where vin_number = '0507011'
                        and period_flg = 'T'
;                        