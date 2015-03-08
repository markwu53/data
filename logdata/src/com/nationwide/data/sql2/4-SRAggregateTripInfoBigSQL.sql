
Create table alex_sr.sr_trip_minute_aggregates
(trip_number, start_timestamp, end_timestamp, accel_count, hard_break_count, night_drive_sec, miles_driven, speed_in_mph)
row format delimited fields terminated by ','
as 
with trip_minute_procesed_info as
(
Select trip_number_s as trip_number, 
position_timestamp_s as start_timestamp, lead(position_timestamp_s, 1) OVER (PARTITION BY trip_number_s) as end_timestamp, 
vss_scrubbed_speed_s as start_speed, lead(vss_scrubbed_speed_s, 1) OVER (PARTITION BY trip_number_s) as end_speed,
if ((lead(vss_scrubbed_speed_s, 1) OVER (PARTITION BY trip_number_s)) - vss_scrubbed_speed_s > 25, 1, 0) as fast_accel_flg,
if ((lead(vss_scrubbed_speed_s, 1) OVER (PARTITION BY trip_number_s)) - vss_scrubbed_speed_s < -25, 1, 0) as hard_break_flg,
if (position_timestamp_s >= 0 and position_timestamp_s <= 6, 1, 0) as night_drive_flg,
vss_scrubbed_speed_s/3600 as miles_driven
from alex_sr.sr_scrubbed_latest
)
select trip_number, min(start_timestamp) as start_time_minute, max(end_timestamp) as end_time_minute, 
sum(fast_accel_flg) as fast_accel_minute_count,  sum(hard_break_flg) as har_break_minute_count, 
sum(night_drive_flg) as night_drive_sec,  sum(miles_driven) as miles_driven_minute_count,
max(end_speed) as minute_speed_in_mph
from trip_minute_procesed_info 
group by trip_number, minute(end_timestamp)
;


Create table alex_sr.sr_trip_info
(vin_number, trip_number, trip_start_timestamp, trip_end_timestamp,
trip_start_day, trip_start_hour, trip_start_min,
trip_end_day, trip_end_hour, trip_end_min,
duration, miles_driven, kms_driven, hard_break_count, fast_accel_count,
night_drive_min, trip_minute_details_json)
row format delimited fields terminated by ','
as
with trip_procesed_info as
(
select trip_number, min(start_timestamp) as trip_start_time, max(end_timestamp) as trip_end_time, 
sum(accel_count) as fast_accel_trip_count,  sum(hard_break_count) as hard_break_trip_count, 
sum(night_drive_sec) as night_drive_trip_sec,  
sum(miles_driven) as km_driven_trip,
sum(miles_driven)/1.7 as miles_driven_trip,
'dummy_event_json_string' as trip_detail_json_string
from alex_sr.sr_trip_minute_aggregates 
group by trip_number
)
select 
substr(trip_number, 1, 7) as trip_number, substr(trip_number, 8) as vin_number, 
trip_start_time, trip_end_time,
day(trip_start_time) as trip_start_day, 
hour(trip_start_time) as trip_start_hour, 
minute(trip_start_time) as trip_start_min, 
day(trip_end_time) as trip_end_day, 
hour(trip_end_time) as trip_end_hour, 
minute(trip_end_time) as trip_end_min, 
minutes_between(trip_end_time, trip_start_time),
miles_driven_trip, km_driven_trip, hard_break_trip_count, fast_accel_trip_count, night_drive_trip_sec/60,
trip_detail_json_string
from trip_procesed_info
;



