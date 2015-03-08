drop table MarkSR.sr_canonical_latest;

create table MarkSR.sr_canonical_latest (trip_number_c
        , position_timestamp_c
        , gps_latitude_c
        , gps_longitude_c
        , gps_heading_c
        , vss_speed_c
        , vss_acceleration_c
        , engine_rpm_c
        , positional_quality_c
        , throttle_position_c
        , accel_longitudinal
        , accel_lateral
        , accel_vertical
        , contract_number_c
        , voucher_number_c
        , odometer_reading_c
        , event_average_speed_c
        , event_average_acceleration_c
        , distance_travelled_c
        , time_elapsed_c
        , gps_point_speed_c
        , road_tp_c)
row format delimited fields terminated by ','
as
select trip_number, position_timestamp, gps_latitude, gps_longitude, gps_heading,
									vss_speed, vss_acceleration, 
									if (engine_rpm < 0, 0, engine_rpm) as engine_rpm_c, 
									positional_quality, throttle_position,
									accel_longitudinal, accel_lateral, accel_vertical,
									'dummy_contract_number', 'dummy_voucher_number', cast(0.0 as double), 0, 0, 0, 0, 0, 'dummy_road_tp'
from MarkSR.ext_nw_ims_raw
;
							
							
select count(*) from MarkSR.sr_canonical_latest;
