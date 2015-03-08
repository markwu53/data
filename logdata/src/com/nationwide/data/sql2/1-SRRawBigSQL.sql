create external table alex_sr.ext_nw_ims_raw (
        trip_number string
        , position_timestamp timestamp
        , gps_latitude double
        , gps_longitude double
        , gps_heading double
        , vss_speed double
        , vss_acceleration double
        , engine_rpm double
        , positional_quality string
        , throttle_position string
        , accel_longitudinal double
        , accel_lateral double
        , accel_vertical double)
row format delimited fields terminated by ','
location '/user/biadmin/SR_';
