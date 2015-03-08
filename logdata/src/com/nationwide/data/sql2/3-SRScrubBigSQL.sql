create table alex_sr.sr_scrubbed_latest (
        trip_number_s
        , position_timestamp_s
        , vss_speed_s
        , vss_scrubbed_speed_s
        , implausible_flg_s
        , scrubbed_record_flg_s)
row format delimited fields terminated by ','
as
select
        trip_number_c
        , position_timestamp_c
        , vss_speed_c
        , if (vss_speed_c < 0, 0, vss_speed_c)
        , 'N'
        , 'N'
from alex_sr.sr_canonical_latest
;



								