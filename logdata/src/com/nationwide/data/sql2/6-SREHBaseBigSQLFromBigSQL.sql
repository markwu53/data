
 
LOAD USING FILE URL
  "/biginsights/hive/warehouse/alex_sr.db/sr_trip_info/sr_trip_info"
INTO HBASE TABLE alex_sr.sre_trip_info1_hbase APPEND
;


LOAD USING FILE URL
  "/biginsights/hive/warehouse/alex_sr.db/sr_vin_summary1"
INTO HBASE TABLE alex_sr.sre_vin_summary1_hbase APPEND
;

