add jar /home/biadmin/mylib/csv-serde-1.1.2.jar;
add jar /home/biadmin/mylib/ihs-log-serde.jar;

--create schema logdata;
--use logdata;

create table logdata.netflow_log (
    Flow_Type string 
    , First_Packet_Time string 
    , Storage_Time string 
    , Source_IP string 
    , Source_Port string 
    , Destination_IP string 
    , Destination_Port string 
    , Source_Bytes string 
    , Destination_Bytes string 
    , Total_Bytes string 
    , Source_Packets string 
    , Destination_Packets string 
    , Total_Packets string 
    , Protocol string 
    , Application string 
    , ICMP_Type_Code string 
    , Source_Flags string 
    , Destination_Flags string 
    , Source_QoS string 
    , Destination_QoS string 
    , Flow_Source string 
    , Flow_Interface string 
    , Source_If_Index string 
    , Destination_If_Index string 
    , Source_ASN string 
    , Destination_ASN string 
)
partitioned by (App string, Path string) 
row format serde 'com.bizo.hive.serde.csv.CSVSerde'
;

create table logdata.ihs_access_log (
    Ip_Address string
    , Port string
    , Process string
    , Access_time string
    , Request_Method string
    , Request_Path string
    , Http_Version string
    , Response_Status string
    , Response_Bytes string
)
partitioned by (App string, Path string) 
row format serde 'com.nw.ihslog.serde.IhsLogSerDe'
;

create table logdata.aroc (
    Timestamp string 
    , Client_Ip string 
    , Server_Ip string 
    , Network_Protocol string 
    , DB_User_Name string 
    , OS_User string 
    , Source_Program string 
    , Service_Name string 
    , Object_name string 
    , SQL_Verb string 
    , Sql string 
)
row format serde 'com.bizo.hive.serde.csv.CSVSerde'
;

----------------------------------------------------
-- below are program generated
----------------------------------------------------

load data local inpath '/home/biadmin/Files10000/RSC/IHS_Logs/access-log17-isc-117-126' into table logdata.ihs_access_log partition (App='RSC', Path='access-log17-isc-117-126');
load data local inpath '/home/biadmin/Files10000/RSC/IHS_Logs/access-log16-isc-117-126' into table logdata.ihs_access_log partition (App='RSC', Path='access-log16-isc-117-126');
load data local inpath '/home/biadmin/Files10000/RSC/IHS_Logs/access-log16-ssc-117-126' into table logdata.ihs_access_log partition (App='RSC', Path='access-log16-ssc-117-126');
load data local inpath '/home/biadmin/Files10000/RSC/IHS_Logs/access-log17-ssc-117-126' into table logdata.ihs_access_log partition (App='RSC', Path='access-log17-ssc-117-126');
load data local inpath '/home/biadmin/Files10000/RSC/Netflow/82-85-117-126.csv' into table logdata.netflow_log partition (App='RSC', Path='82-85-117-126.csv');
load data local inpath '/home/biadmin/Files10000/RSC/Netflow/105 -.106 117-126 flows.csv' into table logdata.netflow_log partition (App='RSC', Path='105 -.106 117-126 flows.csv');
load data local inpath '/home/biadmin/Files10000/RSC/Netflow/nlvmw0016 .17-117-126.csv' into table logdata.netflow_log partition (App='RSC', Path='nlvmw0016 .17-117-126.csv');
load data local inpath '/home/biadmin/Files10000/RSC/Netflow/nlvmw0017.25-117-126.csv' into table logdata.netflow_log partition (App='RSC', Path='nlvmw0017.25-117-126.csv');
load data local inpath '/home/biadmin/Files10000/RSC/Netflow/146 117-126.csv' into table logdata.netflow_log partition (App='RSC', Path='146 117-126.csv');
load data local inpath '/home/biadmin/Files10000/AAC/IHS_Logs/nlvmw0023/nlvmw0023-2.access.log' into table logdata.ihs_access_log partition (App='AAC', Path='nlvmw0023/nlvmw0023-2.access.log');
load data local inpath '/home/biadmin/Files10000/AAC/IHS_Logs/nlvmw0023/nlvmw0023.access.log' into table logdata.ihs_access_log partition (App='AAC', Path='nlvmw0023/nlvmw0023.access.log');
load data local inpath '/home/biadmin/Files10000/AAC/IHS_Logs/nlvmw0022/nlvmw0022-2.access.logs' into table logdata.ihs_access_log partition (App='AAC', Path='nlvmw0022/nlvmw0022-2.access.logs');
load data local inpath '/home/biadmin/Files10000/AAC/IHS_Logs/nlvmw0022/nlvmw0022.access.logs' into table logdata.ihs_access_log partition (App='AAC', Path='nlvmw0022/nlvmw0022.access.logs');
load data local inpath '/home/biadmin/Files10000/AAC/Netflow/123-134Flows-117-126.csv' into table logdata.netflow_log partition (App='AAC', Path='123-134Flows-117-126.csv');
load data local inpath '/home/biadmin/Files10000/AAC/Netflow/nlvmw0022 .5-117-126.csv' into table logdata.netflow_log partition (App='AAC', Path='nlvmw0022 .5-117-126.csv');
load data local inpath '/home/biadmin/Files10000/AAC/Netflow/nlvmw0023 .6-117-126.csv' into table logdata.netflow_log partition (App='AAC', Path='nlvmw0023 .6-117-126.csv');
load data local inpath '/home/biadmin/Files10000/AAC/Netflow/7-.8-117-126-flow.csv' into table logdata.netflow_log partition (App='AAC', Path='7-.8-117-126-flow.csv');
load data local inpath '/home/biadmin/Files10000/AROC/aroc.csv' into table logdata.aroc;
