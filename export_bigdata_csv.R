#Transj Tracking
hive -e 'select * from sor.transj_tracking where unix_timestamp(gpsdatetime, "yyyy-MM-dd hh:mm:ss") >= unix_timestamp("2017-02-20 09:00:00", "yyyy-MM-dd hh:mm:ss") and unix_timestamp(gpsdatetime, "yyyy-MM-dd hh:mm:ss") < unix_timestamp("2017-02-20 10:00:00", "yyyy-MM-dd hh:mm:ss")' | sed 's/[\t]/,/g'  > /home/bigsql/bigr/transj_tracking_2017022009_10.csv;

#Transj Tapinout
hive -e 'select * from sor.transj_tapinout where unix_timestamp(tanggal_jam_transaksi, "dd-MM-yyyy hh:mm:ss") >= unix_timestamp("2017-02-20 09:00:00", "dd-MM-yyyy hh:mm:ss") and unix_timestamp(tanggal_jam_transaksi, "dd-MM-yyyy hh:mm:ss") < unix_timestamp("2017-02-20 10:00:00", "dd-MM-yyyy hh:mm:ss")' | sed 's/[\t]/,/g'  > /home/bigsql/bigr/transj_tapinout_2017022009_10.csv;

################
hive -e "select * from sor.waze_jams WHERE PUBMILLIS>=${PUBMILLIS_START_DATE} AND PUBMILLIS<${PUBMILLIS_END_DATE}" | sed 's/[\t]/,/g'  > /home/bigsql/bigr/waze_jams_20170505.csv;

#!/bin/bash
hive -e "drop table if exists csv_dump;
create table csv_dump ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
LOCATION '/user/jsc/dump_csv' as
select street,city,line_x,line_y from (select distinct street,city,line_x,line_y from sor.waze_jams 
where city in ('Jakarta Selatan','Jakarta Utara','Jakarta Pusat','Jakarta Barat','Jakarta Timur') and (street is not NULL OR street <> '' OR length(street) > 0 )) t1;"

hadoop fs -getmerge /user/jsc/dump_csv /home/bigsql/bigr/street_coordinates_all_201705.csv

#!/bin/bash
hive -e "drop table if exists csv_dump;
create table csv_dump ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
LOCATION '/user/jsc/dump_csv' as
select distinct street,roadtype,city,line_x,line_y,delay,pubmillis from (select distinct street,roadtype,city,line_x,line_y,delay,pubmillis from sor.waze_jams 
where date_id>=20170509 and date_id<20170510 and city in ('Jakarta Selatan','Jakarta Utara','Jakarta Pusat','Jakarta Barat','Jakarta Timur')) t1;"

hadoop fs -getmerge /user/jsc/dump_csv /home/bigsql/bigr/jams_20170509.csv

#!/bin/bash
hive -e "drop table if exists csv_dump;
create table csv_dump ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
LOCATION '/user/jsc/dump_csv' as
select distinct street,line_x,line_y,roadtype,city from sor.waze_jams 
where city in ('Jakarta Selatan','Jakarta Utara','Jakarta Pusat','Jakarta Barat','Jakarta Timur');"

hadoop fs -getmerge /user/jsc/dump_csv /home/bigsql/bigr/wazejams_distinct_street_coordinates_roadtype_city.csv

hive -e "drop table if exists csv_dump;
create table csv_dump ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
LOCATION '/user/jsc/dump_csv' as
select buscode,gpsdatetime from sor.transj_tracking_v3
where date_id=20170617;"

hadoop fs -getmerge /user/jsc/dump_csv /home/bigsql/bigr/v3_20170617.csv

hive -e "drop table if exists csv_dump;
create table csv_dump ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
LOCATION '/user/jsc/dump_csv' as
select buscode,gpsdatetime from sor.transj_tracking5s 
where date_id=20170617;"

hadoop fs -getmerge /user/jsc/dump_csv /home/bigsql/bigr/5s_20170617.csv

hive -e "drop table if exists csv_dump;
create table csv_dump ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
LOCATION '/user/jsc/dump_csv' as
select * from sor.jamsgeofull 
where date_id<=20170407;"

hadoop fs -getmerge /user/jsc/dump_csv /home/bigsql/bigrjams_20170407.csv

#!/bin/bash
hive -e "drop table if exists csv_dump;
create table csv_dump ROW FORMAT DELIMITED
FIELDS TERMINATED BY '~' LINES TERMINATED BY '\n'
LOCATION '/user/jsc/dump_csv' as
select koridor,location,jam,count(distinct buscode) from (select hour(gpsdatetime) as jam,gpsdatetime,koridor,location,buscode from sor.transj_tracking_v3 where date_id=20170801)t1 
group by koridor,location,jam
order by koridor,location,jam asc;"

hadoop fs -getmerge /user/jsc/dump_csv /home/bigsql/bigr/tracking_v3_smy_20170801.csv

hive -e "drop table if exists csv_dump;
create table csv_dump ROW FORMAT DELIMITED
FIELDS TERMINATED BY '~' LINES TERMINATED BY '\n'
LOCATION '/user/jsc/dump_csv' as
select * from sor.transj_tapinout_api where date_id>=20170730;"

hadoop fs -getmerge /user/jsc/dump_csv /home/bigsql/bigr/tapinout_20170730_20170805.csv

hive -e "drop table if exists csv_dump;
create table csv_dump ROW FORMAT DELIMITED
FIELDS TERMINATED BY '~' LINES TERMINATED BY '\n'
LOCATION '/user/jsc/dump_csv' as
select * from sor.transj_tracking_v3 where date_id>=20170730 and date_id<=20170805 and location!='';"

hadoop fs -getmerge /user/jsc/dump_csv /home/bigsql/bigr/tracking_20170730_20170805.csv

hive -e "drop table if exists csv_dump;
create table csv_dump ROW FORMAT DELIMITED
FIELDS TERMINATED BY '~' LINES TERMINATED BY '\n'
LOCATION '/user/jsc/dump_csv' as
select * from sor.dataset_hackaton_stg1;"

hadoop fs -getmerge /user/jsc/dump_csv /home/bigsql/bigr/kemacetan_jakarta_20170701_201728.csv

hive -e "drop table if exists csv_dump;
create table csv_dump ROW FORMAT DELIMITED
FIELDS TERMINATED BY '~' LINES TERMINATED BY '\n'
LOCATION '/user/jsc/dump_csv' as
select * from sor.dataset_hackaton_stg1
where date_id>=20170729;"

hadoop fs -getmerge /user/jsc/dump_csv /home/bigsql/bigr/kemacetan_jakarta_20170729_201811.csv

hive -e "drop table if exists csv_dump;
create table csv_dump ROW FORMAT DELIMITED
FIELDS TERMINATED BY '~' LINES TERMINATED BY '\n'
LOCATION '/user/jsc/dump_csv' as
select * from sor.transj_tracking_v3
where date_id=20170814;"

hadoop fs -getmerge /user/jsc/dump_csv /home/bigsql/bigr/tracking_20170814.csv

#!/bin/bash
hive -e "drop table if exists csv_dump;
create table csv_dump ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
LOCATION '/user/jsc/dump_csv' as
select * from sor.waze_jams 
where date_id>=20170509 and date_id<20170510 and city in ('Jakarta Selatan','Jakarta Utara','Jakarta Pusat','Jakarta Barat','Jakarta Timur');"

hadoop fs -getmerge /user/jsc/dump_csv /home/bigsql/staging/jams_20170509_all.csv

#!/bin/bash
hive -e "drop table if exists csv_dump;
create table csv_dump ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
LOCATION '/user/jsc/dump_csv' as
select * from sor.waze_alerts 
where date_id>=20170509 and date_id<20170510 and city in ('Jakarta Selatan','Jakarta Utara','Jakarta Pusat','Jakarta Barat','Jakarta Timur');"

hadoop fs -getmerge /user/jsc/dump_csv /home/bigsql/staging/alerts_20170509_all.csv

#!/bin/bash
hive -e "drop table if exists csv_dump;
create table csv_dump ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
LOCATION '/user/jsc/dump_csv' as
select * from sor.ambulance
where date_id>=20171026;"

hadoop fs -getmerge /user/jsc/dump_csv /home/bigsql/staging/ambulance_20171026.csv

#!/bin/bash
hive -e "drop table if exists csv_dump;
create table csv_dump ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
LOCATION '/user/jsc/dump_csv' as
select * from sor.waze_alerts
where date_id>=20171023 and city in ('Jakarta Selatan','Jakarta Utara','Jakarta Pusat','Jakarta Barat','Jakarta Timur');"

hadoop fs -getmerge /user/jsc/dump_csv /home/bigsql/staging/alerts_20171023.csv

#!/bin/bash
hive -e "drop table if exists csv_dump;
create table csv_dump ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
LOCATION '/user/jsc/dump_csv' as
select * from sor.waze_alerts
where date_id>=20171001 and city in ('Jakarta Pusat');"

hadoop fs -getmerge /user/jsc/dump_csv /home/bigsql/staging/alerts_20171001_jakpus.csv

#!/bin/bash
hive -e "drop table if exists csv_dump;
create table csv_dump ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
LOCATION '/user/jsc/dump_csv' as
select distinct street,roadtype,city,line_x,line_y,delay,pubmillis from (select distinct street,roadtype,city,line_x,line_y,delay,pubmillis from sor.waze_jams 
where date_id>=20171001 and city in ('Jakarta Pusat')) t1;"

hadoop fs -getmerge /user/jsc/dump_csv /home/bigsql/staging/jams_20171001_jakpus.csv

#########################
### HDFS COPY TO LOCAL
#########################

hadoop fs -copyToLocal /hdfs/source/path /localfs/destination/path
hadoop fs -get /hdfs/source/path /localfs/destination/path

hadoop fs -copyToLocal /user/jsc/complaints/rop/ppn_dttm=20170602/rop_merge_20170602103135.json /home/bigsql/bigr/