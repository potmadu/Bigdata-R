library(lubridate);
library(dplyr);

tapinout = read.csv("F:/tapin20170921.csv",stringsAsFactors=FALSE);

tapinout$tanggal=NULL;
tapinout$jam=NULL;
tapinout$tanggal_jam_upload=NULL;
tapinout$koridor2 = gsub("\\]","",gsub("\\[","",gsub(" ","",substr(tapinout$koridor,1,4))));
tapinout$koridor2[tapinout$koridor2=="N"]="NBRT";
tapinout$koridor=tapinout$koridor2;
tapinout$koridor2=NULL;
tapinout$tanggal = dmy_hms(tapinout$tanggal_jam);
tapinout$jam = hour(tapinout$tanggal);

tapinout %>% 
group_by(flag,jam) %>% 
summarize(transaksi=n()) %>% 
as.data.frame();

tapinout$tanggal = strptime(tapinout$tanggal_jam,"%m-%d-%Y %H:%M:%S")
tapinout$jam = strftime(tapinout$tanggal,"%H");

tapinout_smy_tapin = tapinout %>% 
select(-tanggal) %>% 
filter(flag=="In") %>%
group_by(flag,jam) %>% 
summarize(transaksi=n()) %>% 
as.data.frame();

tapinout_smy_tapout = tapinout %>% 
select(-tanggal) %>% 
filter(flag=="Out") %>%
group_by(flag,jam) %>% 
summarize(transaksi=n()) %>% 
as.data.frame();

par(mfrow=c(1,2));
plot(tapinout_smy_tapin$jam,tapin_smy_tapin$transaksi,type="l");
plot(tapinout_smy_tapout$jam,tapin_smy_tapout$transaksi,type="l");

write.csv(tapinout,"F:/tapin20170921.csv",row.names=FALSE)

###################################################

LOAD DATA LOCAL INPATH '/home/bigsql/staging/tapin20171012.csv' 
INTO TABLE stg.transj_tapinout_hist PARTITION (ppn_dttm=20171012);

DATE_ID=20171012
PPN_DTTM=20171016

hadoop fs -mkdir /apps/hive/warehouse/sor.db/transj_tapinout/ppn_dttm=${PPN_DTTM};
hadoop fs -chmod 777 /apps/hive/warehouse/sor.db/transj_tapinout/ppn_dttm=${PPN_DTTM};
hive -e "alter table SOR.TRANSJ_TAPINOUT DROP IF EXISTS PARTITION (date_id = '${DATE_ID}', ppn_dttm='${PPN_DTTM}');"
hive -e "alter table SOR.TRANSJ_TAPINOUT add partition (date_id = '${DATE_ID}', ppn_dttm='${PPN_DTTM}') location '/apps/hive/warehouse/sor.db/transj_tapinout/ppn_dttm=${PPN_DTTM}';"
hive -e "INSERT OVERWRITE TABLE SOR.TRANSJ_TAPINOUT PARTITION (date_id = ${DATE_ID}, ppn_dttm = ${PPN_DTTM}) select from_unixtime(unix_timestamp(TRANSJ_TAPINOUT_HIST.TANGGAL_JAM,'dd-MM-yyyy hh:mm:ss'),'dd-MM-yyyy') as TANGGAL_TRANSAKSI, from_unixtime(unix_timestamp(TRANSJ_TAPINOUT_HIST.TANGGAL_JAM,'dd-MM-yyyy hh:mm:ss'),'hh:mm:ss') as JAM_TRANSAKSI, TRANSJ_TAPINOUT_HIST.TANGGAL_JAM as TANGGAL_JAM_TRANSAKSI, TRANSJ_TAPINOUT_HIST.TANGGAL_JAM as TANGGAL_MASUK, PAN as NO_PAN, KARTU, KORIDOR, SUBKORIDOR as SUB_KORIDOR, HALTE, FLAG as FLAG_INOUT FROM stg.TRANSJ_TAPINOUT_HIST where PPN_DTTM = ${DATE_ID};"

