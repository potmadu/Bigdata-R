setwd("F:/JSC/Transjakarta/Data/Tapinout/Batch_November_2017/");

library(lubridate);
library(dplyr);

list=20170904,20170905,20171015,20171101,20171105,20171106,20171109

tanggal=20171116;

tapinout = read.csv(paste(tanggal,".csv",sep=""),stringsAsFactors=FALSE);

###################################################
# CEK DATA
###################################################

tapinout$tanggal = dmy_hms(tapinout$tanggal_jam);
tapinout$jam = hour(tapinout$tanggal);

tapinout %>% 
group_by(flag,jam) %>% 
summarize(transaksi=n()) %>% 
as.data.frame();

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
plot(tapinout_smy_tapin$jam,tapinout_smy_tapin$transaksi,type="l");
plot(tapinout_smy_tapout$jam,tapinout_smy_tapout$transaksi,type="l");

tapinout$tanggal=NULL;
tapinout$jam=NULL;

###################################################
# TRANSFORMASI DATA
###################################################

tapinout$tanggal=NULL;
tapinout$jam=NULL;
tapinout$tanggal_jam_upload=NULL;
tapinout$koridor2 = gsub("\\]","",gsub("\\[","",gsub(" ","",substr(tapinout$koridor,1,4))));
tapinout$koridor2[tapinout$koridor2=="N"]="NBRT";
tapinout$koridor=tapinout$koridor2;
tapinout$koridor2=NULL;

write.csv(tapinout,paste(tanggal,"_clean.csv",sep=""),row.names=FALSE)

###################################################
#ETL
###################################################

## ON HIVE

LOAD DATA LOCAL INPATH '/home/bigsql/staging/20171115_clean.csv' INTO TABLE stg.transj_tapinout_hist PARTITION (ppn_dttm=20171115);

## ON OS

DATE_ID=20171116
PPN_DTTM=20171120

hadoop fs -mkdir /apps/hive/warehouse/sor.db/transj_tapinout/ppn_dttm=${PPN_DTTM};
hadoop fs -chmod 777 /apps/hive/warehouse/sor.db/transj_tapinout/ppn_dttm=${PPN_DTTM};
hive -e "alter table SOR.TRANSJ_TAPINOUT DROP IF EXISTS PARTITION (date_id = '${DATE_ID}', ppn_dttm='${PPN_DTTM}');"
hive -e "alter table SOR.TRANSJ_TAPINOUT add partition (date_id = '${DATE_ID}', ppn_dttm='${PPN_DTTM}') location '/apps/hive/warehouse/sor.db/transj_tapinout/ppn_dttm=${PPN_DTTM}';"
hive -e "INSERT OVERWRITE TABLE SOR.TRANSJ_TAPINOUT PARTITION (date_id = ${DATE_ID}, ppn_dttm = ${PPN_DTTM}) select from_unixtime(unix_timestamp(TRANSJ_TAPINOUT_HIST.TANGGAL_JAM,'dd-MM-yyyy hh:mm:ss'),'dd-MM-yyyy') as TANGGAL_TRANSAKSI, from_unixtime(unix_timestamp(TRANSJ_TAPINOUT_HIST.TANGGAL_JAM,'dd-MM-yyyy hh:mm:ss'),'hh:mm:ss') as JAM_TRANSAKSI, TRANSJ_TAPINOUT_HIST.TANGGAL_JAM as TANGGAL_JAM_TRANSAKSI, TRANSJ_TAPINOUT_HIST.TANGGAL_JAM as TANGGAL_MASUK, PAN as NO_PAN, KARTU, KORIDOR, SUBKORIDOR as SUB_KORIDOR, HALTE, FLAG as FLAG_INOUT FROM stg.TRANSJ_TAPINOUT_HIST where PPN_DTTM = ${DATE_ID};"
