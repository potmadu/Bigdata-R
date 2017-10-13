library(lubridate)

tapin20170921 = read.csv("F:/tapin20170921.csv",stringsAsFactors=FALSE);
tapin20170927 = read.csv("F:/tapin20170927.csv",stringsAsFactors=FALSE);

tapin20170921$tanggal=NULL;
tapin20170927$tanggal=NULL;
tapin20170921$jam=NULL;
tapin20170927$jam=NULL;
tapin20170921$tanggal_jam_upload=NULL;
tapin20170927$tanggal_jam_upload=NULL;

tapin20170921$koridor2 = gsub("\\]","",gsub("\\[","",gsub(" ","",substr(tapin20170921$koridor,1,4))));
tapin20170927$koridor2 = gsub("\\]","",gsub("\\[","",gsub(" ","",substr(tapin20170927$koridor,1,4))));

tapin20170921$koridor2[tapin20170921$koridor2=="N"]="NBRT";
tapin20170927$koridor2[tapin20170927$koridor2=="N"]="NBRT";

tapin20170921$koridor=tapin20170921$koridor2;
tapin20170927$koridor=tapin20170927$koridor2;

tapin20170921$koridor2=NULL;
tapin20170927$koridor2=NULL;

tapin20170921$tanggal = dmy_hms(tapin20170921$tanggal_jam);
tapin20170921$jam = hour(tapin20170921$tanggal);
tapin20170921 %>% group_by(flag,jam) %>% summarize(transaksi=n()) %>% as.data.frame();

write.csv(tapin20170921,"F:/tapin20170921",row.names=FALSE)
write.csv(tapin20170927,"F:/tapin20170927",row.names=FALSE)

LOAD DATA LOCAL INPATH '/home/bigsql/staging/tapin20170921.csv' INTO TABLE stg.transj_tapinout_hist PARTITION (ppn_dttm=20170921);
LOAD DATA LOCAL INPATH '/home/bigsql/staging/tapin20170927.csv' INTO TABLE stg.transj_tapinout_hist PARTITION (ppn_dttm=20170927);

DATE_ID=20170921
PPN_DTTM=20170925

hadoop fs -mkdir /apps/hive/warehouse/sor.db/transj_tapinout/ppn_dttm=${PPN_DTTM};
hadoop fs -chmod 777 /apps/hive/warehouse/sor.db/transj_tapinout/ppn_dttm=${PPN_DTTM};

hive -e "alter table SOR.TRANSJ_TAPINOUT DROP IF EXISTS PARTITION (date_id = '${DATE_ID}', ppn_dttm='${PPN_DTTM}');"
hive -e "alter table SOR.TRANSJ_TAPINOUT add partition (date_id = '${DATE_ID}', ppn_dttm='${PPN_DTTM}') location '/apps/hive/warehouse/sor.db/transj_tapinout/ppn_dttm=${PPN_DTTM}';"
hive -e "INSERT OVERWRITE TABLE SOR.TRANSJ_TAPINOUT PARTITION (date_id = ${DATE_ID}, ppn_dttm = ${PPN_DTTM}) select from_unixtime(unix_timestamp(TRANSJ_TAPINOUT_HIST.TANGGAL_JAM,'dd-MM-yyyy hh:mm:ss'),'dd-MM-yyyy') as TANGGAL_TRANSAKSI, from_unixtime(unix_timestamp(TRANSJ_TAPINOUT_HIST.TANGGAL_JAM,'dd-MM-yyyy hh:mm:ss'),'hh:mm:ss') as JAM_TRANSAKSI, TRANSJ_TAPINOUT_HIST.TANGGAL_JAM as TANGGAL_JAM_TRANSAKSI, TRANSJ_TAPINOUT_HIST.TANGGAL_JAM as TANGGAL_MASUK, PAN as NO_PAN, KARTU, KORIDOR, SUBKORIDOR as SUB_KORIDOR, HALTE, FLAG as FLAG_INOUT FROM stg.TRANSJ_TAPINOUT_HIST where PPN_DTTM = ${DATE_ID};"

DATE_ID=20170927
PPN_DTTM=20171001

hadoop fs -mkdir /apps/hive/warehouse/sor.db/transj_tapinout/ppn_dttm=${PPN_DTTM};
hadoop fs -chmod 777 /apps/hive/warehouse/sor.db/transj_tapinout/ppn_dttm=${PPN_DTTM};
hive -e "alter table SOR.TRANSJ_TAPINOUT DROP IF EXISTS PARTITION (date_id = '${DATE_ID}', ppn_dttm='${PPN_DTTM}');"
hive -e "alter table SOR.TRANSJ_TAPINOUT add partition (date_id = '${DATE_ID}', ppn_dttm='${PPN_DTTM}') location '/apps/hive/warehouse/sor.db/transj_tapinout/ppn_dttm=${PPN_DTTM}';"
hive -e "INSERT OVERWRITE TABLE SOR.TRANSJ_TAPINOUT PARTITION (date_id = ${DATE_ID}, ppn_dttm = ${PPN_DTTM}) select from_unixtime(unix_timestamp(TRANSJ_TAPINOUT_HIST.TANGGAL_JAM,'dd-MM-yyyy hh:mm:ss'),'dd-MM-yyyy') as TANGGAL_TRANSAKSI, from_unixtime(unix_timestamp(TRANSJ_TAPINOUT_HIST.TANGGAL_JAM,'dd-MM-yyyy hh:mm:ss'),'hh:mm:ss') as JAM_TRANSAKSI, TRANSJ_TAPINOUT_HIST.TANGGAL_JAM as TANGGAL_JAM_TRANSAKSI, TRANSJ_TAPINOUT_HIST.TANGGAL_JAM as TANGGAL_MASUK, PAN as NO_PAN, KARTU, KORIDOR, SUBKORIDOR as SUB_KORIDOR, HALTE, FLAG as FLAG_INOUT FROM stg.TRANSJ_TAPINOUT_HIST where PPN_DTTM = ${DATE_ID};"

