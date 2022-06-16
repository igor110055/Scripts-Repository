USE analytical;
DROP TABLE IF EXISTS a_log_analise_full; 
CREATE EXTERNAL TABLE a_log_analise_full (
no_cpf string, 
no_telefone string, 
dt_execucao string, 
ds_job string, 
cd_session string, 
ds_sistema_origem string, 
tp_transacao string, 
vl_ratingfraudefull string, 
qtd_ratingsim string, 
vl_ratingfinanceira string, 
ds_ratingfinal string, 
ds_statusdecisaofull string, 
ds_status string, 
ds_motivos string, 
ds_decisao_case_manager string, 
qtd_clearsalescoredti string, 
fl_serasanegativospc string, 
fl_fraudehist string, 
fl_preratingsimpem string, 
fl_cadusqtderestrativcli string, 
fl_cadusvlrtotalrestrativcli string, 
fl_ccsegmentacaoprimaria string, 
fl_ccsegmentacaosecundaria string, 
fl_ccvlrinvestimento string, 
fl_correntista string, 
fl_rendabalde string, 
fl_func string
)
PARTITIONED BY (dt_referencia string)
ROW FORMAT SERDE
'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES ('SERIALIZATION.ENCODING'='UTF-8')
STORED AS INPUTFORMAT'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
'abfs://azhdisimuseprd@azsimdtlkprd.dfs.core.windows.net/data/analytical/a_log_analise_full/';