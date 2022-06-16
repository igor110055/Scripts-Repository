use harmonized;
DROP TABLE IF EXISTS h_fico_clearsale_ffr_output_historico;
CREATE EXTERNAL TABLE `h_fico_clearsale_ffr_output_historico`(
  `no_cpf` string, 
  `no_telefone` string, 
  `dt_execucao` string, 
  `ds_job` string, 
  `cd_sessao` string, 
  `ds_sistema_origem` string, 
  `tp_transacao` string, 
  `cd_ffr` string, 
  `cd_ordem` string, 
  `cd_score` string, 
  `nm_cliente` string, 
  `no_cpf_ffr` string, 
  `ds_email` string, 
  `diagnostico1` string, 
  `diagnostico2` string, 
  `diagnostico3` string, 
  `diagnostico4` string, 
  `diagnostico5` string, 
  `diagnostico6` string, 
  `diagnostico7` string)
PARTITIONED BY ( 
  `dt_carga` string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES ('SERIALIZATION.ENCODING'='UTF-8')
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'abfs://azhdisimuseprd@azsimdtlkprd.dfs.core.windows.net/data/harmonized/FICO/h_fico_clearsale_ffr_output_historico'
