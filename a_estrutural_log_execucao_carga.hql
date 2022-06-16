USE analytical;
DROP TABLE IF EXISTS a_estrutural_log_execucao_carga;
CREATE EXTERNAL TABLE a_estrutural_log_execucao_carga (
nm_processo string,
dt_inicio_execucao string,
dt_fim_execucao string,
tt_execucao string,
ds_status string
)
PARTITIONED BY (aa_carga string)
ROW FORMAT SERDE
'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES (
'SERIALIZATION.ENCODING'='UTF-8')
STORED AS INPUTFORMAT
'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 
'abfs://azhdisimuseprd@azsimdtlkprd.dfs.core.windows.net/data/analytical/a_estrutural_log_execucao_carga';