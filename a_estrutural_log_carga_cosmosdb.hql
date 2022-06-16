USE analytical;
DROP TABLE IF EXISTS a_estrutural_log_carga_cosmosdb;
CREATE EXTERNAL TABLE a_estrutural_log_carga_cosmosdb (
nm_processo string,
nm_tabela string,
ds_envio_cosmos string
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
'abfs://azhdisimuseprd@azsimdtlkprd.dfs.core.windows.net/data/analytical/a_estrutural_log_carga_cosmosdb';