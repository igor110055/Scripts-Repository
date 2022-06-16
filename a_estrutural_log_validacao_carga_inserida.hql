USE analytical;
DROP TABLE IF EXISTS a_estrutural_log_validacao_carga_inserida;
CREATE EXTERNAL TABLE a_estrutural_log_validacao_carga_inserida (
cd_sequencial int,
nm_processo string,
nm_objeto string,
qt_registros_inseridos bigint,
dt_execucao bigint
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
'abfs://azhdisimuseprd@azsimdtlkprd.dfs.core.windows.net/data/analytical/a_estrutural_log_validacao_carga_inserida';