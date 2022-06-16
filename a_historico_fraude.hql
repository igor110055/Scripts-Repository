USE analytical;
DROP TABLE IF EXISTS a_historico_fraude;
CREATE EXTERNAL TABLE a_historico_fraude (
no_cpf STRING,
dt_referencia int,
cd_origem STRING,
ds_motivo STRING
)
PARTITIONED BY (dt_carga string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES ('SERIALIZATION.ENCODING'='UTF-8')
STORED AS INPUTFORMAT'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 'abfs://azhdisimuseprd@azsimdtlkprd.dfs.core.windows.net/data/analytical/a_historico_fraude';