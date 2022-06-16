USE analytical;
DROP TABLE IF EXISTS a_cadus;
CREATE EXTERNAL TABLE a_cadus (
no_cpf string,
qtde_restr_ativ_cli int,
vlr_total_restr_ativ_cli decimal(15,2),
dt_referencia string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES ('encoding'='UTF-8')
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 'abfs://azhdisimuseprd@azsimdtlkprd.dfs.core.windows.net/data/analytical/a_cadus';