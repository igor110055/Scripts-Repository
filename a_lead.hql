USE analytical;
DROP TABLE IF EXISTS a_lead;	
CREATE EXTERNAL TABLE a_lead (
no_cpf string,
no_telefone string,
ds_email string,
ds_nome string,
dt_nascimento string,
ds_campanha string,
dt_primeiro_acesso string,
dt_ultima_atualizacao string,
ds_status_lead string
)
PARTITIONED BY (dt_referencia string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES ('SERIALIZATION.ENCODING'='UTF-8')
STORED AS INPUTFORMAT'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 'abfs://azhdisimuseprd@azsimdtlkprd.dfs.core.windows.net/data/analytical/a_lead';