drop table if exists  harmonized.h_all_check_ancine;
CREATE EXTERNAL TABLE harmonized.h_all_check_ancine
(
no_cpf	string,
no_telefone	string,
dt_execucao	string,
ds_job 	string,
cd_sessao 	string,
ds_sistema_origem 	string,
tp_transacao 	string,
no_cnpj	string,
nm_razao_social	string,
ds_complemento	string,
cd_cep	string,
ds_bairro	string,
nm_cidade	string,
cd_estado	string
)
PARTITIONED BY (dt_carga string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES ('SERIALIZATION.ENCODING'='UTF-8')
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 'abfs://azsacsimdatalakeuseprd@azsacsimdatalakeuseprd.dfs.core.windows.net/data/harmonized/Fico/AllCheckAnaliseFullOutput/ancine/h_all_check_ancine';
