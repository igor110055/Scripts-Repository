USE analytical;
DROP TABLE IF EXISTS a_cetip;	
CREATE EXTERNAL TABLE a_cetip (
no_cpf STRING,
tp_documento INT,
ds_documento STRING,
fl_veiculo_atual_historico INT,
ds_veiculo_atual_historico STRING,
fl_veiculo_financiado STRING,
dt_primeiras_pagto_dpvat INT,
dt_ultimo_pagto_dpvat INT,
dt_inclusao INT,
dt_baixa INT,
cd_uf_emplacamento STRING,
dt_contrato INT,
qt_meses INT,
tp_restricao INT,
ds_restricao STRING,
ds_marca_modelo STRING,
aa_modelo INT,
tp_veiculo STRING,
tp_tabela STRING,
ds_tabela STRING,
cd_tabela INT,
vl_atual String,
dt_aquisicao INT,
dt_venda INT,
fl_autofinanciamento STRING,
dt_referencia string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES ('SERIALIZATION.ENCODING'='UTF-8')
STORED AS INPUTFORMAT'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 'abfs://azhdisimuseprd@azsimdtlkprd.dfs.core.windows.net/data/analytical/a_cetip';