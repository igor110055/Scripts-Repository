USE harmonized;
DROP TABLE IF EXISTS h_vivere_fonte_renda;
CREATE EXTERNAL TABLE h_vivere_fonte_renda (
cd_proposta int,
cd_produto string,
no_cpf_cnpj_cliente string,
ds_natureza_ocupacao string,
vl_renda_ocupacao decimal(17,2),
ds_razao_social string,
ds_profissao string,
no_cnpj_ocupacao string,
dt_admissao int,
cd_cep_ocupacao string,
ds_logradouro_ocupacao string,
ds_complemento_ocupacao string,
no_logradouro_ocupacao int,
ds_bairro_ocupacao string,
ds_cidade_ocupacao string,
no_ddd_ocupacao int,
no_telefone int
)
PARTITIONED BY (dt_carga string)
ROW FORMAT SERDE
'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES (
'SERIALIZATION.ENCODING'='UTF-8')
STORED AS INPUTFORMAT
'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 
'abfs://azhdisimuseprd@azsimdtlkprd.dfs.core.windows.net/data/harmonized/Vivere/h_vivere_fonte_renda';