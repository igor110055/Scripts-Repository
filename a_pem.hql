DROP TABLE IF EXISTS analytical.a_pem;
CREATE EXTERNAL TABLE analytical.a_pem (
id string,
cpf string,
flag_fraude_hist string,
flag_pap string,
flag_cupom string,
pre_rating_sim_pem string,
qtd_dias_atraso_contrato_sim string,
flag_negado_ult_30_dias_sim string,
flag_possui_ctr_sim string,
flag_func string,
flag_monoprodutista string,
flag_correntista string,
tipo_vinculo_cc string,
publico string,
cc_vlr_investimento string,
cc_segmentacao_primaria string,
cc_segmentacao_secundaria string,
renda_balde string,
nivel_confianca string,
flag_possui_auto_quitado string,
flag_possui_moto_quitado string,
cadus_qtde_restr_ativ_cli string,
cadus_vlr_total_restr_ativ_cli string,
saldo_interno_cartao string,
saldo_interno_cheque string,
saldo_interno_cp string,
saldo_interno_reneg string,
pem_01 string,
pem_02 string,
pem_03 string,
pem_04 string,
pem_05 string,
pem_06 string,
pem_07 string,
pem_08 string,
pem_09 string,
pem_10 string
)
PARTITIONED BY (dt_referencia string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES ('SERIALIZATION.ENCODING'='UTF-8')
STORED AS INPUTFORMAT'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 'abfs://azhdisimuseprd@azsimdtlkprd.dfs.core.windows.net/data/analytical/a_pem';