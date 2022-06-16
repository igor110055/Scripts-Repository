USE harmonized;
DROP TABLE IF EXISTS h_financeira_parcelas_historico;	
CREATE EXTERNAL TABLE h_financeira_parcelas_historico (
TP_REGISTO BIGINT COMMENT 'Tipo de Registro (1 - FIXO)',
TP_OPERACAO  STRING COMMENT 'Descricao Tipo de Movimento (I / A - Inclusao / Alteracao )',
CD_VEICULO_LEGAL STRING COMMENT 'Codigo do Veiculo Legal (Banco =001  /  Leasing =002)',
DS_VEICULO_LEGAL STRING COMMENT 'Descricao Codigo do Veiculo Legal (Banco =001  /  Leasing =002)',
CD_FILIAL INT COMMENT 'Codigo da Filial PSA (Bco São Paulo = 19 / Bco Rio de Janeiro = 18)',
NO_CONTRATO BIGINT COMMENT 'Numero do Contrato',
NO_PARCELA BIGINT COMMENT 'Numero da Parcela',
DT_VENCIMENTO_PARCELA INT COMMENT 'Data do Vencimento da parcela',
DT_PAGAMENTO_PARCELA INT COMMENT 'Data de Pagamento da Parcela',
VL_PARCELA STRING COMMENT 'Valor da Parcela',
VL_PRINCIPAL_AMORTIZADO DECIMAL(17,2) COMMENT 'Valor Principal Amortizado',
VL_JUROS_MORA_PARCELA DECIMAL(17,2) COMMENT 'Valor Juros de Mora da Parcela',
VL_RESIDUAL_PARCELA STRING COMMENT 'Valor residual da Parcela  (somente para leasing)',
VL_IOF_PARCELA DECIMAL(17,2) COMMENT 'Valor do IOF da Parcela (para Leasing, IOF = zeros )',
VL_ISS_PARCELA DECIMAL(17,2) COMMENT 'Valor do ISS da Parcela (fixo = zeros)',
VL_DESCONTO_PARCELA DECIMAL(17,2) COMMENT 'Valor do Desconto da Parcela (p/ ocorrencias de pagto)',
VL_MULTA_PARCELA DECIMAL(17,2) COMMENT 'Valor da Multa da Parcela (p/ ocorrencias de pagto)',
VL_TOTAL_PAGO DECIMAL(17,2) COMMENT 'Valor total pago (incluindo juros, iof)',
TP_PAGAMENTO STRING COMMENT 'Tipo de Pagamento (I=Integral  / P=Parcial  / E=Estorno)',
DS_TIPO_PAGAMENTO STRING COMMENT 'Descricao Tipo de Pagamento (I=Integral  / P=Parcial  / E=Estorno)',
TX_CAPTACAO String COMMENT 'Taxa de Captação',
VL_LAMINA DECIMAL(17,2) COMMENT 'Valor da Lamina (tarifa de emissão da lâmina)',
CD_RECEBIMENTO_PRESTACAO STRING COMMENT 'Tipo Recebimento prestação',
DS_CD_RECEBIMENTO_PRESTACAO STRING COMMENT 'Descricao Tipo Recebimento prestação',
VL_VENCIMENTO_PRESTACAO_BANCO DECIMAL(13,2) COMMENT 'Valor Vencimento prestação banco',
VL_RECEBIMENTO_CONTRA_PRESTACAO_SUBSIDIO DECIMAL(13,2) COMMENT 'Valor Recebimento contra-prestação subsidio ',
NR_BOLETO_FIDC STRING COMMENT 'Número boleto FIDC',
FILLER STRING COMMENT 'Espaços em Branco'
)
PARTITIONED BY (DT_CARGA STRING)
ROW FORMAT SERDE'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
'field.delim'='|',
'line.delim'='\n',
'serialization.format'='|')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 'abfs://azhdisimuseprd@azsimdtlkprd.dfs.core.windows.net/data/harmonized/Financeira/h_financeira_parcelas_historico/'
TBLPROPERTIES ('COMPRESS'='GZIP');