#*******************************************************************************************
# NOME: VIVERE_ANALYTICAL_CONTA_CLIENTE.PY
# OBJETIVO: CRIA A TABELA ANALITICA DE CONTA CLIENTE.
# CONTROLE DE VERSAO
# 05/08/2019 - ANA PAULA SEGATELLI - VERSAO INICIAL
# 13/08/2019 - CRISTINA CRUZ - INCLUIDO JOIN COM A TABELA DE FONTE RENDA
#*******************************************************************************************
######## -*- coding: utf-8 -*-########

from imp import reload
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
import datetime as dt
from pyspark.sql.types import *
from datetime import datetime
from pyspark.sql.window import Window
import sys
reload(sys)
sys.setdefaultencoding('utf8')

processo = sys.argv[1]

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("trataharmonized") \
        .getOrCreate()

    # Variavel global que retorna a data atual.
    current_date = dt.datetime.now().strftime('%d/%m/%Y')
    print(current_date)

    def gera_log_execucao(processo, dt_inicio, dt_termino, tempoexecucao, log_msg):
        _spark = SparkSession.builder.appName('log_execucao') \
            .config("hive.exec.dynamic.partition", "true") \
            .config("hive.exec.dynamic.partition.mode", "nonstrict") \
            .config("spark.sql.parquet.writeLegacyFormat", "true") \
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
            .enableHiveSupport() \
            .getOrCreate()

        aa_carga = str(datetime.today().strftime('%Y'))
        log_msg = [(processo, dt_inicio, dt_termino, tempoexecucao, log_msg, aa_carga)]

        tabela_log_insert = _spark.createDataFrame(log_msg, ["nm_processo",
                                                             "dt_inicio_execucao",
                                                             "dt_fim_excucao",
                                                             "tt_execucao",
                                                             "ds_status",
                                                             "aa_carga"])

        tabela_log_insert.write.insertInto("analytical.a_estrutural_log_execucao_carga")

    class TabelasDadosIO:
        _spark = None

        def __init__(self, job):
            if TabelasDadosIO._spark is None:
                TabelasDadosIO._spark = SparkSession.builder.appName(job) \
                    .config("hive.exec.dynamic.partition", "true") \
                    .config("hive.exec.dynamic.partition.mode", "nonstrict") \
                    .config("spark.sql.parquet.writeLegacyFormat", "true") \
                    .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
                    .enableHiveSupport() \
                    .getOrCreate()

        def gravar_parquet(self, df, nome):
            df.repartition(1).write.option("encoding", "UTF-8").mode("overwrite").format("parquet").option("header","false").saveAsTable(nome, path="/data/analytical/a_conta_cliente/")

        def df_proposta_cadastro(self):
            return TabelasDadosIO._spark.table('harmonized.h_vivere_proposta_cadastro')

        def df_fonte_renda(self):
            return TabelasDadosIO._spark.table('harmonized.h_vivere_proposta_produto_renda')

    class Gerenciador:
        # Conexao (session) do Spark e acesso a dados
        _dados_io = None
        _spark_session = None

        # Dataframes originais
        _parametros = None

        def __init__(self, _dados_io):
            self._dados_io = _dados_io
            self._proposta_cadastro = _dados_io.df_proposta_cadastro()
            self._fonte_renda = _dados_io.df_fonte_renda()

        # Chamada de todos os passos da ingestao
        def gera_regras(self):

            propostas_filtradas = filtro_particao(self._proposta_cadastro)

            rendas_filtradas = filtro_particao(self._fonte_renda)

            resultado_propostas = df_gera_extracao_propostas(propostas_filtradas)

            resultado_rendas = df_gera_extracao_rendas(rendas_filtradas)

            resultado_join = df_vivere_join(resultado_propostas, resultado_rendas)

            return resultado_join

    def filtro_particao(df):
        date = df.agg(f.max('dt_carga').alias('max')).collect()[0]['max']
        return df.filter(df['dt_carga'] == date)

    # Funcao que retorna os campos de Proposta Cadastro.
    def df_gera_extracao_propostas(df_prosposta_cadastro):
        return (df_prosposta_cadastro
                .select(
                        df_prosposta_cadastro['cd_proposta'].cast(IntegerType()),
                        df_prosposta_cadastro['nm_cliente'].cast(StringType()),
                        df_prosposta_cadastro['ds_email'].cast(StringType()),
                        df_prosposta_cadastro['dt_nascimento'].cast(IntegerType()),
                        df_prosposta_cadastro['no_ddd'].cast(IntegerType()),
                        df_prosposta_cadastro['no_telefone'].cast(IntegerType()),
                        df_prosposta_cadastro['nm_documento'].cast(StringType()),
                        df_prosposta_cadastro['cd_oe_documento'].cast(StringType()),
                        df_prosposta_cadastro['dt_exp_documento'].cast(IntegerType()),
                        df_prosposta_cadastro['ds_sexo'].cast(StringType()),
                        df_prosposta_cadastro['ds_nacionalidade'].cast(StringType()),
                        df_prosposta_cadastro['vl_patrimonio'].cast(DecimalType(17,2)),
                        df_prosposta_cadastro['ds_estado_civil'].cast(StringType()),
                        df_prosposta_cadastro['nm_mae'].cast(StringType()),
                        df_prosposta_cadastro['cd_cep'].cast(StringType()),
                        df_prosposta_cadastro['ds_logradouro'].cast(StringType()),
                        df_prosposta_cadastro['no_logradouro'].cast(IntegerType()),
                        df_prosposta_cadastro['ds_complemento'].cast(StringType()),
                        df_prosposta_cadastro['ds_bairro'].cast(StringType()),
                        df_prosposta_cadastro['ds_cidade'].cast(StringType()),
                        df_prosposta_cadastro['cd_uf'].cast(StringType()),
                        df_prosposta_cadastro['cd_banco'].cast(IntegerType()),
                        df_prosposta_cadastro['nm_banco'].cast(StringType()),
                        df_prosposta_cadastro['cd_agencia'].cast(IntegerType()),
                        df_prosposta_cadastro['cd_conta'].cast(StringType()),
                        df_prosposta_cadastro['cd_digito_conta'].cast(StringType()),
                        df_prosposta_cadastro['vl_renda_mensal'].cast(DecimalType(17,2)),
                        df_prosposta_cadastro['ds_profissao'].cast(StringType()),
                        df_prosposta_cadastro['vl_veiculo'].cast(DecimalType(17,2)),
                        df_prosposta_cadastro['ds_marca'].cast(StringType()),
                        df_prosposta_cadastro['ds_modelo'].cast(StringType()),
                        df_prosposta_cadastro['aa_ano'].cast(IntegerType()),
                        df_prosposta_cadastro['ds_combustivel'].cast(StringType()),
                        df_prosposta_cadastro['dt_carga'].cast(StringType())
                        )
                )

    # Funcao que realiza o tratamento das colunas de Fonte Renda, pivoteando os dados.
    def df_gera_extracao_rendas(df_fonte_renda):
        df1 = (df_fonte_renda
                .withColumn('cd_proposta', df_fonte_renda['cd_proposta'].cast('integer'))
                .withColumn('row_number', f.row_number().over(Window.partitionBy('cd_proposta', 'cd_produto', 'no_cpf_cnpj_cliente', 'dt_carga').orderBy('cd_proposta', 'cd_produto', 'no_cpf_cnpj_cliente', 'dt_carga')))
                .filter(f.col('row_number') <= 5)
                )

        df2 = (df1
                .withColumn('row_number', f.concat(f.lit('ds_natureza_ocupacao_'), f.col('row_number')))
                .groupBy('cd_proposta', 'cd_produto', 'no_cpf_cnpj_cliente').pivot('row_number', ['ds_natureza_ocupacao_{}'.format(i) for i in [1, 2, 3, 4, 5]])
                .agg(f.first('ds_natureza_ocupacao'))
                )

        df3 = (df1
                .withColumn('row_number', f.concat(f.lit('vl_renda_ocupacao_'), f.col('row_number')))
                .groupBy('cd_proposta', 'cd_produto', 'no_cpf_cnpj_cliente').pivot('row_number', ['vl_renda_ocupacao_{}'.format(i) for i in [1, 2, 3, 4, 5]])
                .agg(f.first('vl_renda_ocupacao'))
                )

        df4 = (df1
                .withColumn('row_number', f.concat(f.lit('ds_razao_social_'), f.col('row_number')))
                .groupBy('cd_proposta', 'cd_produto', 'no_cpf_cnpj_cliente').pivot('row_number', ['ds_razao_social_{}'.format(i) for i in [1, 2, 3, 4, 5]])
                .agg(f.first('ds_razao_social'))
                )         

        df5 = (df1
                .withColumn('row_number', f.concat(f.lit('ds_profissao_'), f.col('row_number')))
                .groupBy('cd_proposta', 'cd_produto', 'no_cpf_cnpj_cliente').pivot('row_number', ['ds_profissao_{}'.format(i) for i in [1, 2, 3, 4, 5]])
                .agg(f.first('ds_profissao'))
                )      

        df6 = (df1
                .withColumn('row_number', f.concat(f.lit('no_cnpj_ocupacao_'), f.col('row_number')))
                .groupBy('cd_proposta', 'cd_produto', 'no_cpf_cnpj_cliente').pivot('row_number', ['no_cnpj_ocupacao_{}'.format(i) for i in [1, 2, 3, 4, 5]])
                .agg(f.first('no_cnpj_ocupacao'))
                )

        df7 = (df1
                .withColumn('row_number', f.concat(f.lit('dt_admissao_'), f.col('row_number')))
                .groupBy('cd_proposta', 'cd_produto', 'no_cpf_cnpj_cliente').pivot('row_number', ['dt_admissao_{}'.format(i) for i in [1, 2, 3, 4, 5]])
                .agg(f.first('dt_admissao'))
                )

        df8 = (df1
                .withColumn('row_number', f.concat(f.lit('cd_cep_ocupacao_'), f.col('row_number')))
                .groupBy('cd_proposta', 'cd_produto', 'no_cpf_cnpj_cliente').pivot('row_number', ['cd_cep_ocupacao_{}'.format(i) for i in [1, 2, 3, 4, 5]])
                .agg(f.first('cd_cep_ocupacao'))
                )

        df9 =  (df1
                .withColumn('row_number', f.concat(f.lit('ds_logradouro_ocupacao_'), f.col('row_number')))
                .groupBy('cd_proposta', 'cd_produto', 'no_cpf_cnpj_cliente').pivot('row_number', ['ds_logradouro_ocupacao_{}'.format(i) for i in [1, 2, 3, 4, 5]])
                .agg(f.first('ds_logradouro_ocupacao'))
                )               

        df10 = (df1
                .withColumn('row_number', f.concat(f.lit('ds_complemento_ocupacao_'), f.col('row_number')))
                .groupBy('cd_proposta', 'cd_produto', 'no_cpf_cnpj_cliente').pivot('row_number', ['ds_complemento_ocupacao_{}'.format(i) for i in [1, 2, 3, 4, 5]])
                .agg(f.first('ds_complemento_ocupacao'))
                )              

        df11 = (df1
                .withColumn('row_number', f.concat(f.lit('no_logradouro_ocupacao_'), f.col('row_number')))
                .groupBy('cd_proposta', 'cd_produto', 'no_cpf_cnpj_cliente').pivot('row_number', ['no_logradouro_ocupacao_{}'.format(i) for i in [1, 2, 3, 4, 5]])
                .agg(f.first('no_logradouro_ocupacao'))
                )               

        df12 = (df1
                .withColumn('row_number', f.concat(f.lit('ds_bairro_ocupacao_'), f.col('row_number')))
                .groupBy('cd_proposta', 'cd_produto', 'no_cpf_cnpj_cliente').pivot('row_number', ['ds_bairro_ocupacao_{}'.format(i) for i in [1, 2, 3, 4, 5]])
                .agg(f.first('ds_bairro_ocupacao'))
                )                

        df13 = (df1
                .withColumn('row_number', f.concat(f.lit('ds_cidade_ocupacao_'), f.col('row_number')))
                .groupBy('cd_proposta', 'cd_produto', 'no_cpf_cnpj_cliente').pivot('row_number', ['ds_cidade_ocupacao_{}'.format(i) for i in [1, 2, 3, 4, 5]])
                .agg(f.first('ds_cidade_ocupacao'))
                )                  

        df14 = (df1
                .withColumn('row_number', f.concat(f.lit('no_ddd_ocupacao_'), f.col('row_number')))
                .groupBy('cd_proposta', 'cd_produto', 'no_cpf_cnpj_cliente').pivot('row_number', ['no_ddd_ocupacao_{}'.format(i) for i in [1, 2, 3, 4, 5]])
                .agg(f.first('no_ddd_ocupacao'))
                )                

        df15 = (df1
                .withColumn('row_number', f.concat(f.lit('no_telefone_'), f.col('row_number')))
                .groupBy('cd_proposta', 'cd_produto', 'no_cpf_cnpj_cliente').pivot('row_number', ['no_telefone_{}'.format(i) for i in [1, 2, 3, 4, 5]])
                .agg(f.first('no_telefone'))
                )              

        df_join = (df2
                    .join(df3, ['cd_proposta','cd_produto','no_cpf_cnpj_cliente'], 'left')
                    .join(df4, ['cd_proposta', 'cd_produto', 'no_cpf_cnpj_cliente'], 'left')
                    .join(df5, ['cd_proposta', 'cd_produto', 'no_cpf_cnpj_cliente'], 'left')
                    .join(df6, ['cd_proposta', 'cd_produto', 'no_cpf_cnpj_cliente'], 'left')
                    .join(df7, ['cd_proposta', 'cd_produto', 'no_cpf_cnpj_cliente'], 'left')
                    .join(df8, ['cd_proposta', 'cd_produto', 'no_cpf_cnpj_cliente'], 'left')
                    .join(df9, ['cd_proposta', 'cd_produto', 'no_cpf_cnpj_cliente'], 'left')
                    .join(df10, ['cd_proposta', 'cd_produto', 'no_cpf_cnpj_cliente'], 'left')
                    .join(df11, ['cd_proposta', 'cd_produto', 'no_cpf_cnpj_cliente'], 'left')
                    .join(df12, ['cd_proposta', 'cd_produto', 'no_cpf_cnpj_cliente'], 'left')
                    .join(df13, ['cd_proposta', 'cd_produto', 'no_cpf_cnpj_cliente'], 'left')
                    .join(df14, ['cd_proposta', 'cd_produto', 'no_cpf_cnpj_cliente'], 'left')
                    .join(df15, ['cd_proposta', 'cd_produto', 'no_cpf_cnpj_cliente'], 'left')
                    )  

        return df_join

    # Funcao que realiza o join entre as tabelas de Proposta Cadastro e Renda.
    def df_vivere_join(df_gera_extracao_propostas, df_gera_extracao_rendas):
        df = (df_gera_extracao_propostas
                .join(df_gera_extracao_rendas, 'cd_proposta', 'left')
                .withColumn('dt_referencia', df_gera_extracao_propostas['dt_carga'])
                .drop('dt_carga')
                )
        return df    

    df3 = None
    dt_inicio = str(datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
    fmt = '%Y-%m-%d %H:%M:%S'
    d1 = datetime.strptime(dt_inicio, fmt)

    try:
        _dados_io = TabelasDadosIO('RED_VIVERE_CONTA_CLIENTE')
        _gerenciador = Gerenciador(_dados_io)
        resultado = _gerenciador.gera_regras()

        tabela_hive_gravar = 'analytical.a_conta_cliente'
        # url = '/hive/warehouse/analytical.db/a_conta_cliente/'
        _dados_io.gravar_parquet(resultado, tabela_hive_gravar)
    except Exception as ex:
        df3 = 'ERRO: {}'.format(str(ex))
        dt_fim = str(datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
        d2 = datetime.strptime(dt_fim, fmt)
        diff = d2 - d1
        tempoexec = str(diff)
        gera_log_execucao(processo, dt_inicio, dt_fim, tempoexec, df3)
        exit(1)
    else:
        df3 = ("succeeded")
        dt_fim = str(datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
        d2 = datetime.strptime(dt_fim, fmt)
        diff = d2 - d1
        tempoexec = str(diff)
        gera_log_execucao(processo, dt_inicio, dt_fim, tempoexec, df3)
        exit(0)

print("fim processo")
spark.stop()