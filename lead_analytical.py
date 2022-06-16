########## -*- coding: utf-8 -*-############
############ Versão - 1.0 ###############
#### Vivere - Analytical - Proposta Produto ####
############ Cristina Cruz ###############
#####################################

import os
import subprocess
from pyspark.sql import SparkSession, HiveContext
from pyspark.sql import functions as f
from pyspark.sql import types as t
from py4j.java_gateway import java_import
from sys import argv
import datetime as dt
from pyspark.sql.types import *
from datetime import datetime
from datetime import timedelta
from pyspark.sql.functions import col

from pyspark.sql.window import Window

import sys

reload(sys)
sys.setdefaultencoding('utf-8')

processo = sys.argv[1]

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("trataharmonized") \
        .getOrCreate()

    # Variável global que retorna a data atual.
    current_date = dt.datetime.now().strftime('%d/%m/%Y')
    print(current_date)


    class DadosIO:
        _spark = None
        _fs = None

        def __init__(self, job):
            if DadosIO._spark is None:
                DadosIO._spark = SparkSession.builder.appName(job) \
                    .config("hive.exec.dynamic.partition", "true") \
                    .config("hive.exec.dynamic.partition.mode", "nonstrict") \
                    .config("spark.sql.parquet.writeLegacyFormat", "true") \
                    .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
                    .config("spark.sql.hive.llap", "false") \
                    .enableHiveSupport() \
                    .getOrCreate()

                java_import(DadosIO._spark._jvm, 'org.apache.hadoop.fs.Path')
                DadosIO._fs = DadosIO._spark._jvm.org.apache.hadoop.fs.FileSystem. \
                    get(DadosIO._spark._jsc.hadoopConfiguration())

        def spark_session(self):
            return DadosIO._spark

        def gravar_parquet(self, df, nome):
            URL = '/hive/warehouse/analytical.db/a_lead/'
            df.write.option("encoding", "UTF-8").mode("overwrite").format("parquet").option("header",
                                                                                            "false").partitionBy(
                'dt_referencia').saveAsTable(nome)

        def vivere_evento_cpf(self):
            return DadosIO._spark.table('harmonized.h_vivere_evento_cpf')

        def vivere_dados_nao_clientes(self):
            return DadosIO._spark.table('harmonized.h_vivere_dados_nao_clientes')

        def vivere_repesc(self):
            return DadosIO._spark.table('harmonized.h_vivere_repesc')


    class Gerenciador:
        # Conexao (session) do Spark e acesso a dados
        _dados_io = None
        _spark_session = None
        _vivere_evento_cpf = None
        _vivere_dados_nao_clientes = None
        _vivere_repesc = None
        # _urlFile = None

        # Dataframes originais
        _parametros = None

        def __init__(self, _dados_io):
            self._dados_io = _dados_io
            self._vivere_evento_cpf = _dados_io.vivere_evento_cpf()
            self._vivere_dados_nao_clientes = _dados_io.vivere_dados_nao_clientes()
            self._vivere_repesc = _dados_io.vivere_repesc()


        def gera_regras(self):
           print(1)
           df1 = vivere_evento_cpf_df1(self._vivere_evento_cpf)
           df2 = vivere_dados_nao_clientes_df2(self._vivere_dados_nao_clientes)
           df3 = vivere_repesc_df3(self._vivere_repesc)
           print(2)
           resultado= None
           resultado2= None
           print("df 1")
          # if (df1.count) > 0 and (df2.count) > 0:
           resultado = join_tables_vivere(df1,df2, df3)


           resultado.show(10)
           return resultado


    # Função que retorna a última partição.
    def filtro_datemax(df):
        date = df.agg(f.max('dt_referencia').alias('max')).collect()[0]['max']
        return df.filter(df['dt_referencia'] == date)



    def vivere_evento_cpf_df1(vivere_evento_cpf):
        return (vivere_evento_cpf
            .select(
            vivere_evento_cpf['no_cpf'].cast(StringType()).alias('no_cpf_evento'),
            vivere_evento_cpf['no_cpf'].cast(StringType()),
            vivere_evento_cpf['no_telefone'].cast(StringType()),
            vivere_evento_cpf['dt_execucao'].cast(StringType()),
            vivere_evento_cpf['ds_job'].cast(StringType()),
            vivere_evento_cpf['cd_sessao'].cast(StringType()),
            vivere_evento_cpf['ds_sistema_origem'].cast(StringType()),
            vivere_evento_cpf['tp_transacao'].cast(StringType()),
            vivere_evento_cpf['no_cpf_evento'].cast(StringType()),
            vivere_evento_cpf['ds_campanha'].cast(StringType()),
            vivere_evento_cpf['dt_carga'].alias('dt_referencia')
        )
        )


    def vivere_dados_nao_clientes_df2(vivere_dados_nao_clientes):
        return (vivere_dados_nao_clientes
            .select(
            vivere_dados_nao_clientes['no_cpf'].cast(StringType()).alias('no_cpf_cliente'),
            vivere_dados_nao_clientes['no_cpf'].cast(StringType()),
            vivere_dados_nao_clientes['no_telefone'].cast(StringType()),
            vivere_dados_nao_clientes['dt_execucao'].cast(StringType()),
            vivere_dados_nao_clientes['ds_job'].cast(StringType()),
            vivere_dados_nao_clientes['cd_sessao'].cast(StringType()),
            vivere_dados_nao_clientes['ds_sistema_origem'].cast(StringType()),
            vivere_dados_nao_clientes['tp_transacao'].cast(StringType()),
            vivere_dados_nao_clientes['dt_data_hora'].cast(StringType()),
            vivere_dados_nao_clientes['no_cpf_nao_cliente'].cast(StringType()),
            vivere_dados_nao_clientes['ds_email'].cast(StringType()),
            vivere_dados_nao_clientes['ds_nome'].cast(StringType()),
            vivere_dados_nao_clientes['no_celular'].cast(StringType()),
            vivere_dados_nao_clientes['dt_nascimento'].cast(StringType()),
            vivere_dados_nao_clientes['dt_carga'].alias('dt_referencia')
        )
        )


    def vivere_repesc_df3(vivere_repesc):
        return (vivere_repesc
            .select(
            vivere_repesc['no_cpf'].cast(StringType()).alias('no_cpf_repesc'),
            vivere_repesc['no_cpf'].cast(StringType()),
            vivere_repesc['no_telefone'].cast(StringType()),
            vivere_repesc['dt_execucao'].cast(StringType()),
            vivere_repesc['ds_job'].cast(StringType()),
            vivere_repesc['cd_sessao'].cast(StringType()),
            vivere_repesc['ds_sistema_origem'].cast(StringType()),
            vivere_repesc['tp_transacao'].cast(StringType()),
            vivere_repesc['cd_repesc'].cast(StringType()),
            vivere_repesc['cd_respesc_fico'].cast(StringType()),
            vivere_repesc['cd_rating'].cast(StringType()),
            vivere_repesc['pc_entrada_minima_credito'].cast(StringType()),
            vivere_repesc['pc_entrada_recomendada'].cast(StringType()),
            vivere_repesc['vl_maximo_financiado'].cast(StringType()),
            vivere_repesc['ds_prazo_maximo'].cast(StringType()),
            vivere_repesc['ds_simular'].cast(StringType()),
            vivere_repesc['fl_permissao_acesso'].cast(StringType()),
            vivere_repesc['cd_desvio_tela'].cast(StringType()),
            vivere_repesc['ds_cor_red_on'].cast(StringType()),
            vivere_repesc['cd_barrinha_serasa'].cast(StringType()),
            vivere_repesc['cd_barrinha_sim'].cast(StringType()),
            vivere_repesc['vl_cp_puro_valor_maximo'].cast(StringType()),
            vivere_repesc['pc_cp_puro_perc_valor_recomendado'].cast(StringType()),
            vivere_repesc['ds_cp_puro_prazo_maximo'].cast(StringType()),
            vivere_repesc['pc_auto_perc_valor_carro'].cast(StringType()),
            vivere_repesc['vl_auto_valor_financiado_maximo'].cast(StringType()),
            vivere_repesc['pc_auto_perc_valor_recomendado'].cast(StringType()),
            vivere_repesc['ds_auto_prazo_maximo'].cast(StringType()),
            vivere_repesc['pc_moto_perc_valor_moto'].cast(StringType()),
            vivere_repesc['vl_moto_valor_financiado_maximo'].cast(StringType()),
            vivere_repesc['pc_moto_perc_valor_recomendado'].cast(StringType()),
            vivere_repesc['ds_moto_prazo_maximo'].cast(StringType()),
            vivere_repesc['pc_renda_perc_valor_renda'].cast(StringType()),
            vivere_repesc['vl_renda_valor_fin_maximo_inferior'].cast(StringType()),
            vivere_repesc['vl_renda_valor_fin_maximo_superior'].cast(StringType()),
            vivere_repesc['pc_renda_perc_valor_recomendado'].cast(StringType()),
            vivere_repesc['ds_renda_prazo_maximo'].cast(StringType()),
            vivere_repesc['vl_cdc_fatura_valor_maximo'].cast(StringType()),
            vivere_repesc['vl_cdc_fatura_valor_recomendado'].cast(StringType()),
            vivere_repesc['ds_cdc_fatura_prazo_maximo'].cast(StringType()),
            vivere_repesc['vl_cdc_boleto_valor_maximo'].cast(StringType()),
            vivere_repesc['pc_cdc_boleto_perc_valor_recomendado'].cast(StringType()),
            vivere_repesc['ds_cdc_boleto_prazo_maximo'].cast(StringType()),
            vivere_repesc['dt_carga'].alias('dt_referencia')
        )
        )



    def join_tables_vivere(df1, df2, df3):
        df5 = (df1.alias('a')
               .join(df2.alias('b'), (df1['no_cpf'] == df2['no_cpf']) & (df2['cd_sessao'] == df1['cd_sessao']), 'left')
               .join(df3.alias('c'), (df2['no_cpf'] == df3['no_cpf']) & (df3['cd_sessao'] == df2['cd_sessao']), 'left')
               ).withColumn('status_lead',
                            f.when((df1['no_cpf'].isNotNull()) & (df2['no_cpf'].isNull()) & (df3['no_cpf'].isNull() ), "Novo")
                             .when((df1['no_cpf'].isNotNull()) & (df2['no_cpf'].isNotNull()) & (df3['no_cpf'].isNull()),"Pre Cadastro")
                             .when((df1['no_cpf'].isNotNull()) & (df2['no_cpf'].isNotNull()) & (df3['no_cpf'].isNotNull()),"Convertido").otherwise(0)
                            )


        df5.show(10)
        df6 = df5.select(df5['a.no_cpf'],
                         df5['a.no_telefone'],
                         df5['a.dt_execucao'],
                         df5['a.ds_job'],
                         df5['a.cd_sessao'],
                         df5['a.ds_sistema_origem'],
                         df5['a.tp_transacao'],
                         df5['b.dt_data_hora'],
                         df5['b.no_cpf_nao_cliente'],
                         df5['b.ds_email'],
                         df5['b.ds_nome'],
                         df5['b.no_celular'],
                         df5['b.dt_nascimento'],
                         df5['a.ds_campanha'],
                         df5['b.dt_referencia'],
                         df5['status_lead']
            )
        print('teste')
        df6.show(10)
        return df6


    # Função de log de execução de carga.
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
        tabela_log_insert.show()
        tabela_log_insert.write.insertInto("analytical.a_estrutural_log_execucao_carga")


    df3 = None
    dt_inicio = str(datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
    fmt = '%Y-%m-%d %H:%M:%S'
    d1 = datetime.strptime(dt_inicio, fmt)

    try:
        _dados_io = DadosIO('RED_VIVERE_LEAD')

        _gerenciador = Gerenciador(_dados_io)
        df_fico_fastdata = _gerenciador.gera_regras()
        df_fico_fastdata.show()

        tabela_hive_gravar = 'analytical.a_lead'
        _dados_io.gravar_parquet(df_fico_fastdata, tabela_hive_gravar)
    except Exception as ex:
        df3 = 'ERRO: {}'.format(str(ex))
    else:
        df3 = ("succeeded")
    finally:
        dt_fim = str(datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
        d2 = datetime.strptime(dt_fim, fmt)
        diff = d2 - d1
        tempoexec = str(diff)
        print(df3)
        gera_log_execucao(processo, dt_inicio, dt_fim, tempoexec, df3)
    spark.stop()
