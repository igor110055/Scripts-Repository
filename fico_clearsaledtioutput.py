#*******************************************************************************************
# NOME: FICO_CLEARSALEDTIOUTPUT.PY
# OBJETIVO: CARREGA AS INFORMACOES NA TABELA HIVE.
# CONTROLE DE VERSAO
# 30/07/2019 - VERSAO INICIAL - CRISTINA CRUZ
#*******************************************************************************************
######## -*- coding: utf-8 -*-########

import sys
import os
from pyspark.sql import SparkSession, HiveContext
from pyspark.sql import functions as f
from pyspark.sql import types as t
from py4j.java_gateway import java_import
from sys import argv
import datetime as dt
from pyspark.sql.types import *
from datetime import datetime

processo = sys.argv[1]

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("trataharmonized") \
        .getOrCreate()

    # Varivel global que retorna a data atual.
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
        tabela_log_insert.show()
        tabela_log_insert.write.insertInto("analytical.a_estrutural_log_execucao_carga")

    class ArquivosDadosIO:
        _spark = None
        _fs = None

        def __init__(self, job):
            if ArquivosDadosIO._spark is None:
                ArquivosDadosIO._spark = SparkSession.builder.appName(job).getOrCreate()
                java_import(ArquivosDadosIO._spark._jvm, 'org.apache.hadoop.fs.Path')
                ArquivosDadosIO._fs = ArquivosDadosIO._spark._jvm.org.apache.hadoop.fs.FileSystem. \
                    get(ArquivosDadosIO._spark._jsc.hadoopConfiguration())

        def spark_session(self):
            return ArquivosDadosIO._spark

        def fico_fastdata(self):
            return ArquivosDadosIO._spark.read.csv('/data/raw/FICO/ClearSaleDTIOutput/consolidado/{}/'.format(str(datetime.today().strftime('%Y%m%d'))), header=False, sep=';')

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
            URL = '/hive/warehouse/harmonized.db/h_fico_clearsale_dti_output/'
            df.write.option("encoding", "UTF-8").mode("overwrite").format("parquet").option("header","false").partitionBy('dt_carga').saveAsTable(nome)

    class Gerenciador:
        # Conexao (session) do Spark e acesso a dados
        _dados_io = None
        _spark_session = None

        # Dataframes originais
        _parametros = None

        def __init__(self, _dados_io):
            self._dados_io = _dados_io
            self._fico_fastdata = _dados_io.fico_fastdata()

        def gera_regras(self):
            resultado = fico_fastdata(self._fico_fastdata)
 
            return resultado

    def fico_fastdata(df_fico_fastdata):
        return (df_fico_fastdata
                .select(
                        f.when((f.length(f.trim(f.col('_c0'))) != 0), df_fico_fastdata['_c0'])
                        .otherwise(f.lit(None).cast(StringType())).alias('no_cpf'),
                        f.when((f.length(f.trim(f.col('_c1'))) != 0), df_fico_fastdata['_c1'])
                        .otherwise(f.lit(None).cast(StringType())).alias('no_telefone'),
                        f.when((f.length(f.trim(f.col('_c2'))) != 0), df_fico_fastdata['_c2'])
                        .otherwise(f.lit(None).cast(StringType())).alias('dt_execucao'),
                        f.when((f.length(f.trim(f.col('_c3'))) != 0), df_fico_fastdata['_c3'])
                        .otherwise(f.lit(None).cast(StringType())).alias('ds_job'),
                        f.when((f.length(f.trim(f.col('_c4'))) != 0), df_fico_fastdata['_c4'])
                        .otherwise(f.lit(None).cast(StringType())).alias('cd_sessao'),
                        f.when((f.length(f.trim(f.col('_c5'))) != 0), df_fico_fastdata['_c5'])
                        .otherwise(f.lit(None).cast(StringType())).alias('ds_sistema_origem'),
                        f.when((f.length(f.trim(f.col('_c6'))) != 0), df_fico_fastdata['_c6'])
                        .otherwise(f.lit(None).cast(StringType())).alias('tp_transacao'),
                        f.when((f.length(f.trim(f.col('_c7'))) != 0), df_fico_fastdata['_c7'])
                        .otherwise(f.lit(None).cast(StringType())).alias('cd_dti'),
                        f.when((f.length(f.trim(f.col('_c8'))) != 0), df_fico_fastdata['_c8'])
                        .otherwise(f.lit(None).cast(StringType())).alias('ds_documento'),
                        f.when((f.length(f.trim(f.col('_c9'))) != 0), df_fico_fastdata['_c9'])
                        .otherwise(f.lit(None).cast(StringType())).alias('dt_nascimento'),
                        f.when((f.length(f.trim(f.col('_c10'))) != 0), df_fico_fastdata['_c10'])
                        .otherwise(f.lit(None).cast(StringType())).alias('dt_referencia'),
                        f.when((f.length(f.trim(f.col('_c11'))) != 0), df_fico_fastdata['_c11'])
                        .otherwise(f.lit(None).cast(StringType())).alias('cd_score'),
                        f.when((f.length(f.trim(f.col('_c12'))) != 0), df_fico_fastdata['_c12'])
                        .otherwise(f.lit(None).cast(StringType())).alias('dt_dti'),
                        f.when((f.length(f.trim(f.col('_c13'))) != 0), df_fico_fastdata['_c13'])
                        .otherwise(f.lit(None).cast(StringType())).alias('ds_digital')
                        )
                .withColumn('dt_carga', f.date_format(f.lit(dt.datetime.now()), 'yyyMMdd'))
                )

    df3 = None
    dt_inicio = str(datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
    fmt = '%Y-%m-%d %H:%M:%S'
    d1 = datetime.strptime(dt_inicio, fmt)
    try:
        _dados_io = ArquivosDadosIO('RED_FICO')
        _dadosTabela_io = TabelasDadosIO('RED_FICO')

        _gerenciador = Gerenciador(_dados_io)
        df_fico_fastdata = _gerenciador.gera_regras()

        tabela_hive_gravar = 'harmonized.h_fico_clearsale_dti_output'
        _dadosTabela_io.gravar_parquet(df_fico_fastdata, tabela_hive_gravar)
    
    except Exception as ex:
        df3 = 'ERRO: {}'.format(str(ex))
        dt_fim = str(datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
        d2 = datetime.strptime(dt_fim, fmt)
        diff = d2 - d1
        tempoexec = str(diff)
        gera_log_execucao(processo, dt_inicio, dt_fim, tempoexec, df3)
        spark.stop()
        exit(1)
    else:
        df3 = ("succeeded")
        dt_fim = str(datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
        d2 = datetime.strptime(dt_fim, fmt)
        diff = d2 - d1
        tempoexec = str(diff)
        gera_log_execucao(processo, dt_inicio, dt_fim, tempoexec, df3)
        spark.stop()
        exit(0)
