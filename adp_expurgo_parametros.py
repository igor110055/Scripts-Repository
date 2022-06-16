######################################
######## -*- coding: utf-8 -*- #######
############ Versão - 1.0 ############
######## Expurgo - Parametros ########
######## Victor Buoro da Silva #######
######################################

from pyspark.sql import SparkSession
from py4j.java_gateway import java_import
from pyspark.sql import functions as f
from datetime import datetime
import sys
processo = sys.argv[1]

if __name__ == "__main__":
    spark = SparkSession.builder.appName("expurgo_parametros").getOrCreate()
    class ArquivosDadosIO:
        _spark = None
        _fs = None

        def __init__(self, job):
            if ArquivosDadosIO._spark is None:
                ArquivosDadosIO._spark = SparkSession.builder.appName(job).getOrCreate()
                java_import(ArquivosDadosIO._spark._jvm, 'org.apache.hadoop.fs.Path')
                ArquivosDadosIO._fs = ArquivosDadosIO._spark._jvm.org.apache.hadoop.fs.FileSystem.\
                    get(ArquivosDadosIO._spark._jsc.hadoopConfiguration())

        def spark_session(self):
            return ArquivosDadosIO._spark

        def parametros(self):
            return ArquivosDadosIO._spark.read.csv('/data/raw/Componentes/Expurgo_parametros/', header=True, sep=";")

    class TabelasDadosIO:
        _spark = None

        def __init__(self, job):
            if TabelasDadosIO._spark is None:
                TabelasDadosIO._spark = SparkSession.builder.appName(job) \
                .config("hive.exec.dynamic.partition.mode", "nonstrict") \
                .config("spark.sql.parquet.writeLegacyFormat","true")\
                .enableHiveSupport() \
                .getOrCreate()

        def gravar_parquet(self, df, nome):
            URL = '/hive/warehouse/harmonized.db/h_estrutural_expurgo/'
            df.write.option("encoding","UTF-8").mode("overwrite").format("parquet").option("header", "false").saveAsTable(nome)

    class Gerenciador:
        # Conexao (session) do Spark e acesso a dados
        _dados_io = None
        _spark_session = None

        # Dataframes originais
        _parametros = None

        def __init__(self, _dados_io):
            self._dados_io = _dados_io
            self._spark_session = _dados_io.spark_session()
            self._parametros = _dados_io.parametros()

        def gera_regras(self):
            resultado = parametros(self._parametros)
            return resultado

    # Função que retorna os dados do arquivo de parametros do expurgo.
    def parametros(df_parametros):
        df_parametros = df_parametros\
            .withColumn('NM_PROCESSO', f.lit(f.upper(df_parametros['NM_PROCESSO'])))\
            .withColumn('NM_CAMADA', f.lit(f.upper(df_parametros['NM_CAMADA'])))\
            .withColumn('NM_DATABASE', f.lit(f.upper(df_parametros['NM_DATABASE'])))\
            .withColumn('NM_TABELA', f.lit(f.upper(df_parametros['NM_TABELA'])))\
            .withColumn('NM_PARTICAO', f.lit(f.upper(df_parametros['NM_PARTICAO'])))\
            .withColumn('FL_ATIVO', f.lit(f.upper(df_parametros['FL_ATIVO'])))
        return (df_parametros.select('NM_PROCESSO',
                                     'NM_CAMADA',
                                     'NM_DATABASE',
                                     'NM_TABELA',
                                     'NM_DIR_ORIGEM',
                                     'NM_DIR_HISTORICO',
                                     'NM_PARTICAO',
                                     'FL_ATIVO'))

    def gera_log_execucao(processo, dt_inicio, dt_termino, tempoexecucao, log_msg):
        _spark = SparkSession.builder.appName('log_execucao') \
            .config("hive.exec.dynamic.partition", "true") \
            .config("hive.exec.dynamic.partition.mode", "nonstrict") \
            .config("spark.sql.parquet.writeLegacyFormat", "true") \
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
            .enableHiveSupport() \
            .getOrCreate()

        #aa_carga = str(2020)
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
        _dados_io = ArquivosDadosIO('RED_EXPURGO_PARAMETROS')
        _dadosTabela_io = TabelasDadosIO('RED_EXPURGO_PARAMETROS')

        _gerenciador = Gerenciador(_dados_io)
        df_parametros = _gerenciador.gera_regras()
        tabela_hive = 'harmonized.h_estrutural_expurgo'
        _dadosTabela_io.gravar_parquet(df_parametros, tabela_hive)
    except Exception as ex:
        df3 = 'ERRO: {}'.format(str(ex))
    else:
        df3 = ("succeeded")
    finally:
        dt_fim = str(datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
        d2 = datetime.strptime(dt_fim, fmt)
        diff = d2 - d1
        tempoexec = str(diff)
        gera_log_execucao(processo, dt_inicio, dt_fim, tempoexec, df3)
        exit(0)
        
    spark.stop()

