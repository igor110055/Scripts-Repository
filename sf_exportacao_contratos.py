######################################
######## -*- coding: utf-8 -*-########
#########EXPORTACAO SALESFORCE #######
########### CONTRATOS  ###############
############# Main ###################
#############Versão - 1.0#############
######################################

# -*- coding: utf-8 -*-
import sys
from imp import reload
from pyspark.sql import SparkSession, HiveContext
from pyspark.sql import functions as f
from pyspark.sql.types import *
reload(sys)
sys.setdefaultencoding('utf8')

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("trataharmonized") \
        .getOrCreate()


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

    def df_clientes(self):
        return TabelasDadosIO._spark.table('harmonized.h_financeira_clientes')

    def df_propostas_clientes(self):
        return TabelasDadosIO._spark.table('harmonized.h_financeira_propostas_clientes')

    def df_propostas(self):
        return TabelasDadosIO._spark.table('harmonized.h_financeira_propostas')

    def df_contratos(self):
        return TabelasDadosIO._spark.table('harmonized.h_financeira_contratos')


class Gerenciador:
    # Conexao (session) do Spark e acesso a dados
    _dados_io = None
    _spark_session = None

    # Dataframes originais
    _parametros = None

    def __init__(self, _dados_io):
        self._dados_io = _dados_io

        self._propostas = _dados_io.df_propostas()
        self._clientes = _dados_io.df_clientes()
        self._prop_clientes = _dados_io.df_propostas_clientes()
        self._contratos = _dados_io.df_contratos()

    # chamada de todos os passos da ingestao
    def gera_regras(self):
        # variaveis
        path_csv = 'abfs://sparkazredhdi-dev@azredlake.dfs.core.windows.net/data/harmonized/Exportacao/SalesForce/temp/'
        print('origens')
        propostas = df_propostas(self._propostas)
        clientes = df_clientes(self._clientes)
        prop_clientes = df_prop_clientes(self._prop_clientes)
        contratos = df_contratos(self._contratos)
        contratos_filtrados = filtro_particao(contratos)

        print('gerar df final')
        result = df_gera_extracao_contratos(contratos_filtrados, propostas, clientes, prop_clientes)

        print("gravar csv")
        gravar_txt(self, result, path_csv)

        return

# # print("funcao gravar")
# def gravar_json(self, df, path_json):
#     df.repartition(1).write.format("org.apache.spark.sql.json").mode("overwrite").save(path_json)

def gravar_txt(self,df,path):
    df.repartition(1)\
    .write.format("com.databricks.spark.csv")\
        .option('delimiter', ';')\
        .option('header',True)\
        .mode("overwrite").save(path)


def filtro_particao(df):
    date = df.agg(f.max('dt_carga').alias('max')).collect()[0]['max']
    return df.filter(df['dt_carga'] == date)

# definicoes dos laytout das tabelas envolvidas no processo de exportacao de contratos para SalesForce
def aplica_layout_cliente():
    return [
        'tp_pessoa', 'no_cpf_cnpj', 'cd_seq_cli'
    ]


def aplica_layout_propostas():
    return [
        'cd_proposta', 'no_contrato', 'cd_agencia', 'qt_dias_maior_atraso', 'cd_status', 'dt_carga'
    ]


def aplica_layout_propostas_clientes():
    return [
        'cd_proposta', 'no_cpf_cnpj', 'no_cliente', 'cd_crb', 'tp_pessoa'
    ]


def aplica_layout_contratos():
    return [
        'dt_carga', 'no_contrato', 'cd_cliente', 'cd_proposta', 'st_operacao', 'vl_total_emissao_contrato',
        'qt_prestacao_contrato',
        'dt_emissao_contrato', 'dt_termino_contrato',
    ]


# bloco das origens das tabelas do hive
def df_contratos(contratos):
    return (contratos
            .select(aplica_layout_contratos())
            )


def df_propostas(propostas):
    return (propostas
            .filter(propostas['cd_status'] == 'EF')
            .select(aplica_layout_propostas())
            )


def df_prop_clientes(prop_clientes):
    return (prop_clientes
            .filter(prop_clientes['no_cliente'] != 0)
            .select(aplica_layout_propostas_clientes())
            )


def df_clientes(clientes):
    return (clientes
            .select(aplica_layout_cliente())
            )

# query extracao de contratos/propostas
def df_gera_extracao_contratos(df_contratos, df_prospostas, df_clientes, df_prop_clientes):
    return (df_contratos
        .join(df_prospostas, on='no_contrato', how='inner')
        .join(df_prop_clientes, on='cd_proposta', how='inner')
        .join(df_clientes, df_clientes['cd_seq_cli'] == df_contratos['cd_cliente'], how='inner')
        .select(
                df_contratos['st_operacao'].alias('Status'),
                df_prop_clientes['no_cpf_cnpj'].alias('no_cpf_cnpj'),
                df_clientes['no_cpf_cnpj'].alias('no_cpf_cnpj_2'),
                df_prop_clientes['tp_pessoa'].alias('Tipo_Pessoa__c'),
                df_prospostas['qt_dias_maior_atraso'].alias('Dias_Atraso__c'),
                f.when(df_prop_clientes['cd_crb'] == "X", False).otherwise(True).alias('Correntista__c'),
                df_contratos['vl_total_emissao_contrato'].alias('Valor_Contrato__c'),
                f.concat(f.substring(df_contratos['dt_emissao_contrato'], 1, 4), f.lit('-'),
                         f.substring(df_contratos['dt_emissao_contrato'], 5, 2), f.lit('-'),
                         f.substring(df_contratos['dt_emissao_contrato'], 7, 2)
                         ).cast("Date").alias('StartDate'),
                f.round(f.datediff(f.concat(f.substring(df_contratos['dt_termino_contrato'], 1, 4), f.lit('-'),
                                            f.substring(df_contratos['dt_termino_contrato'], 5, 2), f.lit('-'),
                                            f.substring(df_contratos['dt_termino_contrato'], 7, 2)).cast("date"),
                                   f.concat(f.substring(df_contratos['dt_emissao_contrato'], 1, 4), f.lit('-'),
                                            f.substring(df_contratos['dt_emissao_contrato'], 5, 2), f.lit('-'),
                                            f.substring(df_contratos['dt_emissao_contrato'], 7, 2)).cast("date")
                                   ) / 12).cast(IntegerType()).alias('ContractTerm'),
                df_prospostas['cd_proposta'].alias('Contrato_SIM__c')
                ))

_dados_io = TabelasDadosIO('RED_EXPORTACAO_CONTRATOS_SALESFORCE')

_gerenciador = Gerenciador(_dados_io)
_final = _gerenciador.gera_regras()

spark.stop()



