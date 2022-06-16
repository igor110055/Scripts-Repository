import sys
import os
from time import sleep
import subprocess
import logging
from pyspark.sql import types as t
from pyspark.sql import functions as f
from pyspark.sql import SparkSession, HiveContext
from datetime import datetime
# ----------------------------------------------------------
####################################################
# Functions Auxiliares                             #
# autor: Victor Buoro                              #
####################################################
''' INICIO UDFs v1.0'''
def toInteger(df, nome_campos):
    '''
    :param df: Nome do dataframe que sera realizada a transformacao
    :param nome_campo: Array de colunas que deseja transdormar em inteiro
    :return: Campo(s) passados por parametro no formato inteiro (removidos qualquer caracter que nao seja numero)
    '''
    for nome_campo in nome_campos:
        df = df.withColumn(nome_campo + "_aux", f.lit(f.regexp_replace(df[nome_campo], '[^\d]', '')).cast(t.IntegerType()))
        df = df.drop(nome_campo).withColumnRenamed(nome_campo+"_aux", nome_campo)
    return df

def maiuscula(df, nome_campos):
    '''
    :param df: Nome do dataframe que sera realizada a transformacao
    :param nome_campos: Array de colunas que deseja aplicar uppercase
    :return: Colunas passadas pelo parametro nome_campos com o texto em maiusculo
    '''
    for nome_campo in nome_campos:
        df = df.withColumn(nome_campo + "_aux", f.lit(f.upper(df[nome_campo])).cast(t.StringType()))
        df = df.drop(nome_campo).withColumnRenamed(nome_campo+"_aux", nome_campo)
    return df

def para_decimal(df, nome_campos, total_digitos, casas_decimais):
    '''
    :param df: Nome do dataframe que sera realizada a transformacao
    :param nome_campos: Array de colunas que deseja transformar em decimal
    :param digitos: Quantidade total de digitos (antes e depois da virgula)
    :param casas_decimais: Quantidade de casas decimais a direita da virgula da saida
    :return: Campo passado por parametro no formato Decimal

    Obs: Os campos de casas decimais e digitos sao obrigatorios para evitar problemas como perder casas decimais. Fica
    a cargo do desenvolvedor informar o tamanho dos campos.
    '''

    for nome_campo in nome_campos:
        df = df.withColumn(nome_campo + "_aux", f.lit(f.regexp_replace(f.translate(df[nome_campo], ',', '.'), '[^\d^\.]', '')).cast(t.DecimalType(total_digitos, casas_decimais)))
        df = df.drop(nome_campo).withColumnRenamed(nome_campo+"_aux", nome_campo)
    return df

def formata_decimal_em_string(df, nome_campo, tamanho):
    df = df\
        .withColumn(nome_campo+"_aux",
                    f.lit(f.lpad(f.regexp_replace(df[nome_campo], '[^\d]', ''), tamanho, "0"))
                    .cast(t.StringType()))
    df = df.drop(nome_campo).withColumnRenamed(nome_campo+"_aux", nome_campo)
    return df

def formata_cpf(df, nome_campo):
    '''
    :param df: Nome do dataframe que sera realizada a transformacao
    :param nome_campo: Nome da coluna que deseja formatar
    :return: CPF no formato XXXXXXXXXXX
    '''
    return formata_decimal_em_string(df, nome_campo, 11)

def formata_cnpj(df, nome_campo):
    '''
    :param df: Nome do dataframe que sera realizada a transformacao
    :param nome_campo: Nome da coluna que deseja formatar
    :return: CNPJ no formato XXXXXXXXXXXXXX
    '''
    return formata_decimal_em_string(df, nome_campo, 14)

def formata_cep(df, nome_campo):
    '''
    :param df: Nome do dataframe que sera realizada a transformacao
    :param nome_campo: Nome da coluna que deseja formatar
    :return: CEP no formato XXXXXXXX
    '''
    return formata_decimal_em_string(df, nome_campo, 8)

def formata_data(df, nome_campo, formato_entrada='ddMMyyy', formato_saida='yyyMMdd'):
    '''
    :param df: Nome do dataframe que sera realizada a transformacao
    :param nome_campo: Nome da coluna que deseja formatar
    :param formato_entrada: Define um formato de data de entrada(padrao: ddMMyyy)
    :param formato_saida: Define um formato de data de saida(padrao: yyyMMdd)
    :return: Campo Inteiro com data no formato definido pelo padrao de saida(padrao: yyyMMdd)
    '''

    df = toInteger(df.withColumn(nome_campo+"_aux", f.from_unixtime(f.unix_timestamp(df[nome_campo], formato_entrada), formato_saida)), [nome_campo+"_aux"])
    # df = toInteger(df, nome_campo+"_aux")
    df = df.drop(nome_campo).withColumnRenamed(nome_campo+"_aux", nome_campo)
    return df

#    Para realizar os depara, necessario criar dataframe intermediario

def retorna_depara(_dados_io, lista_depara, df, nome_campo, adiciona_substitui="A"):
    '''
    :param lista_depara: Dicionario com a correspondencia codigo: valor (Ex.: [('m', 'masculino'),('f', 'feminio')]
    :param df: Dataframe que contem a chave que sera convertida
    :param nome_campo: Nome do campo que sera convertido no depara
    :param adiciona_substitui: Adiciona uma coluna com valor sem remover codigo original ou substitui o codigo pelo valor na mesma coluna
    :return: retorna dataframe com depara realizado
    '''

    # from itertools import chain
    # mapping_expr = f.create_map([f.lit(x) for x in chain(*lista_depara.items())])
    # return df.withColumn(nome_campo+'_descricao', mapping_expr[df[nome_campo]])

    depara = _dados_io._spark.createDataFrame(lista_depara, [nome_campo, nome_campo+'_descricao'])

    # Converter os 2 campos para maiusculo para garantir comparacao
    depara = maiuscula(depara, [nome_campo, nome_campo + '_descricao'])
    df = maiuscula(df, [nome_campo])
    df = df.withColumn(nome_campo+'_codigo', df[nome_campo])
    df_com_depara = df.join(depara, nome_campo, 'left').drop(nome_campo)
    df_com_depara = df_com_depara.withColumnRenamed(nome_campo+'_codigo', nome_campo)
    if adiciona_substitui == "S":
        df_com_depara = (df_com_depara
                         .drop(nome_campo)
                         .withColumnRenamed(nome_campo + '_descricao', nome_campo))

    return df_com_depara

def depara_sexo(_dados_io, df, nome_campo, adiciona_substitui="A"):
    lista_depara = [('m', 'masculino'),
                   ('f', 'feminio')]
    return retorna_depara(_dados_io, lista_depara, df, nome_campo, adiciona_substitui)

def depara_civil(_dados_io, df, nome_campo, adiciona_substitui="A"):
    lista_depara = [('S', 'solteiro'),
                    ('C', 'casado'),
                    ('V', 'viuvo'),
                    ('D', 'divorciado'),
                    ('Q', 'desquitado'),
                    ('O', 'outros')]
    return retorna_depara(_dados_io, lista_depara, df, nome_campo, adiciona_substitui)

def depara_porte_empresa(_dados_io, df, nome_campo, adiciona_substitui="A"):
    lista_depara = [('p', 'pequena'),
                    ('m', 'media'),
                    ('i', 'industria'),
                    ('n', 'outros')]
    return retorna_depara(_dados_io, lista_depara, df, nome_campo, adiciona_substitui)

def depara_local_correspondencia(_dados_io, df, nome_campo, adiciona_substitui="A"):
    lista_depara = [(1, 'endereco residencial'),
                    (2, 'endereco comercial')]
    return retorna_depara(_dados_io, lista_depara, df, nome_campo, adiciona_substitui)

def depara_tipo_moeda(_dados_io, df, nome_campo, adiciona_substitui="A"):
    lista_depara = [('T', 'trd'),
                    ('D', 'dolar'),
                    ('R', 'real')]
    return retorna_depara(_dados_io, lista_depara, df, nome_campo, adiciona_substitui)

def depara_tipo_pessoa(_dados_io, df, nome_campo, adiciona_substitui="A"):
    lista_depara = [('1', 'Pessoa Fisica'),
                    ('2', 'Pessoa Juridica')]
    return retorna_depara(_dados_io, lista_depara, df, nome_campo, adiciona_substitui)

def depara_subsegmento(_dados_io, df, nome_campo, adiciona_substitui="A"):
    lista_depara = [('369', 'CP SIM'),
                    ('377', 'CP Fatura SIM'),
                    ('380', 'CDC Boleto SIM'),
                    ('399', 'CDC SIM'),
                    ('106', 'CP VEICULOS SIM'),
                    ('107', 'CDC VEICULOS SIM'),
                    ('740', 'CP MOTOS SIM'),
                    ('782', 'CDC MOTOS SIM'),
                    ('120', 'Peugeot'),
                    ('130', 'Citroen')]
    return retorna_depara(lista_depara, df, nome_campo, adiciona_substitui)

def depara_tipo_operacao(_dados_io, df, nome_campo, adiciona_substitui="A"):
    lista_depara = [('i', 'inclusao'),
                    ('a', 'alteracao'),
                    ('e', 'exclusao')]
    return retorna_depara(lista_depara, df, nome_campo, adiciona_substitui)

''' FIM UDFs '''
####################################################
# FIM Functions Auxiliares                         #
####################################################
logger = logging.getLogger("")
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s: FIN_EXPURGO %(message)s'
)


def setError(msg):
    # print(msg)
    logger.info(msg)
    sys.exit(1)


def setInfo(msg):
    # print(msg)
    logger.info(msg)


def getSleep(secunds):
    print "sleep"
    sleep(secunds)
    print "waking"


def debug(printDebug, stop=True):
    print("Debug:")
    print printDebug

    if (stop):
        print("Debug-Stop:Executed!")
        os._exit(0)

def isArchiveExist(path_and_name_archive):
    return os.path.exists(path_and_name_archive)


def listFilesHDFS(path):
    '''
    :param path: diretorio que deseja listar os arquivos
    :return: lista com os arquivos no diretorio informado
    '''
    cmd = ['hdfs', 'dfs', '-ls', '-C', path]
    files = subprocess.check_output(cmd).strip().split('\n')
    return files


def moveFilesHDFS(src, dst):
    '''
    :param src: arquivo a ser movido
    :param dst: diretorio que deseja mover o arquivo
    :return: move o arquivo para o diretorio informado
    '''
    cmd = ['hdfs', 'dfs', '-mv', src, dst]
    return subprocess.check_output(cmd).strip().split('\n')


def copyFilesHDFS(src, dst):
    '''
    :param src: arquivo a ser movido
    :param dst: diretorio que deseja mover o arquivo
    :return: move o arquivo para o diretorio informado
    '''
    cmd = ['hdfs', 'dfs', '-cp', src, dst]
    return subprocess.check_output(cmd).strip().split('\n')


def getFileHDFS(src):
    '''
    :param src: arquivo com diretorio do HDFS
    :return: coloca o arquivo na pasta de trabalho do servidor (local onde esta sendo executado o job)
    '''
    cmd = ['hdfs', 'dfs', '-get', src, '.']
    return subprocess.check_output(cmd).strip().split('\n')


def putFileHDFS(nome_arquivo, dst):
    '''
    :param nome_arquivo: nome do arquivo a ser colocado no HDFS
    :param dst: Diretorio do HDFS que deseja colocar o arquivo
    :return: insere o arquivo no HDFS
    '''
    cmd = ['hdfs', 'dfs', '-put', nome_arquivo, dst]
    return subprocess.check_output(cmd).strip().split('\n')


def removeFileHDFS(src):
    '''
    :param src: nome do arquivo com diretorio no HDFS
    :return: remove arquivo do HDFS
    '''
    if (src != '/'):
        cmd = ['hdfs', 'dfs', '-rm', '-r', src]
        return subprocess.check_output(cmd).strip().split('\n')
    return False


def removeFile(src):
    '''
    :param src: nome do arquivo com diretorio
    :return: remove arquivo do servidor
    '''
    return os.remove(src)

def gera_log_execucao(processo, dt_inicio, dt_termino, tempoexecucao, log_msg):
    _spark = SparkSession.builder.appName('log_execucao') \
        .config("hive.exec.dynamic.partition", "true") \
        .config("hive.exec.dynamic.partition.mode", "nonstrict") \
        .config("spark.sql.parquet.writeLegacyFormat", "true") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .enableHiveSupport() \
        .getOrCreate()

    # aa_carga = str(2020)
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

def limpa_espaco_branco(col):
    return (f.when(f.length(f.trim(col)) != 0, col).otherwise(f.lit(None)))
#####################################################
# FIM Functions utils                               #
#####################################################
