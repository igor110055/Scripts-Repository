# *******************************************************************************************
# NOME: BATCH_PARSE_XML_AFM_OUTPUT.PY
# OBJETIVO: CARREGA AS INFORMACOES NA TABELA HIVE E CONSOLIDA OS ARQUIVOS XML.
# CONTROLE DE VERSAO
# 30/07/2019 - VERSAO INICIAL - VICTOR BUORO
# *******************************************************************************************
######## -*- coding: utf-8 -*-########

import subprocess
import xml.etree.ElementTree as ET
import csv
import datetime

from pyspark.sql import Row
from pyspark.sql.session import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark import SparkContext, SparkConf

from datetime import datetime
import sys

row_cabecalho = Row('col1', 'col2')
row_consulta = Row('columnName1', 'columnName2', 'columnName3', 'columnName4', 'columnName5', 'columnName6',
                   'columnName7', 'columnName8')

ano = str(datetime.now().year)
mes = str(datetime.now().month).zfill(2)
dia = str(datetime.now().day).zfill(2)

processo = str(sys.argv[1])

if __name__ == "__main__":
    conf = SparkConf().setAppName("afmoutput")
    sc = SparkContext(conf=conf)
    spark = SparkSession.builder.appName("afmoutput").getOrCreate()

    # Variavel global que retorna a data atual.
    current_date = datetime.now().strftime('%d/%m/%Y')
    print(current_date)


# Carrega as variaveis de configuracao
def carregaLib():
    _getLibs = "/data/source/lib"
    cmd = ['hdfs', 'dfs', '-get', _getLibs, '.']
    subprocess.check_output(cmd).strip().split('\n')
    sys.path.insert(0, "lib")


carregaLib()
import Config.Constants as conf
import Util.Function as udf_f


def gera_arquivo(nome_diretorio, nome_arquivo, nome_arquivo_final):
    print("victor entrou na gera_arquivo")
    # Lista os arquivos existente no diretorio no qual o streaming esta sendo gerado.
    v_blob_spark = conf.getBlobUrl('azredlake')
    print("v_blob_spark: " + format(v_blob_spark))
    args = "hdfs dfs -ls " + v_blob_spark + "/data/raw/FICO/PayloadAFMOutput/consolidado/" + ano + mes + dia + "/" + nome_diretorio + "/*.csv | awk '{print $9}'"
    print("args: " + format(args))
    proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    s_output, s_err = proc.communicate()
    print("s_output: " + format(s_output))
    all_dart_dirs = s_output.split()
    print("all_dart_dirs: " + format(all_dart_dirs))
    sed = "sed 's/|/\n/g'"
    for line in all_dart_dirs:
        args_trata = "hadoop fs -cat " + line + " |  " + repr(
            sed) + " | hadoop fs -appendToFile - /data/raw/FICO/PayloadAFMOutput/consolidado/" + ano + mes + dia + "/" + nome_diretorio + "/" + nome_arquivo + ".csv"
        args_trata = args_trata.replace('"', '')
        print("args_trata: " + format(args_trata))
        proc_trata = subprocess.Popen(args_trata, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        s_output_trata, s_err_trata = proc_trata.communicate()

    # Lista os arquivos existente no diretorio no qual o cabecalho esta sendo gerado para recuperar o cabecalho
    args_cabecalho = "hdfs dfs -ls " + v_blob_spark + "/data/raw/FICO/PayloadAFMOutput/consolidado/" + ano + mes + dia + "/" + nome_diretorio + "/cabecalho/*.csv | awk '{print $9}'"
    proc_cabecalho = subprocess.Popen(args_cabecalho, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    s_output_cabecalho, s_err_cabecalho = proc_cabecalho.communicate()
    all_dart_dirs_cabecalho = s_output_cabecalho.split()
    for line_cabecalho in all_dart_dirs_cabecalho:
        args_trata_cabecalho = 'hadoop fs -cat ' + line_cabecalho + ' |  sed -n "1p" | hadoop fs -put -f - /data/raw/FICO/PayloadAFMOutput/consolidado/' + ano + mes + dia + '/' + nome_diretorio + '/cabecalho/tratado.csv'
    proc_trata_cabecalho = subprocess.Popen(args_trata_cabecalho, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                            shell=True)
    s_output_trata_cabecalho, s_err_trata_cabecalho = proc_trata_cabecalho.communicate()

    # Gera o arquivo final
    args_final = "hadoop fs -cat " + v_blob_spark + "/data/raw/FICO/PayloadAFMOutput/consolidado/" + ano + mes + dia + "/" + nome_diretorio + "/cabecalho/tratado.csv abfs://sparkazredhdi-dev@azredlake.dfs.core.windows.net/data/raw/FICO/PayloadAFMOutput/consolidado/" + ano + mes + dia + "/" + nome_diretorio + "/" + nome_arquivo + ".csv | hadoop fs -put -f - abfs://sparkazredhdi-dev@azredlake.dfs.core.windows.net/data/raw/FICO/PayloadAFMOutput/consolidado/" + ano + mes + dia + "/" + nome_diretorio + "/" + nome_arquivo_final + ".csv"
    proc_final = subprocess.Popen(args_final, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    s_output_final, s_err_final = proc_final.communicate()


def parser_xml_cabecalho(string_xml, str_tag, str_tag_elemento_anterior, str_tag_elemento_anterior2):
    root = ET.fromstring(string_xml.encode('ISO-8859-1', errors='replace'))
    elemento_anterior = ""
    str_dados_header = ""
    iter = root.getiterator()
    for element in iter:
        if element.tag == "cabecalho":
            str_cabecalho_header = ""
            if element.getchildren():
                for child in element:
                    if child.tag is not None:
                        child_tag = child.tag.encode("utf-8")
                    str_cabecalho_header = str_cabecalho_header + child_tag + ";"
        if element.tag == str_tag and (
            elemento_anterior == str_tag_elemento_anterior or elemento_anterior == str_tag_elemento_anterior2):
            str_dados_header = ""
            if element.getchildren():
                for child in element:
                    if child.tag is not None:
                        child_tag = child.tag.encode("utf-8")
                    str_dados_header = str_dados_header + child_tag + ";"
            str_dados_header = str_dados_header[:-1]
            str_dados_header = str_cabecalho_header + str_dados_header
        elemento_anterior = element.tag
    return row_cabecalho(str_dados_header)


def parser_xml(string_xml, str_tag, str_tag_elemento_anterior, str_tag_elemento_anterior2):
    lst = []
    root = ET.fromstring(string_xml.encode('ISO-8859-1', errors='replace'))
    elemento_anterior = ""
    iter = root.getiterator()
    for element in iter:
        if element.tag == "cabecalho":
            str_cabecalho = ""
            if element.getchildren():
                for child in element:
                    child_text = ""
                    if child.text is not None:
                        child_text = child.text.encode("utf-8")
                    str_cabecalho = str_cabecalho + child_text + ";"
        if element.tag == str_tag and (
            elemento_anterior == str_tag_elemento_anterior or elemento_anterior == str_tag_elemento_anterior2):
            str_dados = ""
            if element.getchildren():
                for child in element:
                    child_text = ""
                    if child.text is not None:
                        child_text = child.text.encode("utf-8")
                    str_dados = str_dados + child_text + ";"
            str_dados = str_dados[:-1]
            str_dados = str_cabecalho + str_dados
            lst.append(str_dados)
        elemento_anterior = element.tag
    return lst


def parser_xml_consulta(string_xml):
    print("entrou parser_xml_consulta")
    root = ET.fromstring(string_xml.encode('ISO-8859-1', errors='replace'))
    iter = root.getiterator()
    for element in iter:
        print("element: " + element)
        if element.tag == "cabecalho":
            str_cabecalho = ""
            if element.getchildren():
                for child in element:
                    child_text = ""
                    if child.text is not None:
                        child_text = child.text.encode("utf-8")
                    str_cabecalho = str_cabecalho + child_text + ";"
        if element.tag == "numeroConsulta":
            str_consulta = ""
            element_text = ""
            if element.text is not None:
                element_text = element.text.encode("utf-8")
            str_consulta = element_text
            str_consulta = str_cabecalho + str_consulta
    return row_consulta(str_consulta)


def parser_xml_consulta_cabecalho(string_xml):
    root = ET.fromstring(string_xml.encode('ISO-8859-1', errors='replace'))
    iter = root.getiterator()
    for element in iter:
        if element.tag == "cabecalho":
            str_cabecalho = ""
            if element.getchildren():
                for child in element:
                    if child.tag is not None:
                        child_tag = child.tag.encode("utf-8")
                    str_cabecalho = str_cabecalho + child_tag + ";"
        if element.tag == "numeroConsulta":
            str_consulta = ""
            if element.tag is not None:
                element_tag = element.tag.encode("utf-8")
            str_consulta = element_tag
            str_consulta = str_cabecalho + str_consulta
    return row_consulta(str_consulta)


df3 = None
dt_inicio = str(datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
fmt = '%Y-%m-%d %H:%M:%S'
d1 = datetime.strptime(dt_inicio, fmt)
try:
    rdd = sc.wholeTextFiles("/data/raw/FICO/PayloadAFMOutput/detalhado/" + ano + mes + dia + "/*.txt")

    # applicationresponse
    lst_numeroConsulta = rdd.map(lambda (string_file): parser_xml_consulta(string_file[1]))
    df_numeroConsulta = spark.createDataFrame(lst_numeroConsulta)
    df_numeroConsulta.write.format("csv").option("sep", "|").mode("overwrite").save(
        "/data/raw/FICO/PayloadAFMOutput/consolidado/" + ano + mes + dia + "/ns2:header")

    # numeroConsulta_cabecalho = rdd.map(lambda (string_file): parser_xml_consulta_cabecalho(string_file[1]))
    # df_numeroConsulta_cabecalho = spark.createDataFrame(numeroConsulta_cabecalho)
    # df_numeroConsulta_cabecalho.write.format("csv").option("sep","|").mode("overwrite").save("/data/raw/FICO/PayloadAFMOutput/consolidado/"+ano+mes+dia+"/ns2:header/cabecalho")

    gera_arquivo("ns2:header", "ns2:ApplicationResponse", "arquivo_applicationresponse_final")
    #
    # #dadosCadastrais
    # lst_dadosCadastrais = rdd.map(lambda (string_file): parser_xml(string_file[1],"dadosCadastrais","banco_dadosCadastrais","nacionalidade")).filter(lambda x: x is not None).filter(lambda x: x != "").filter(lambda x: x).filter(bool)
    # if lst_dadosCadastrais.isEmpty() == False:
    #    df_dadosCadastrais = spark.createDataFrame(lst_dadosCadastrais)
    #    df_dadosCadastrais.write.format("csv").option("sep","|").mode("overwrite").save("/data/raw/FICO/PayloadAFMOutput/consolidado/"+ano+mes+dia+"/dadosCadastrais")
    #
    #    dadosCadastrais_cabecalho = rdd.map(lambda (string_file): parser_xml_cabecalho(string_file[1],"dadosCadastrais","banco_dadosCadastrais","nacionalidade")).filter(lambda x: x is not None).filter(lambda x: x != "").filter(lambda x: x).filter(bool)
    #    df_dadosCadastrais_cabecalho = spark.createDataFrame(dadosCadastrais_cabecalho)
    #    df_dadosCadastrais_cabecalho.write.format("csv").option("sep","|").mode("overwrite").save("/data/raw/FICO/PayloadAFMOutput/consolidado/"+ano+mes+dia+"/dadosCadastrais/cabecalho")
    #
    #    gera_arquivo("dadosCadastrais","arquivo_dadosCadastrais","arquivo_dadosCadastrais_final")

except Exception as ex:
    df3 = 'ERRO: {}'.format(str(ex))
    dt_fim = str(datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
    d2 = datetime.strptime(dt_fim, fmt)
    diff = d2 - d1
    tempoexec = str(diff)
    udf_f.gera_log_execucao(processo, dt_inicio, dt_fim, tempoexec, df3)
    spark.stop()
    exit(1)
else:
    df3 = ("succeeded")
    dt_fim = str(datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
    d2 = datetime.strptime(dt_fim, fmt)
    diff = d2 - d1
    tempoexec = str(diff)
    udf_f.gera_log_execucao(processo, dt_inicio, dt_fim, tempoexec, df3)
    spark.stop()
    exit(0)
