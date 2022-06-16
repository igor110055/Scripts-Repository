########################################
######## -*- coding: utf-8 -*-##########
#########ALL CHECK -FICO ###############
############# PARSE XML  ###############
############## Main ####################
##############Versão - 1.0##############
########################################

import subprocess
import xml.etree.ElementTree as ET
import csv
import datetime

from pyspark.sql import Row
from pyspark import SparkContext
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

row_cabecalho = Row('col1', 'col2')
row_consulta = Row('columnName1', 'columnName2', 'columnName3','columnName4','columnName5','columnName6','columnName7','columnName8')
# Configura as variaveis que serao utilizadas para montar o nome dos diretorios.
ano = str(datetime.datetime.now().year)
mes = str(datetime.datetime.now().month).zfill(2)
dia = str(datetime.datetime.now().day).zfill(2)

if __name__ == "__main__":
   sc = SparkContext('local')
   spark = SparkSession(sc)
   session = SparkSession.builder.appName("allcheck").getOrCreate()

def gera_arquivo(nome_diretorio,nome_arquivo,nome_arquivo_final):
   #Lista os arquivos existente no diretorio no qual o streaming esta sendo gerado.
   args = "hdfs dfs -ls abfs://sparkazredhdi-dev@azredlake.dfs.core.windows.net/data/raw/FICO/AllCheckAnaliseFullOutput/diario/"+ano+mes+dia+"/"+nome_diretorio+"/*.csv | awk '{print $9}'"
   proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
   s_output, s_err = proc.communicate()
   all_dart_dirs = s_output.split()
   sed = "sed 's/|/\n/g'"
   for line in all_dart_dirs:
      args_trata = "hadoop fs -cat "+line+" |  "+repr(sed)+" | hadoop fs -put -f - /data/raw/FICO/AllCheckAnaliseFullOutput/diario/"+ano+mes+dia+"/"+nome_diretorio+"/"+nome_arquivo+".csv"
      args_trata = args_trata.replace('"','')
   proc_trata = subprocess.Popen(args_trata, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
   s_output_trata, s_err_trata = proc_trata.communicate()

   #Lista os arquivos existente no diretorio no qual o cabecalho esta sendo gerado para recuperar o cabecalho
   args_cabecalho = "hdfs dfs -ls abfs://sparkazredhdi-dev@azredlake.dfs.core.windows.net/data/raw/FICO/AllCheckAnaliseFullOutput/diario/"+ano+mes+dia+"/"+nome_diretorio+"/cabecalho/*.csv | awk '{print $9}'"
   proc_cabecalho = subprocess.Popen(args_cabecalho, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
   s_output_cabecalho, s_err_cabecalho = proc_cabecalho.communicate()
   all_dart_dirs_cabecalho = s_output_cabecalho.split()
   for line_cabecalho in all_dart_dirs_cabecalho:
      args_trata_cabecalho = 'hadoop fs -cat '+line_cabecalho+' |  sed -n "1p" | hadoop fs -put -f - /data/raw/FICO/AllCheckAnaliseFullOutput/diario/'+ano+mes+dia+'/'+nome_diretorio+'/cabecalho/tratado.csv'
   proc_trata_cabecalho = subprocess.Popen(args_trata_cabecalho, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
   s_output_trata_cabecalho, s_err_trata_cabecalho = proc_trata_cabecalho.communicate()

   #Gera o arquivo final
   args_final = "hadoop fs -cat abfs://sparkazredhdi-dev@azredlake.dfs.core.windows.net/data/raw/FICO/AllCheckAnaliseFullOutput/diario/"+ano+mes+dia+"/"+nome_diretorio+"/cabecalho/tratado.csv abfs://sparkazredhdi-dev@azredlake.dfs.core.windows.net/data/raw/FICO/AllCheckAnaliseFullOutput/diario/"+ano+mes+dia+"/"+nome_diretorio+"/"+nome_arquivo+".csv | hadoop fs -put -f - abfs://sparkazredhdi-dev@azredlake.dfs.core.windows.net/data/raw/FICO/AllCheckAnaliseFullOutput/diario/"+ano+mes+dia+"/"+nome_diretorio+"/"+nome_arquivo_final+".csv"
   proc_final = subprocess.Popen(args_final, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
   s_output_final, s_err_final = proc_final.communicate()

def parser_xml_cabecalho(string_xml,str_tag,str_tag_elemento_anterior,str_tag_elemento_anterior2):
   root = ET.fromstring(string_xml.encode('ISO-8859-1', errors='replace'))
   elemento_anterior = ""
   str_dados_header=""
   iter = root.getiterator()
   for element in iter:
      if element.tag == "cabecalho":
         str_cabecalho_header = ""
         if element.getchildren():
            for child in element:
               if child.tag is not None:
                  child_tag = child.tag.encode("utf-8")
               str_cabecalho_header = str_cabecalho_header + child_tag + ";"
      if element.tag == str_tag and (elemento_anterior == str_tag_elemento_anterior or elemento_anterior == str_tag_elemento_anterior2):
         str_dados_header = ""
         if element.getchildren():
            for child in element:
               if child.tag is not None:
                  child_tag = child.tag.encode("utf-8")
               str_dados_header = str_dados_header + child_tag + ";"
         str_dados_header = str_dados_header[:-1]
         str_dados_header = str_cabecalho_header + str_dados_header
      elemento_anterior=element.tag
   return row_cabecalho(str_dados_header)

def parser_xml(string_xml,str_tag,str_tag_elemento_anterior,str_tag_elemento_anterior2):
   lst=[]
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
      if element.tag == str_tag and (elemento_anterior == str_tag_elemento_anterior or elemento_anterior == str_tag_elemento_anterior2):
         str_dados  = ""
         if element.getchildren():
            for child in element:
               child_text = ""
               if child.text is not None:
                  child_text = child.text.encode("utf-8")
               str_dados = str_dados + child_text + ";"
         str_dados = str_dados[:-1]
         str_dados = str_cabecalho + str_dados
         lst.append(str_dados) 
      elemento_anterior=element.tag
   return lst

def parser_xml_consulta(string_xml):
   root = ET.fromstring(string_xml.encode('ISO-8859-1', errors='replace'))
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

rdd = sc.wholeTextFiles("/data/raw/FICO/AllCheckAnaliseFullOutput/historico/"+ano+mes+dia+"/*.txt")

#numeroConsulta
lst_numeroConsulta = rdd.map(lambda (string_file): parser_xml_consulta(string_file[1]))
df_numeroConsulta = spark.createDataFrame(lst_numeroConsulta)
df_numeroConsulta.write.format("csv").option("sep","|").mode("overwrite").save("/data/raw/FICO/AllCheckAnaliseFullOutput/diario/"+ano+mes+dia+"/numeroConsulta")

numeroConsulta_cabecalho = rdd.map(lambda (string_file): parser_xml_consulta_cabecalho(string_file[1]))
df_numeroConsulta_cabecalho = spark.createDataFrame(numeroConsulta_cabecalho)
df_numeroConsulta_cabecalho.write.format("csv").option("sep","|").mode("overwrite").save("/data/raw/FICO/AllCheckAnaliseFullOutput/diario/"+ano+mes+dia+"/numeroConsulta/cabecalho")

gera_arquivo("numeroConsulta","arquivo_numeroConsulta","arquivo_numeroConsulta_final")

#dadosCadastrais
lst_dadosCadastrais = rdd.map(lambda (string_file): parser_xml(string_file[1],"dadosCadastrais","banco_dadosCadastrais","nacionalidade")).filter(lambda x: x is not None).filter(lambda x: x != "").filter(lambda x: x).filter(bool)
if lst_dadosCadastrais.isEmpty() == False:
   df_dadosCadastrais = spark.createDataFrame(lst_dadosCadastrais)
   df_dadosCadastrais.write.format("csv").option("sep","|").mode("overwrite").save("/data/raw/FICO/AllCheckAnaliseFullOutput/diario/"+ano+mes+dia+"/dadosCadastrais")

   dadosCadastrais_cabecalho = rdd.map(lambda (string_file): parser_xml_cabecalho(string_file[1],"dadosCadastrais","banco_dadosCadastrais","nacionalidade")).filter(lambda x: x is not None).filter(lambda x: x != "").filter(lambda x: x).filter(bool)
   df_dadosCadastrais_cabecalho = spark.createDataFrame(dadosCadastrais_cabecalho)
   df_dadosCadastrais_cabecalho.write.format("csv").option("sep","|").mode("overwrite").save("/data/raw/FICO/AllCheckAnaliseFullOutput/diario/"+ano+mes+dia+"/dadosCadastrais/cabecalho")

   gera_arquivo("dadosCadastrais","arquivo_dadosCadastrais","arquivo_dadosCadastrais_final")

#Consta Obito
lst_consta_obito = rdd.map(lambda (string_file): parser_xml(string_file[1],"consta_obito","banco_obitos","dataNascimento")).filter(lambda x: x is not None).filter(lambda x: x != "").filter(lambda x: x).filter(bool)
if lst_consta_obito.isEmpty() == False:
   df_consta_obito = spark.createDataFrame(lst_consta_obito)
   df_consta_obito.write.format("csv").option("sep","|").mode("overwrite").save("/data/raw/FICO/AllCheckAnaliseFullOutput/diario/"+ano+mes+dia+"/consta_obito")

   consta_obito_cabecalho = rdd.map(lambda (string_file): parser_xml_cabecalho(string_file[1],"consta_obito","banco_obitos","dataNascimento")).filter(lambda x: x is not None).filter(lambda x: x != "").filter(lambda x: x).filter(bool)
   df_consta_obito_cabecalho = spark.createDataFrame(consta_obito_cabecalho)
   df_consta_obito_cabecalho.write.format("csv").option("sep","|").mode("overwrite").save("/data/raw/FICO/AllCheckAnaliseFullOutput/diario/"+ano+mes+dia+"/consta_obito/cabecalho")

   gera_arquivo("consta_obito","arquivo_consta_obito","arquivo_consta_obito_final")

#NovoTelefone
lst_telefone = rdd.map(lambda (string_file): parser_xml(string_file[1],"novo_telefone","banco_telefone","dataInstalacao")).filter(lambda x: x is not None).filter(lambda x: x != "").filter(lambda x: x).filter(bool)
if lst_telefone.isEmpty() == False:
   df_novo_telefone = spark.createDataFrame(lst_telefone)
   df_novo_telefone.write.format("csv").option("sep","|").mode("overwrite").save("/data/raw/FICO/AllCheckAnaliseFullOutput/diario/"+ano+mes+dia+"/novoTelefone")

   telefone_cabecalho = rdd.map(lambda (string_file): parser_xml_cabecalho(string_file[1],"novo_telefone","banco_telefone","dataInstalacao")).filter(lambda x: x is not None).filter(lambda x: x != "").filter(lambda x: x).filter(bool)
   df_novo_telefone_cabecalho = spark.createDataFrame(telefone_cabecalho)
   df_novo_telefone_cabecalho.write.format("csv").option("sep","|").mode("overwrite").save("/data/raw/FICO/AllCheckAnaliseFullOutput/diario/"+ano+mes+dia+"/novoTelefone/cabecalho")

   gera_arquivo("novoTelefone","arquivo_novo_telefone","arquivo_novo_telefone_final")

#novo_socios
lst_novo_socios = rdd.map(lambda (string_file): parser_xml(string_file[1],"novo_socios","banco_sociedades","socios_da_empresa")).filter(lambda x: x is not None).filter(lambda x: x != "").filter(lambda x: x).filter(bool)
if lst_novo_socios.isEmpty() == False:
   df_novo_socios = spark.createDataFrame(lst_consta_obito)
   df_novo_socios.write.format("csv").option("sep","|").mode("overwrite").save("/data/raw/FICO/AllCheckAnaliseFullOutput/diario/"+ano+mes+dia+"/novo_socios")

   novo_socios_cabecalho = rdd.map(lambda (string_file): parser_xml_cabecalho(string_file[1],"novo_socios","banco_sociedades","socios_da_empresa")).filter(lambda x: x is not None).filter(lambda x: x != "").filter(lambda x: x).filter(bool)
   df_novo_socios_cabecalho = spark.createDataFrame(novo_socios_cabecalho)
   df_novo_socios_cabecalho.write.format("csv").option("sep","|").mode("overwrite").save("/data/raw/FICO/AllCheckAnaliseFullOutput/diario/"+ano+mes+dia+"/novo_socios/cabecalho")

   gera_arquivo("novo_socios","arquivo_novo_socios","arquivo_novo_socios_final")

#empresas
lst_empresas = rdd.map(lambda (string_file): parser_xml(string_file[1],"empresas","banco_empresas","dataSituacao_especial")).filter(lambda x: x is not None).filter(lambda x: x != "").filter(lambda x: x).filter(bool)
if lst_empresas.isEmpty() == False:
   df_empresas = spark.createDataFrame(lst_empresas)
   df_empresas.write.format("csv").option("sep","|").mode("overwrite").save("/data/raw/FICO/AllCheckAnaliseFullOutput/diario/"+ano+mes+dia+"/empresas")

   empresas_cabecalho = rdd.map(lambda (string_file): parser_xml_cabecalho(string_file[1],"empresas","banco_empresas","dataSituacao_especial")).filter(lambda x: x is not None).filter(lambda x: x != "").filter(lambda x: x).filter(bool)
   df_empresas_cabecalho = spark.createDataFrame(empresas_cabecalho)
   df_empresas_cabecalho.write.format("csv").option("sep","|").mode("overwrite").save("/data/raw/FICO/AllCheckAnaliseFullOutput/diario/"+ano+mes+dia+"/empresas/cabecalho")

   gera_arquivo("empresas","arquivo_empresas","arquivo_empresas_final")

#novo_veiculo
lst_novo_veiculo = rdd.map(lambda (string_file): parser_xml(string_file[1],"novo_veiculo","banco_veiculos","ano_base")).filter(lambda x: x is not None).filter(lambda x: x != "").filter(lambda x: x).filter(bool)
if lst_novo_veiculo.isEmpty() == False:
   df_novo_veiculo = spark.createDataFrame(lst_novo_veiculo)
   df_novo_veiculo.write.format("csv").option("sep","|").mode("overwrite").save("/data/raw/FICO/AllCheckAnaliseFullOutput/diario/"+ano+mes+dia+"/novo_veiculo")

   novo_veiculo_cabecalho = rdd.map(lambda (string_file): parser_xml_cabecalho(string_file[1],"novo_veiculo","banco_veiculos","ano_base")).filter(lambda x: x is not None).filter(lambda x: x != "").filter(lambda x: x).filter(bool)
   df_novo_veiculo_cabecalho = spark.createDataFrame(novo_veiculo_cabecalho)
   df_novo_veiculo_cabecalho.write.format("csv").option("sep","|").mode("overwrite").save("/data/raw/FICO/AllCheckAnaliseFullOutput/diario/"+ano+mes+dia+"/novo_veiculo/cabecalho")

   gera_arquivo("novo_veiculo","arquivo_novo_veiculo","arquivo_novo_veiculo_final")

#novo_ccf
lst_novo_ccf = rdd.map(lambda (string_file): parser_xml(string_file[1],"novo_ccf","banco_ccf","dataDaUltimaOcorrencia")).filter(lambda x: x is not None).filter(lambda x: x != "").filter(lambda x: x).filter(bool)
if lst_novo_ccf.isEmpty() == False:
   df_novo_ccf = spark.createDataFrame(lst_novo_ccf)
   df_novo_ccf.write.format("csv").option("sep","|").mode("overwrite").save("/data/raw/FICO/AllCheckAnaliseFullOutput/diario/"+ano+mes+dia+"/novo_ccf")

   novo_ccf_cabecalho = rdd.map(lambda (string_file): parser_xml_cabecalho(string_file[1],"novo_ccf","banco_ccf","dataDaUltimaOcorrencia")).filter(lambda x: x is not None).filter(lambda x: x != "").filter(lambda x: x).filter(bool)
   df_novo_ccf_cabecalho = spark.createDataFrame(novo_ccf_cabecalho)
   df_novo_ccf_cabecalho.write.format("csv").option("sep","|").mode("overwrite").save("/data/raw/FICO/AllCheckAnaliseFullOutput/diario/"+ano+mes+dia+"/novo_ccf/cabecalho")

   gera_arquivo("novo_ccf","arquivo_novo_ccf","arquivo_novo_ccf_final")

#novo_dadosBancarios
lst_novo_dadosBancarios = rdd.map(lambda (string_file): parser_xml(string_file[1],"novo_dadosBancarios","dadosBancarios","agencia")).filter(lambda x: x is not None).filter(lambda x: x != "").filter(lambda x: x).filter(bool)
if lst_novo_dadosBancarios.isEmpty() == False:
   df_novo_dadosBancarios = spark.createDataFrame(lst_novo_dadosBancarios)
   df_novo_dadosBancarios.write.format("csv").option("sep","|").mode("overwrite").save("/data/raw/FICO/AllCheckAnaliseFullOutput/diario/"+ano+mes+dia+"/novo_dadosBancarios")

   novo_dadosBancarios_cabecalho = rdd.map(lambda (string_file): parser_xml_cabecalho(string_file[1],"novo_dadosBancarios","dadosBancarios","agencia")).filter(lambda x: x is not None).filter(lambda x: x != "").filter(lambda x: x).filter(bool)
   df_novo_dadosBancarios_cabecalho = spark.createDataFrame(novo_dadosBancarios_cabecalho)
   df_novo_dadosBancarios_cabecalho.write.format("csv").option("sep","|").mode("overwrite").save("/data/raw/FICO/AllCheckAnaliseFullOutput/diario/"+ano+mes+dia+"/novo_dadosBancarios/cabecalho")

   gera_arquivo("novo_dadosBancarios","arquivo_novo_dadosBancarios","arquivo_novo_dadosBancarios_final")

#novo_historicoTrabalho
lst_novo_historicoTrabalho = rdd.map(lambda (string_file): parser_xml(string_file[1],"novo_historicoTrabalho","historicoTrabalho","nomeEmpresa")).filter(lambda x: x is not None).filter(lambda x: x != "").filter(lambda x: x).filter(bool)
if lst_novo_historicoTrabalho.isEmpty() == False:
   df_novo_historicoTrabalho = spark.createDataFrame(lst_novo_historicoTrabalho)
   df_novo_historicoTrabalho.write.format("csv").option("sep","|").mode("overwrite").save("/data/raw/FICO/AllCheckAnaliseFullOutput/diario/"+ano+mes+dia+"/novo_historicoTrabalho")

   novo_historicoTrabalho_cabecalho = rdd.map(lambda (string_file): parser_xml_cabecalho(string_file[1],"novo_historicoTrabalho","historicoTrabalho","nomeEmpresa")).filter(lambda x: x is not None).filter(lambda x: x != "").filter(lambda x: x).filter(bool)
   df_novo_historicoTrabalho_cabecalho = spark.createDataFrame(novo_historicoTrabalho_cabecalho)
   df_novo_historicoTrabalho_cabecalho.write.format("csv").option("sep","|").mode("overwrite").save("/data/raw/FICO/AllCheckAnaliseFullOutput/diario/"+ano+mes+dia+"/novo_historicoTrabalho/cabecalho")

   gera_arquivo("novo_historicoTrabalho","arquivo_novo_historicoTrabalho","arquivo_novo_historicoTrabalho_final")

#registro
lst_registro = rdd.map(lambda (string_file): parser_xml(string_file[1],"registro","redeVigiada","telefone")).filter(lambda x: x is not None).filter(lambda x: x != "").filter(lambda x: x).filter(bool)
if lst_registro.isEmpty() == False:
   df_registro = spark.createDataFrame(lst_registro)
   df_registro.write.format("csv").option("sep","|").mode("overwrite").save("/data/raw/FICO/AllCheckAnaliseFullOutput/diario/"+ano+mes+dia+"/registro")

   registro_cabecalho = rdd.map(lambda (string_file): parser_xml_cabecalho(string_file[1],"registro","redeVigiada","telefone")).filter(lambda x: x is not None).filter(lambda x: x != "").filter(lambda x: x).filter(bool)
   df_registro_cabecalho = spark.createDataFrame(registro_cabecalho)
   df_registro_cabecalho.write.format("csv").option("sep","|").mode("overwrite").save("/data/raw/FICO/AllCheckAnaliseFullOutput/diario/"+ano+mes+dia+"/registro/cabecalho")

   gera_arquivo("registro","arquivo_registro","arquivo_registro_final")

#consta_ppe
lst_consta_ppe = rdd.map(lambda (string_file): parser_xml(string_file[1],"consta_ppe","banco_ppe","instituicao")).filter(lambda x: x is not None).filter(lambda x: x != "").filter(lambda x: x).filter(bool)
if lst_consta_ppe.isEmpty() == False:
   df_consta_ppe = spark.createDataFrame(lst_consta_ppe)
   df_consta_ppe.write.format("csv").option("sep","|").mode("overwrite").save("/data/raw/FICO/AllCheckAnaliseFullOutput/diario/"+ano+mes+dia+"/consta_ppe")

   consta_ppe_cabecalho = rdd.map(lambda (string_file): parser_xml_cabecalho(string_file[1],"consta_ppe","banco_ppe","instituicao")).filter(lambda x: x is not None).filter(lambda x: x != "").filter(lambda x: x).filter(bool)
   df_consta_ppe_cabecalho = spark.createDataFrame(consta_ppe_cabecalho)
   df_consta_ppe_cabecalho.write.format("csv").option("sep","|").mode("overwrite").save("/data/raw/FICO/AllCheckAnaliseFullOutput/diario/"+ano+mes+dia+"/consta_ppe/cabecalho")

   gera_arquivo("consta_ppe","arquivo_consta_ppe","arquivo_consta_ppe_final")

#consta_ppebens
lst_consta_ppebens = rdd.map(lambda (string_file): parser_xml(string_file[1],"consta_ppebens","banco_ppebens","data_atualizacao")).filter(lambda x: x is not None).filter(lambda x: x != "").filter(lambda x: x).filter(bool)
if lst_consta_ppebens.isEmpty() == False:
   df_consta_ppebens = spark.createDataFrame(lst_consta_ppebens)
   df_consta_ppebens.write.format("csv").option("sep","|").mode("overwrite").save("/data/raw/FICO/AllCheckAnaliseFullOutput/diario/"+ano+mes+dia+"/consta_ppebens")

   consta_ppebens_cabecalho = rdd.map(lambda (string_file): parser_xml_cabecalho(string_file[1],"consta_ppebens","banco_ppebens","data_atualizacao")).filter(lambda x: x is not None).filter(lambda x: x != "").filter(lambda x: x).filter(bool)
   df_consta_ppebens_cabecalho = spark.createDataFrame(consta_ppebens_cabecalho)
   df_consta_ppebens_cabecalho.write.format("csv").option("sep","|").mode("overwrite").save("/data/raw/FICO/AllCheckAnaliseFullOutput/diario/"+ano+mes+dia+"/consta_ppebens/cabecalho")

   gera_arquivo("consta_ppebens","arquivo_consta_ppebens","arquivo_consta_ppebens_final")

#consta_tcu
lst_consta_tcu = rdd.map(lambda (string_file): parser_xml(string_file[1],"consta_tcu","banco_tcu","data_julgamento")).filter(lambda x: x is not None).filter(lambda x: x != "").filter(lambda x: x).filter(bool)
if lst_consta_tcu.isEmpty() == False:
   df_consta_tcu = spark.createDataFrame(lst_consta_tcu)
   df_consta_tcu.write.format("csv").option("sep","|").mode("overwrite").save("/data/raw/FICO/AllCheckAnaliseFullOutput/diario/"+ano+mes+dia+"/consta_tcu")

   consta_tcu_cabecalho = rdd.map(lambda (string_file): parser_xml_cabecalho(string_file[1],"consta_tcu","banco_tcu","data_julgamento")).filter(lambda x: x is not None).filter(lambda x: x != "").filter(lambda x: x).filter(bool)
   df_consta_tcu_cabecalho = spark.createDataFrame(consta_tcu_cabecalho)
   df_consta_tcu_cabecalho.write.format("csv").option("sep","|").mode("overwrite").save("/data/raw/FICO/AllCheckAnaliseFullOutput/diario/"+ano+mes+dia+"/consta_tcu/cabecalho")
   gera_arquivo("consta_tcu","arquivo_consta_tcu","arquivo_consta_tcu_final")

#consta_ceis
lst_consta_ceis = rdd.map(lambda (string_file): parser_xml(string_file[1],"consta_ceis","banco_ceis","data_final")).filter(lambda x: x is not None).filter(lambda x: x != "").filter(lambda x: x).filter(bool)
if lst_consta_ceis.isEmpty() == False:
   df_consta_ceis = spark.createDataFrame(lst_consta_ceis)
   df_consta_ceis.write.format("csv").option("sep","|").mode("overwrite").save("/data/raw/FICO/AllCheckAnaliseFullOutput/diario/"+ano+mes+dia+"/consta_ceis")

   consta_ceis_cabecalho = rdd.map(lambda (string_file): parser_xml_cabecalho(string_file[1],"consta_ceis","banco_ceis","data_final")).filter(lambda x: x is not None).filter(lambda x: x != "").filter(lambda x: x).filter(bool)
   df_consta_ceis_cabecalho = spark.createDataFrame(consta_ceis_cabecalho)
   df_consta_ceis_cabecalho.write.format("csv").option("sep","|").mode("overwrite").save("/data/raw/FICO/AllCheckAnaliseFullOutput/diario/"+ano+mes+dia+"/consta_ceis/cabecalho")

   gera_arquivo("consta_ceis","arquivo_consta_ceis","arquivo_consta_ceis_final")

#consta_cepim
lst_consta_cepim = rdd.map(lambda (string_file): parser_xml(string_file[1],"consta_cepim","banco_cepim","motivo_impedimento")).filter(lambda x: x is not None).filter(lambda x: x != "").filter(lambda x: x).filter(bool)
if lst_consta_cepim.isEmpty() == False:
   df_consta_cepim = spark.createDataFrame(lst_consta_cepim)
   df_consta_cepim.write.format("csv").option("sep","|").mode("overwrite").save("/data/raw/FICO/AllCheckAnaliseFullOutput/diario/"+ano+mes+dia+"/consta_cepim")

   consta_cepim_cabecalho = rdd.map(lambda (string_file): parser_xml_cabecalho(string_file[1],"consta_cepim","banco_cepim","motivo_impedimento")).filter(lambda x: x is not None).filter(lambda x: x != "").filter(lambda x: x).filter(bool)
   df_consta_cepim_cabecalho = spark.createDataFrame(consta_cepim_cabecalho)
   df_consta_cepim_cabecalho.write.format("csv").option("sep","|").mode("overwrite").save("/data/raw/FICO/AllCheckAnaliseFullOutput/diario/"+ano+mes+dia+"/consta_cepim/cabecalho")

   gera_arquivo("consta_cepim","arquivo_consta_cepim","arquivo_consta_cepim_final")

#consta_mte
lst_consta_mte = rdd.map(lambda (string_file): parser_xml(string_file[1],"consta_mte","banco_mte","num_trabalhadores")).filter(lambda x: x is not None).filter(lambda x: x != "").filter(lambda x: x).filter(bool)
if lst_consta_mte.isEmpty() == False:
   df_consta_mte = spark.createDataFrame(lst_consta_mte)
   df_consta_mte.write.format("csv").option("sep","|").mode("overwrite").save("/data/raw/FICO/AllCheckAnaliseFullOutput/diario/"+ano+mes+dia+"/consta_mte")

   consta_mte_cabecalho = rdd.map(lambda (string_file): parser_xml_cabecalho(string_file[1],"consta_mte","banco_mte","num_trabalhadores")).filter(lambda x: x is not None).filter(lambda x: x != "").filter(lambda x: x).filter(bool)
   df_consta_mte_cabecalho = spark.createDataFrame(consta_mte_cabecalho)
   df_consta_mte_cabecalho.write.format("csv").option("sep","|").mode("overwrite").save("/data/raw/FICO/AllCheckAnaliseFullOutput/diario/"+ano+mes+dia+"/consta_mte/cabecalho")

   gera_arquivo("consta_mte","arquivo_consta_mte","arquivo_consta_mte_final")

#consta_parentesco
lst_consta_parentesco = rdd.map(lambda (string_file): parser_xml(string_file[1],"consta_parentesco","banco_parentesco","grau_parentesco")).filter(lambda x: x is not None).filter(lambda x: x != "").filter(lambda x: x).filter(bool)
if lst_consta_parentesco.isEmpty() == False:
   df_consta_parentesco = spark.createDataFrame(lst_consta_parentesco)
   df_consta_parentesco.write.format("csv").option("sep","|").mode("overwrite").save("/data/raw/FICO/AllCheckAnaliseFullOutput/diario/"+ano+mes+dia+"/consta_parentesco")

   consta_parentesco_cabecalho = rdd.map(lambda (string_file): parser_xml_cabecalho(string_file[1],"consta_parentesco","banco_parentesco","grau_parentesco")).filter(lambda x: x is not None).filter(lambda x: x != "").filter(lambda x: x).filter(bool)
   df_consta_parentesco_cabecalho = spark.createDataFrame(consta_parentesco_cabecalho)
   df_consta_parentesco_cabecalho.write.format("csv").option("sep","|").mode("overwrite").save("/data/raw/FICO/AllCheckAnaliseFullOutput/diario/"+ano+mes+dia+"/consta_parentesco/cabecalho")

   gera_arquivo("consta_parentesco","arquivo_consta_parentesco","arquivo_consta_parentesco_final")

#consta_beneficio
lst_consta_beneficio = rdd.map(lambda (string_file): parser_xml(string_file[1],"consta_beneficio","banco_beneficiosInss","descricaoBeneficio")).filter(lambda x: x is not None).filter(lambda x: x != "").filter(lambda x: x).filter(bool)
if lst_consta_beneficio.isEmpty() == False:
   df_consta_beneficio = spark.createDataFrame(lst_consta_beneficio)
   df_consta_beneficio.write.format("csv").option("sep","|").mode("overwrite").save("/data/raw/FICO/AllCheckAnaliseFullOutput/diario/"+ano+mes+dia+"/consta_beneficio")

   consta_beneficio_cabecalho = rdd.map(lambda (string_file): parser_xml_cabecalho(string_file[1],"consta_beneficio","banco_beneficiosInss","descricaoBeneficio")).filter(lambda x: x is not None).filter(lambda x: x != "").filter(lambda x: x).filter(bool)
   df_consta_beneficio_cabecalho = spark.createDataFrame(consta_beneficio_cabecalho)
   df_consta_beneficio_cabecalho.write.format("csv").option("sep","|").mode("overwrite").save("/data/raw/FICO/AllCheckAnaliseFullOutput/diario/"+ano+mes+dia+"/consta_beneficio/cabecalho")

   gera_arquivo("consta_beneficio","arquivo_consta_beneficio","arquivo_consta_beneficio_final")

#consta_servidor_federal
lst_consta_servidor_federal = rdd.map(lambda (string_file): parser_xml(string_file[1],"consta_servidor_federal","banco_servidor_federal","servidor")).filter(lambda x: x is not None).filter(lambda x: x != "").filter(lambda x: x).filter(bool)
if lst_consta_servidor_federal.isEmpty() == False:
   df_consta_servidor_federal = spark.createDataFrame(lst_consta_servidor_federal)
   df_consta_servidor_federal.write.format("csv").option("sep","|").mode("overwrite").save("/data/raw/FICO/AllCheckAnaliseFullOutput/diario/"+ano+mes+dia+"/consta_servidor_federal")

   consta_servidor_federal_cabecalho = rdd.map(lambda (string_file): parser_xml_cabecalho(string_file[1],"consta_servidor_federal","banco_servidor_federal","servidor")).filter(lambda x: x is not None).filter(lambda x: x != "").filter(lambda x: x).filter(bool)
   df_consta_servidor_federal_cabecalho = spark.createDataFrame(consta_servidor_federal_cabecalho)
   df_consta_servidor_federal_cabecalho.write.format("csv").option("sep","|").mode("overwrite").save("/data/raw/FICO/AllCheckAnaliseFullOutput/diario/"+ano+mes+dia+"/consta_servidor_federal/cabecalho")

   gera_arquivo("consta_servidor_federal","arquivo_consta_servidor_federal","arquivo_consta_servidor_federal_final")

#consta_ceaf
lst_consta_ceaf = rdd.map(lambda (string_file): parser_xml(string_file[1],"consta_ceaf","banco_ceaf","data_public_punicao")).filter(lambda x: x is not None).filter(lambda x: x != "").filter(lambda x: x).filter(bool)
if lst_consta_ceaf.isEmpty() == False:
   df_consta_ceaf = spark.createDataFrame(lst_consta_ceaf)
   df_consta_ceaf.write.format("csv").option("sep","|").mode("overwrite").save("/data/raw/FICO/AllCheckAnaliseFullOutput/diario/"+ano+mes+dia+"/consta_ceaf")

   consta_ceaf_cabecalho = rdd.map(lambda (string_file): parser_xml_cabecalho(string_file[1],"consta_ceaf","banco_ceaf","data_public_punicao")).filter(lambda x: x is not None).filter(lambda x: x != "").filter(lambda x: x).filter(bool)
   df_consta_ceaf_cabecalho = spark.createDataFrame(consta_ceaf_cabecalho)
   df_consta_ceaf_cabecalho.write.format("csv").option("sep","|").mode("overwrite").save("/data/raw/FICO/AllCheckAnaliseFullOutput/diario/"+ano+mes+dia+"/consta_ceaf/cabecalho")

   gera_arquivo("consta_ceaf","arquivo_consta_ceaf","arquivo_consta_ceaf_final")

#consta_possivel_morador
lst_consta_possivel_morador = rdd.map(lambda (string_file): parser_xml(string_file[1],"consta_possivel_morador","banco_possivel_morador","nome_morador")).filter(lambda x: x is not None).filter(lambda x: x != "").filter(lambda x: x).filter(bool)
if lst_consta_possivel_morador.isEmpty() == False:
   df_consta_possivel_morador = spark.createDataFrame(lst_consta_possivel_morador)
   df_consta_possivel_morador.write.format("csv").option("sep","|").mode("overwrite").save("/data/raw/FICO/AllCheckAnaliseFullOutput/diario/"+ano+mes+dia+"/consta_possivel_morador")

   consta_possivel_morador_cabecalho = rdd.map(lambda (string_file): parser_xml_cabecalho(string_file[1],"consta_possivel_morador","banco_possivel_morador","nome_morador")).filter(lambda x: x is not None).filter(lambda x: x != "").filter(lambda x: x).filter(bool)
   df_consta_possivel_morador_cabecalho = spark.createDataFrame(consta_possivel_morador_cabecalho)
   df_consta_possivel_morador_cabecalho.write.format("csv").option("sep","|").mode("overwrite").save("/data/raw/FICO/AllCheckAnaliseFullOutput/diario/"+ano+mes+dia+"/consta_possivel_morador/cabecalho")

   gera_arquivo("consta_possivel_morador","arquivo_consta_possivel_morador","arquivo_consta_possivel_morador_final")

#consta_email
lst_consta_email = rdd.map(lambda (string_file): parser_xml(string_file[1],"consta_email","banco_email","email")).filter(lambda x: x is not None).filter(lambda x: x != "").filter(lambda x: x).filter(bool)
if lst_consta_email.isEmpty() == False:
   df_consta_email = spark.createDataFrame(lst_consta_email)
   df_consta_email.write.format("csv").option("sep","|").mode("overwrite").save("/data/raw/FICO/AllCheckAnaliseFullOutput/diario/"+ano+mes+dia+"/consta_email")

   consta_email_cabecalho = rdd.map(lambda (string_file): parser_xml_cabecalho(string_file[1],"consta_email","banco_email","email")).filter(lambda x: x is not None).filter(lambda x: x != "").filter(lambda x: x).filter(bool)
   df_consta_email_cabecalho = spark.createDataFrame(consta_email_cabecalho)
   df_consta_email_cabecalho.write.format("csv").option("sep","|").mode("overwrite").save("/data/raw/FICO/AllCheckAnaliseFullOutput/diario/"+ano+mes+dia+"/consta_email/cabecalho")

   gera_arquivo("consta_email","arquivo_consta_email","arquivo_consta_email_final")

#consta_parentesco
lst_consta_ppeparentesco = rdd.map(lambda (string_file): parser_xml(string_file[1],"consta_parentesco","banco_ppeParentesco","grau_parentesco")).filter(lambda x: x is not None).filter(lambda x: x != "").filter(lambda x: x).filter(bool)
if lst_consta_ppeparentesco.isEmpty() == False:
   df_consta_ppeparentesco = spark.createDataFrame(lst_consta_ppeparentesco)
   df_consta_ppeparentesco.write.format("csv").option("sep","|").mode("overwrite").save("/data/raw/FICO/AllCheckAnaliseFullOutput/diario/"+ano+mes+dia+"/consta_ppeparentesco")

   consta_ppeparentesco_cabecalho = rdd.map(lambda (string_file): parser_xml_cabecalho(string_file[1],"consta_parentesco","banco_ppeParentesco","grau_parentesco")).filter(lambda x: x is not None).filter(lambda x: x != "").filter(lambda x: x).filter(bool)
   df_consta_ppeparentesco_cabecalho = spark.createDataFrame(consta_ppeparentesco_cabecalho)
   df_consta_ppeparentesco_cabecalho.write.format("csv").option("sep","|").mode("overwrite").save("/data/raw/FICO/AllCheckAnaliseFullOutput/diario/"+ano+mes+dia+"/consta_ppeparentesco/cabecalho")

   gera_arquivo("consta_ppeparentesco","arquivo_consta_ppeparentesco","arquivo_consta_ppeparentesco_final")

#novo_ancine
lst_novo_ancine = rdd.map(lambda (string_file): parser_xml(string_file[1],"novo_ancine","banco_ancine","estado")).filter(lambda x: x is not None).filter(lambda x: x != "").filter(lambda x: x).filter(bool)
if lst_novo_ancine.isEmpty() == False:
   df_novo_ancine = spark.createDataFrame(lst_novo_ancine)
   df_novo_ancine.write.format("csv").option("sep","|").mode("overwrite").save("/data/raw/FICO/AllCheckAnaliseFullOutput/diario/"+ano+mes+dia+"/novo_ancine")

   novo_ancine_cabecalho = rdd.map(lambda (string_file): parser_xml_cabecalho(string_file[1],"novo_ancine","banco_ancine","estado")).filter(lambda x: x is not None).filter(lambda x: x != "").filter(lambda x: x).filter(bool)
   df_novo_ancine_cabecalho = spark.createDataFrame(novo_ancine_cabecalho)
   df_novo_ancine_cabecalho.write.format("csv").option("sep","|").mode("overwrite").save("/data/raw/FICO/AllCheckAnaliseFullOutput/diario/"+ano+mes+dia+"/novo_ancine/cabecalho")

   gera_arquivo("novo_ancine","arquivo_novo_ancine","arquivo_novo_ancine_final")

#novo_anttEmpresas
lst_novo_anttEmpresas = rdd.map(lambda (string_file): parser_xml(string_file[1],"novo_anttEmpresas","banco_anttEmpresas","endereco")).filter(lambda x: x is not None).filter(lambda x: x != "").filter(lambda x: x).filter(bool)
if lst_novo_anttEmpresas.isEmpty() == False:
   df_novo_anttEmpresas = spark.createDataFrame(lst_novo_anttEmpresas)
   df_novo_anttEmpresas.write.format("csv").option("sep","|").mode("overwrite").save("/data/raw/FICO/AllCheckAnaliseFullOutput/diario/"+ano+mes+dia+"/novo_anttEmpresas")

   novo_anttEmpresas_cabecalho = rdd.map(lambda (string_file): parser_xml_cabecalho(string_file[1],"novo_anttEmpresas","banco_anttEmpresas","endereco")).filter(lambda x: x is not None).filter(lambda x: x != "").filter(lambda x: x).filter(bool)
   df_novo_anttEmpresas_cabecalho = spark.createDataFrame(novo_anttEmpresas_cabecalho)
   df_novo_anttEmpresas_cabecalho.write.format("csv").option("sep","|").mode("overwrite").save("/data/raw/FICO/AllCheckAnaliseFullOutput/diario/"+ano+mes+dia+"/novo_anttEmpresas/cabecalho")

   gera_arquivo("novo_anttEmpresas","arquivo_novo_anttEmpresas","arquivo_novo_anttEmpresas_final")

#novo_ans
lst_novo_ans = rdd.map(lambda (string_file): parser_xml(string_file[1],"novo_ans","banco_ans","modalidade")).filter(lambda x: x is not None).filter(lambda x: x != "").filter(lambda x: x).filter(bool)
if lst_novo_ans.isEmpty() == False:
   df_novo_ans = spark.createDataFrame(lst_novo_ans)
   df_novo_ans.write.format("csv").option("sep","|").mode("overwrite").save("/data/raw/FICO/AllCheckAnaliseFullOutput/diario/"+ano+mes+dia+"/novo_ans")

   novo_ans_cabecalho = rdd.map(lambda (string_file): parser_xml_cabecalho(string_file[1],"novo_ans","banco_ans","modalidade")).filter(lambda x: x is not None).filter(lambda x: x != "").filter(lambda x: x).filter(bool)
   df_novo_ans_cabecalho = spark.createDataFrame(novo_ans_cabecalho)
   df_novo_ans_cabecalho.write.format("csv").option("sep","|").mode("overwrite").save("/data/raw/FICO/AllCheckAnaliseFullOutput/diario/"+ano+mes+dia+"/novo_ans/cabecalho")

   gera_arquivo("novo_ans","arquivo_novo_ans","arquivo_novo_ans_final")

#novo_anvisa
lst_novo_anvisa = rdd.map(lambda (string_file): parser_xml(string_file[1],"novo_anvisa","banco_anvisa","situacao")).filter(lambda x: x is not None).filter(lambda x: x != "").filter(lambda x: x).filter(bool)
if lst_novo_anvisa.isEmpty() == False:
   df_novo_anvisa = spark.createDataFrame(lst_novo_anvisa)
   df_novo_anvisa.write.format("csv").option("sep","|").mode("overwrite").save("/data/raw/FICO/AllCheckAnaliseFullOutput/diario/"+ano+mes+dia+"/novo_anvisa")

   novo_anvisa_cabecalho = rdd.map(lambda (string_file): parser_xml_cabecalho(string_file[1],"novo_anvisa","banco_anvisa","situacao")).filter(lambda x: x is not None).filter(lambda x: x != "").filter(lambda x: x).filter(bool)
   df_novo_anvisa_cabecalho = spark.createDataFrame(novo_anvisa_cabecalho)
   df_novo_anvisa_cabecalho.write.format("csv").option("sep","|").mode("overwrite").save("/data/raw/FICO/AllCheckAnaliseFullOutput/diario/"+ano+mes+dia+"/novo_anvisa/cabecalho")

   gera_arquivo("novo_anvisa","arquivo_novo_anvisa","arquivo_novo_anvisa_final")

#novo_rabAnac
lst_novo_rabAnac = rdd.map(lambda (string_file): parser_xml(string_file[1],"novo_rabAnac","banco_rabAnac","nomeFabricante")).filter(lambda x: x is not None).filter(lambda x: x != "").filter(lambda x: x).filter(bool)
if lst_novo_rabAnac.isEmpty() == False:
   df_novo_rabAnac = spark.createDataFrame(lst_novo_rabAnac)
   df_novo_rabAnac.write.format("csv").option("sep","|").mode("overwrite").save("/data/raw/FICO/AllCheckAnaliseFullOutput/diario/"+ano+mes+dia+"/novo_rabAnac")

   novo_rabAnac_cabecalho = rdd.map(lambda (string_file): parser_xml_cabecalho(string_file[1],"novo_rabAnac","banco_rabAnac","nomeFabricante")).filter(lambda x: x is not None).filter(lambda x: x != "").filter(lambda x: x).filter(bool)
   df_novo_rabAnac_cabecalho = spark.createDataFrame(novo_rabAnac_cabecalho)
   df_novo_rabAnac_cabecalho.write.format("csv").option("sep","|").mode("overwrite").save("/data/raw/FICO/AllCheckAnaliseFullOutput/diario/"+ano+mes+dia+"/novo_rabAnac/cabecalho")

   gera_arquivo("novo_rabAnac","arquivo_novo_rabAnac","arquivo_novo_rabAnac_final")

#novo_cnes
lst_novo_cnes = rdd.map(lambda (string_file): parser_xml(string_file[1],"novo_cnes","banco_cnes","nomeFantasia")).filter(lambda x: x is not None).filter(lambda x: x != "").filter(lambda x: x).filter(bool)
if lst_novo_cnes.isEmpty() == False:
   df_novo_cnes = spark.createDataFrame(lst_novo_cnes)
   df_novo_cnes.write.format("csv").option("sep","|").mode("overwrite").save("/data/raw/FICO/AllCheckAnaliseFullOutput/diario/"+ano+mes+dia+"/novo_cnes")

   novo_cnes_cabecalho = rdd.map(lambda (string_file): parser_xml_cabecalho(string_file[1],"novo_cnes","banco_cnes","nomeFantasia")).filter(lambda x: x is not None).filter(lambda x: x != "").filter(lambda x: x).filter(bool)
   df_novo_cnes_cabecalho = spark.createDataFrame(novo_cnes_cabecalho)
   df_novo_cnes_cabecalho.write.format("csv").option("sep","|").mode("overwrite").save("/data/raw/FICO/AllCheckAnaliseFullOutput/diario/"+ano+mes+dia+"/novo_cnes/cabecalho")

   gera_arquivo("novo_cnes","arquivo_novo_cnes","arquivo_novo_cnes_final")

#novo_confea
lst_novo_confea = rdd.map(lambda (string_file): parser_xml(string_file[1],"novo_confea","banco_confea","posGraduacao")).filter(lambda x: x is not None).filter(lambda x: x != "").filter(lambda x: x).filter(bool)
if lst_novo_confea.isEmpty() == False:
   df_novo_confea = spark.createDataFrame(lst_novo_confea)
   df_novo_confea.write.format("csv").option("sep","|").mode("overwrite").save("/data/raw/FICO/AllCheckAnaliseFullOutput/diario/"+ano+mes+dia+"/novo_confea")

   novo_confea_cabecalho = rdd.map(lambda (string_file): parser_xml_cabecalho(string_file[1],"novo_confea","banco_confea","posGraduacao")).filter(lambda x: x is not None).filter(lambda x: x != "").filter(lambda x: x).filter(bool)
   df_novo_confea_cabecalho = spark.createDataFrame(novo_confea_cabecalho)
   df_novo_confea_cabecalho.write.format("csv").option("sep","|").mode("overwrite").save("/data/raw/FICO/AllCheckAnaliseFullOutput/diario/"+ano+mes+dia+"/novo_confea/cabecalho")

   gera_arquivo("novo_confea","arquivo_novo_confea","arquivo_novo_confea_final")

#novo_coren
lst_novo_coren = rdd.map(lambda (string_file): parser_xml(string_file[1],"novo_coren","banco_coren","observacao")).filter(lambda x: x is not None).filter(lambda x: x != "").filter(lambda x: x).filter(bool)
if lst_novo_coren.isEmpty() == False:
   df_novo_coren = spark.createDataFrame(lst_novo_coren)
   df_novo_coren.write.format("csv").option("sep","|").mode("overwrite").save("/data/raw/FICO/AllCheckAnaliseFullOutput/diario/"+ano+mes+dia+"/novo_coren")

   novo_coren_cabecalho = rdd.map(lambda (string_file): parser_xml_cabecalho(string_file[1],"novo_coren","banco_coren","observacao")).filter(lambda x: x is not None).filter(lambda x: x != "").filter(lambda x: x).filter(bool)
   df_novo_coren_cabecalho = spark.createDataFrame(novo_coren_cabecalho)
   df_novo_coren_cabecalho.write.format("csv").option("sep","|").mode("overwrite").save("/data/raw/FICO/AllCheckAnaliseFullOutput/diario/"+ano+mes+dia+"/novo_coren/cabecalho")

   gera_arquivo("novo_coren","arquivo_novo_coren","arquivo_novo_coren_final")

#novo_siccau
lst_novo_siccau = rdd.map(lambda (string_file): parser_xml(string_file[1],"novo_siccau","banco_siccau","status")).filter(lambda x: x is not None).filter(lambda x: x != "").filter(lambda x: x).filter(bool)
if lst_novo_siccau.isEmpty() == False:
   df_novo_siccau = spark.createDataFrame(lst_novo_siccau)
   df_novo_siccau.write.format("csv").option("sep","|").mode("overwrite").save("/data/raw/FICO/AllCheckAnaliseFullOutput/diario/"+ano+mes+dia+"/novo_siccau")

   novo_siccau_cabecalho = rdd.map(lambda (string_file): parser_xml_cabecalho(string_file[1],"novo_siccau","banco_siccau","status")).filter(lambda x: x is not None).filter(lambda x: x != "").filter(lambda x: x).filter(bool)
   df_novo_siccau_cabecalho = spark.createDataFrame(novo_siccau_cabecalho)
   df_novo_siccau_cabecalho.write.format("csv").option("sep","|").mode("overwrite").save("/data/raw/FICO/AllCheckAnaliseFullOutput/diario/"+ano+mes+dia+"/novo_siccau/cabecalho")

   gera_arquivo("novo_siccau","arquivo_novo_siccau","arquivo_novo_siccau_final")

#novo_crosp
lst_novo_crosp = rdd.map(lambda (string_file): parser_xml(string_file[1],"novo_crosp","banco_crosp","habilitacao")).filter(lambda x: x is not None).filter(lambda x: x != "").filter(lambda x: x).filter(bool)
if lst_novo_crosp.isEmpty() == False:
   df_novo_crosp = spark.createDataFrame(lst_novo_crosp)
   df_novo_crosp.write.format("csv").option("sep","|").mode("overwrite").save("/data/raw/FICO/AllCheckAnaliseFullOutput/diario/"+ano+mes+dia+"/novo_crosp")

   novo_crosp_cabecalho = rdd.map(lambda (string_file): parser_xml_cabecalho(string_file[1],"novo_crosp","banco_crosp","habilitacao")).filter(lambda x: x is not None).filter(lambda x: x != "").filter(lambda x: x).filter(bool)
   df_novo_crosp_cabecalho = spark.createDataFrame(novo_crosp_cabecalho)
   df_novo_crosp_cabecalho.write.format("csv").option("sep","|").mode("overwrite").save("/data/raw/FICO/AllCheckAnaliseFullOutput/diario/"+ano+mes+dia+"/novo_crosp/cabecalho")

   gera_arquivo("novo_crosp","arquivo_novo_crosp","arquivo_novo_crosp_final") 

#novo_cfmv
lst_novo_cfmv = rdd.map(lambda (string_file): parser_xml(string_file[1],"novo_cfmv","banco_cfmv","uf")).filter(lambda x: x is not None).filter(lambda x: x != "").filter(lambda x: x).filter(bool)
if lst_novo_cfmv.isEmpty() == False:
   df_novo_cfmv = spark.createDataFrame(lst_novo_cfmv)
   df_novo_cfmv.write.format("csv").option("sep","|").mode("overwrite").save("/data/raw/FICO/AllCheckAnaliseFullOutput/diario/"+ano+mes+dia+"/novo_cfmv")

   novo_cfmv_cabecalho = rdd.map(lambda (string_file): parser_xml_cabecalho(string_file[1],"novo_cfmv","banco_cfmv","uf")).filter(lambda x: x is not None).filter(lambda x: x != "").filter(lambda x: x).filter(bool)
   df_novo_cfmv_cabecalho = spark.createDataFrame(novo_cfmv_cabecalho)
   df_novo_cfmv_cabecalho.write.format("csv").option("sep","|").mode("overwrite").save("/data/raw/FICO/AllCheckAnaliseFullOutput/diario/"+ano+mes+dia+"/novo_cfmv/cabecalho")

   gera_arquivo("novo_cfmv","arquivo_novo_cfmv","arquivo_novo_cfmv_final")

#novo_cfc
lst_novo_cfc = rdd.map(lambda (string_file): parser_xml(string_file[1],"novo_cfc","banco_cfc","situacao")).filter(lambda x: x is not None).filter(lambda x: x != "").filter(lambda x: x).filter(bool)
if lst_novo_cfc.isEmpty() == False:
   df_novo_cfc = spark.createDataFrame(lst_novo_cfc)
   df_novo_cfc.write.format("csv").option("sep","|").mode("overwrite").save("/data/raw/FICO/AllCheckAnaliseFullOutput/diario/"+ano+mes+dia+"/novo_cfc")

   novo_cfc_cabecalho = rdd.map(lambda (string_file): parser_xml_cabecalho(string_file[1],"novo_cfc","banco_cfc","situacao")).filter(lambda x: x is not None).filter(lambda x: x != "").filter(lambda x: x).filter(bool)
   df_novo_cfc_cabecalho = spark.createDataFrame(novo_cfc_cabecalho)
   df_novo_cfc_cabecalho.write.format("csv").option("sep","|").mode("overwrite").save("/data/raw/FICO/AllCheckAnaliseFullOutput/diario/"+ano+mes+dia+"/novo_cfc/cabecalho")

   gera_arquivo("novo_cfc","arquivo_novo_cfc","arquivo_novo_cfc_final")

#novo_telefone_fraude
lst_novo_telefone_fraude = rdd.map(lambda (string_file): parser_xml(string_file[1],"novo_telefone","banco_telefoneFraude","estado")).filter(lambda x: x is not None).filter(lambda x: x != "").filter(lambda x: x).filter(bool)
if lst_novo_telefone_fraude.isEmpty() == False:
   df_novo_telefone_fraude = spark.createDataFrame(lst_novo_telefone_fraude)
   df_novo_telefone_fraude.write.format("csv").option("sep","|").mode("overwrite").save("/data/raw/FICO/AllCheckAnaliseFullOutput/diario/"+ano+mes+dia+"/novo_telefone_fraude")

   novo_telefone_fraude_cabecalho = rdd.map(lambda (string_file): parser_xml_cabecalho(string_file[1],"novo_telefone","banco_telefoneFraude","estado")).filter(lambda x: x is not None).filter(lambda x: x != "").filter(lambda x: x).filter(bool)
   df_novo_telefone_fraude_cabecalho = spark.createDataFrame(novo_telefone_fraude_cabecalho)
   df_novo_telefone_fraude_cabecalho.write.format("csv").option("sep","|").mode("overwrite").save("/data/raw/FICO/AllCheckAnaliseFullOutput/diario/"+ano+mes+dia+"/novo_telefone_fraude/cabecalho")

   gera_arquivo("novo_telefone_fraude","arquivo_novo_telefone_fraude","arquivo_novo_telefone_fraude_final")

#novo_anp
lst_novo_anp = rdd.map(lambda (string_file): parser_xml(string_file[1],"novo_anp","banco_anp","tipo")).filter(lambda x: x is not None).filter(lambda x: x != "").filter(lambda x: x).filter(bool)
if lst_novo_anp.isEmpty() == False:
   df_novo_anp = spark.createDataFrame(lst_novo_anp)
   df_novo_anp.write.format("csv").option("sep","|").mode("overwrite").save("/data/raw/FICO/AllCheckAnaliseFullOutput/diario/"+ano+mes+dia+"/novo_anp")

   novo_anp_cabecalho = rdd.map(lambda (string_file): parser_xml_cabecalho(string_file[1],"novo_anp","banco_anp","tipo")).filter(lambda x: x is not None).filter(lambda x: x != "").filter(lambda x: x).filter(bool)
   df_novo_anp_cabecalho = spark.createDataFrame(novo_anp_cabecalho)
   df_novo_anp_cabecalho.write.format("csv").option("sep","|").mode("overwrite").save("/data/raw/FICO/AllCheckAnaliseFullOutput/diario/"+ano+mes+dia+"/novo_anp/cabecalho")

   gera_arquivo("novo_anp","arquivo_novo_anp","arquivo_novo_anp_final")

#consta_ppePrestacaoContasDoador
lst_consta_ppePrestacaoContasDoador = rdd.map(lambda (string_file): parser_xml(string_file[1],"consta_ppePrestacaoContasDoador","banco_ppePrestacaoContasDoador","valor_doado")).filter(lambda x: x is not None).filter(lambda x: x != "").filter(lambda x: x).filter(bool)
if lst_consta_ppePrestacaoContasDoador.isEmpty() == False:
   df_consta_ppePrestacaoContasDoador = spark.createDataFrame(lst_consta_ppePrestacaoContasDoador)
   df_consta_ppePrestacaoContasDoador.write.format("csv").option("sep","|").mode("overwrite").save("/data/raw/FICO/AllCheckAnaliseFullOutput/diario/"+ano+mes+dia+"/consta_ppePrestacaoContasDoador")

   consta_ppePrestacaoContasDoador_cabecalho = rdd.map(lambda (string_file): parser_xml_cabecalho(string_file[1],"consta_ppePrestacaoContasDoador","banco_ppePrestacaoContasDoador","valor_doado")).filter(lambda x: x is not None).filter(lambda x: x != "").filter(lambda x: x).filter(bool)
   df_consta_ppePrestacaoContasDoador_cabecalho = spark.createDataFrame(consta_ppePrestacaoContasDoador_cabecalho)
   df_consta_ppePrestacaoContasDoador_cabecalho.write.format("csv").option("sep","|").mode("overwrite").save("/data/raw/FICO/AllCheckAnaliseFullOutput/diario/"+ano+mes+dia+"/consta_ppePrestacaoContasDoador/cabecalho")

   gera_arquivo("consta_ppePrestacaoContasDoador","arquivo_consta_ppePrestacaoContasDoador","arquivo_consta_ppePrestacaoContasDoador_final")

#consta_endereco
lst_consta_endereco = rdd.map(lambda (string_file): parser_xml(string_file[1],"consta_endereco","banco_endereco","estado")).filter(lambda x: x is not None).filter(lambda x: x != "").filter(lambda x: x).filter(bool)
if lst_consta_endereco.isEmpty() == False:
   df_consta_endereco = spark.createDataFrame(lst_consta_endereco)
   df_consta_endereco.write.format("csv").option("sep","|").mode("overwrite").save("/data/raw/FICO/AllCheckAnaliseFullOutput/diario/"+ano+mes+dia+"/consta_endereco")

   consta_endereco_cabecalho = rdd.map(lambda (string_file): parser_xml_cabecalho(string_file[1],"consta_endereco","banco_endereco","estado")).filter(lambda x: x is not None).filter(lambda x: x != "").filter(lambda x: x).filter(bool)
   df_consta_endereco_cabecalho = spark.createDataFrame(consta_endereco_cabecalho)
   df_consta_endereco_cabecalho.write.format("csv").option("sep","|").mode("overwrite").save("/data/raw/FICO/AllCheckAnaliseFullOutput/diario/"+ano+mes+dia+"/consta_endereco/cabecalho")

   gera_arquivo("consta_endereco","arquivo_consta_endereco","arquivo_consta_endereco_final")

spark.stop()
