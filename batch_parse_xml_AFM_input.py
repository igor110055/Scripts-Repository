import subprocess
from pyspark.sql import Row
import xml.etree.ElementTree as ET
import datetime
from lxml import etree
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

if __name__ == "__main__":
   sc = SparkContext('local')
   spark = SparkSession(sc)
   session = SparkSession.builder.appName("allcheck").getOrCreate()

# Configura as variaveis que serao utilizadas para montar o nome dos diretorios.
ano = str(datetime.datetime.now().year)
mes = str(datetime.datetime.now().month).zfill(2)
dia = str(datetime.datetime.now().day).zfill(2)
row_data = Row("col1")
row_cabecalho = Row("col1")

def parser_xml_itens(string_xml):
   valor = ""
   root = ET.fromstring(string_xml.encode("utf-8"))
   # recupera as informacnoes dos itens do xml, exceto sod o user data
   iter = root.getiterator()
   for element in iter:
      verifica_comentario = element.tag.find("<cyfunction")
      if element.getchildren() and element.tag != "{http://www.fico.com/fraud/schemas/types}userData" and verifica_comentario <> 0:
         for child in element:
            child_text = ""
            verifica_comentario_child = str(child.tag).find("<cyfunction")
            if child.tag != "{http://www.fico.com/fraud/schemas/types}userData" and verifica_comentario_child <> 0 and len(child) == 0:
               if child.text is not None:
                  child_text = child.text.encode("utf-8")
                  child_text = str(child_text)
               child_text = child_text.strip()
               valor = valor + child_text + ";"
   # recupera as informacnoes do user data
   iter = root.getiterator()
   for element in iter:
      verifica_comentario = element.tag.find("<cyfunction")
      if element.getchildren() and element.tag == "{http://www.fico.com/fraud/schemas/types}userData" and verifica_comentario <> 0:
         for child in element:
            child_text = ""
            if child.tag == "{http://www.fico.com/fraud/schemas/types}value":
               if child.text is not None:
                  child_text = child.text.encode("utf-8")
                  child_text = str(child_text)
                  child_text = child_text.strip()
                  valor = valor + child_text + ";"
   valor = valor[:-1]
   return row_data(valor)

def parser_xml_itens_cabecalho(string_xml):
   cabecalho = ""
   root = ET.fromstring(string_xml.encode("utf-8"))
   # recupera as informacnoes dos itens do xml, exceto sod o user data
   iter = root.getiterator()
   for element in iter:
      verifica_comentario = element.tag.find("<cyfunction")
      if element.getchildren() and element.tag != "{http://www.fico.com/fraud/schemas/types}userData" and verifica_comentario <> 0:
         for child in element:
            child_tag = ""
            verifica_comentario_child = str(child.tag).find("<cyfunction")
            if child.tag != "{http://www.fico.com/fraud/schemas/types}userData" and verifica_comentario_child <> 0 and len(child) == 0:
               if child_tag is not None:
                  child_tag = child.tag.encode("utf-8")
                  child_tag = str(child_tag)
               child_tag = child_tag.strip()
               cabecalho = str(cabecalho) + child_tag.replace("{http://www.fico.com/fraud/schemas}","").replace("{http://www.fico.com/fraud/schemas/types}","") + ";"
   # recupera as informacnoes do user data
   iter = root.getiterator()
   for element in iter:
      verifica_comentario = element.tag.find("<cyfunction")
      if element.getchildren() and element.tag == "{http://www.fico.com/fraud/schemas/types}userData" and verifica_comentario <> 0:
         for child in element:
            child_tag = ""
            if child.tag == "{http://www.fico.com/fraud/schemas/types}name":
               if child.text is not None:
                  child_tag = child.tag.encode("utf-8")
                  child_tag = str(child_tag)
                  cabecalho = cabecalho + child.text.encode("utf-8") + ";"
   cabecalho = cabecalho[:-1]
   return row_cabecalho(cabecalho)

def gera_arquivo(nome_diretorio,nome_arquivo,nome_arquivo_final):
   #Lista os arquivos existente no diretorio no qual o streaming esta sendo gerado.
   args = "hdfs dfs -ls abfs://sparkazredhdi-dev@azredlake.dfs.core.windows.net/data/raw/FICO/PayloadAFMInput/consolidado/"+ano+mes+dia+"/*.csv | awk '{print $9}'"
   proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
   s_output, s_err = proc.communicate()
   all_dart_dirs = s_output.split()
   sed = "sed "
   sed2 = "s/\"//g"
   for line in all_dart_dirs:
      args_trata = "hadoop fs -cat "+line+" |  " + repr(sed) + "'" + repr(sed2) +" | hadoop fs -put -f - /data/raw/FICO/PayloadAFMInput/consolidado/"+ano+mes+dia+"/"+nome_arquivo+".csv"
      args_trata = args_trata.replace("'sed","sed")
      args_trata = args_trata.replace("'''","'")
   print(args_trata)
   proc_trata = subprocess.Popen(args_trata, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
   s_output_trata, s_err_trata = proc_trata.communicate()

   #Lista os arquivos existente no diretorio no qual o cabecalho esta sendo gerado para recuperar o cabecalho
   args_cabecalho = "hdfs dfs -ls abfs://sparkazredhdi-dev@azredlake.dfs.core.windows.net/data/raw/FICO/PayloadAFMInput/consolidado/"+ano+mes+dia+"/cabecalho/*.csv | awk '{print $9}'"
   proc_cabecalho = subprocess.Popen(args_cabecalho, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
   s_output_cabecalho, s_err_cabecalho = proc_cabecalho.communicate()
   all_dart_dirs_cabecalho = s_output_cabecalho.split()
   for line_cabecalho in all_dart_dirs_cabecalho:
      args_trata_cabecalho = 'hadoop fs -cat '+line_cabecalho+' |  sed -n "1p" |  ' + repr(sed) + "'" + repr(sed2) + ' | hadoop fs -put -f - /data/raw/FICO/PayloadAFMInput/consolidado/'+ano+mes+dia+'/cabecalho/tratado.csv'
      args_trata_cabecalho = args_trata_cabecalho.replace("'sed","sed")
      args_trata_cabecalho = args_trata_cabecalho.replace("'''","'")
   print(args_trata_cabecalho)
   proc_trata_cabecalho = subprocess.Popen(args_trata_cabecalho, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
   s_output_trata_cabecalho, s_err_trata_cabecalho = proc_trata_cabecalho.communicate()

   #Gera o arquivo final
   args_final = "hadoop fs -cat abfs://sparkazredhdi-dev@azredlake.dfs.core.windows.net/data/raw/FICO/PayloadAFMInput/consolidado/"+ano+mes+dia+"/cabecalho/tratado.csv abfs://sparkazredhdi-dev@azredlake.dfs.core.windows.net/data/raw/FICO/PayloadAFMInput/consolidado/"+ano+mes+dia+"/"+nome_arquivo+".csv | hadoop fs -put -f - abfs://sparkazredhdi-dev@azredlake.dfs.core.windows.net/data/raw/FICO/PayloadAFMInput/consolidado/"+ano+mes+dia+"/"+nome_arquivo_final+".csv"
   print(args_final)
   proc_final = subprocess.Popen(args_final, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
   s_output_final, s_err_final = proc_final.communicate()

rdd = sc.wholeTextFiles("/data/raw/FICO/PayloadAFMInput/detalhado/"+ano+mes+dia+"/*.txt")
lst = rdd.map(lambda (string_file): parser_xml_itens(string_file[1]))
df = spark.createDataFrame(lst)
df .write.format("csv").option("sep",";").mode("overwrite").save("/data/raw/FICO/PayloadAFMInput/consolidado/"+ano+mes+dia)

lst_cabecalho = rdd.map(lambda (string_file): parser_xml_itens_cabecalho(string_file[1]))
df_cabecalho = spark.createDataFrame(lst_cabecalho)
df_cabecalho.write.format("csv").option("sep",";").mode("overwrite").save("/data/raw/FICO/PayloadAFMInput/consolidado/"+ano+mes+dia+"/cabecalho")

gera_arquivo("PayloadAFMInput","arquivo_PayloadAFMInput","arquivo_PayloadAFMInput_final")

spark.stop()

