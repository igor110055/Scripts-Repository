#####################################
######## -*- coding: utf-8 -*-########
#########EXPORTACAO SALESFORCE #######
########### post CONTRATOS ###########
############# Main ###################
#############Versão - 1.0#############
######################################

# -*- coding: utf-8 -*-
import sys
import json, requests
import shlex
import subprocess
import logging
import pandas as pd
import datetime

from imp import reload
from pyspark import SparkContext, SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
reload(sys)
sys.setdefaultencoding('utf8')

print("iniciou processo")

chave_blob = "uxEa+N5G+94z+0/UNdOfdJbBkn7iQ4pPOZ0FNgU14WNKsVIy7sHA/T1arnyxGMFB46KBFTsHtNr2jQr2pl64Hg=="
blob = "fs.azure.account.key.azreddevstgacc.blob.core.windows.net"
sales_force_client_id="3MVG9er.T8KbeePSZKM9mVmglS2jh.dDzs6r7ON4qYiil2KICQvmKBaBY_19qB.cama2b11g8woFnB31G1Pf6"
sales_force_client_secret ="65C406A6D1FE79C9E4E7C1619CF5B2A281F6B59EDC9E55FE9E14ACEB545F1521"
sales_force_user_name="gilberto.oliveira@emprestimosim.prd.com.devred"
sales_force_password="Tin@2911"

cols = ['Mensagem']
lst = []

ano = str(datetime.datetime.now().year)
mes = str(datetime.datetime.now().month).zfill(2)
dia = str(datetime.datetime.now().day).zfill(2)

#Configura a sessao com as credenciais para acessar o blob storage
spark = SparkSession.builder.config(blob,chave_blob).getOrCreate()

#Script para recuperar o Token dinamico do sales force
print("**********************************************************")
print("***********RECUPERA O TOKEN DO SALES FORCE****************")
print("**********************************************************")
cmd = 'curl -v https://red--devred.my.salesforce.com/services/oauth2/token -d "grant_type=password" -d "client_id='+sales_force_client_id+'" -d "client_secret='+sales_force_client_secret+'"  -d "username='+sales_force_user_name+'" -d "password='+sales_force_password+'"'
args = shlex.split(cmd)
process = subprocess.Popen(args, shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
stdout, stderr = process.communicate()
body = json.loads(stdout)
token = body["access_token"]
# print(token)

cmd_post='curl https://red--devred.my.salesforce.com/services/data/v42.0/sobjects/contract/ -H "Authorization: Bearer '+token+'" -H "Content-Type: application/json" -d '


print("**********************************************************")
print("******RECUPERA O ARQUIVO DO BLOB E INICIA O LOOP**********")
print("**********************************************************")
# Le arquivo a ser enviado para o sales force do blob storage
lines = spark.read.text(
   # "/data/harmonized/Exportacao/SalesForce/Contratos/")
'abfs://sparkazredhdi-dev@azredlake.dfs.core.windows.net/data/harmonized/Exportacao/SalesForce/Contratos/')
#lines = spark.read.text("/data/harmonized/Exportacao/SalesForce/Contratos/newrecords2.json")

# coleta o RDD para uma lista
llist = lines.collect()

for line in llist:
   string_json = ''.join(line)
   string_json_final  = "'" + string_json + "'"
   cmd_post_final = cmd_post + string_json_final
   args_post = shlex.split(cmd_post_final)
   process_post = subprocess.Popen(args_post, shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
   stdout_post, stderr_post = process_post.communicate()
   stdout_post_grava = '{"retorno":'+stdout_post +', "mensagem":' + string_json + '}'
   lst.append([stdout_post_grava])
   #subprocess.call("echo '"+stdout_post_grava+"' | hadoop fs -appendToFile - /data/harmonized/Exportacao/SalesForce/Contratos/log/controle_carga.log",shell=True)
   # print(stdout_post_grava)


print("**********************************************************")
print("******************GERA O ARQUIVO DE LOG*******************")
print("**********************************************************")
df = pd.DataFrame(lst, columns=cols)
df.to_json("controle_carga.json")
subprocess.call("hadoop fs -copyFromLocal -f controle_carga.json /data/harmonized/Exportacao/SalesForce/Contratos/log/controle_carga_"+ano+mes+dia+".log",shell=True)
subprocess.call("rm controle_carga.json",shell=True)
print("Finalizou o processo de post Sales Force")
spark.stop()
