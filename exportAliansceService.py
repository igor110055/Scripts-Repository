import json
from logging import error, log
import os
import base64 as b64
import hashlib
from Cryptodome.Cipher import AES
from Crypto import Random
import unicodedata
import sys
from pyspark.sql import group
from pyspark.sql.functions import translate, regexp_replace, trim, udf, lpad, col, get_json_object, to_timestamp, \
    to_date, coalesce, upper, lit, concat, when, struct, row_number, length, regexp_extract
from pyspark.sql.window import Window
from pyspark.sql.types import *
import json
from datetime import date, datetime, timedelta
from google.oauth2.service_account import Credentials
from google.cloud.secretmanager import SecretManagerServiceClient
from emr_ingestion.utils.config import Config
import boto3
from zooxdata.s3 import S3

class exporterAliansceService:

    def __init__(self, spark, lastFile=None):
        self.spark = spark
        self.credentials = Credentials.from_service_account_info(Config.ALIANSCE_CONFIG)
        self.client = SecretManagerServiceClient(credentials=self.credentials)
        self.resource_name = "projects/40873255192/secrets/zoox-cripto-password/versions/1"
        self.response = self.client.access_secret_version({'name': self.resource_name})
        self.pwd = self.response.payload.data.decode("UTF-8")
        self.lastFile = lastFile
        self.list_id_persons = []
        self.nameFile = self.lastFile.split("/")[-1]
        self.parte1_path = "s3://zd-results/project/aliansce/zdt-771/PARTE1_PERSON_JOIN_ALIANSCE/{}/".format(self.nameFile.split(".")[0])
        self.parte2_path = "s3://zd-results/project/aliansce/zdt-771/PARTE2_PERSON_DATA_ALIANSCE/{}/".format(self.nameFile.split(".")[0])
        self.parte3_path = "s3://zd-results/project/aliansce/zdt-771/PARTE3_LOGINDATA_REDUCED/{}/".format(self.nameFile.split(".")[0])
        self.parte4_path = "s3://zd-results/project/aliansce/zdt-771/PARTE4_FINAL_ENRICH/{}/".format(self.nameFile.split(".")[0])
        #self.parte5_path = "s3://zd-results/project/aliansce/zdt-771/PARTE5_FINAL_ENRICH_CPTYPE/"
        self.log = "project/aliansce/zdt-771/LOGS/{}.txt".format(self.nameFile.split(".")[0])
        self.temp_bucket = "zd-results"
        self.bucket = "external-aliansce"

    @classmethod
    def encrypt(cls, message, password):
        message = str(message)
        private_key = hashlib.sha256(password.encode("utf-8")).digest()
        iv = Random.new().read(AES.block_size)
        cipher = AES.new(private_key, AES.MODE_CFB, iv)
        encripted = b64.b64encode(iv + cipher.encrypt(message.encode("utf-8")))
        return encripted.decode('utf-8')

    @classmethod
    def decrypt(cls, message, password):
        private_key = hashlib.sha256(password.encode("utf-8")).digest()
        message = b64.b64decode(message.encode("utf-8"))
        iv = message[:16]
        cipher = AES.new(private_key, AES.MODE_CFB, iv)
        decripted = cipher.decrypt(message[16:])
        return decripted.decode('utf-8')

    @classmethod
    def wrangle_email(cls, address_data):
        try:
            address_data = json.loads(address_data)
            for address in address_data:
                if address.get("Priority") == 1:
                    return json.dumps(address)
        except Exception as e:
            print(e)
            pass

    @classmethod
    def wrangle_phone_lemit(cls, address_data):
        try:
            address_data = json.loads(address_data)
            for address in address_data:
                if address.get("ranking") in [1, 2, 3]:
                    return json.dumps(address)
        except Exception as e:
            pass

    @classmethod
    def wrangle_phone_bigdata(cls, address_data, phone_type):
        try:
            address_data = json.loads(address_data)
            for address in address_data:
                if address.get("Priority") in [1, 2, 3] and address.get("Type") == phone_type:
                    return json.dumps(address)
        except Exception as e:
            pass

    @classmethod
    def get_type(cls, phone_number):
        try:
            if len(phone_number) == 8 or len(phone_number) == 10:
                return "0"
            else:
                return "1"
        except:
            pass

    @classmethod
    def check_phone_trusted(cls, phone):
        try:
            if len(phone) > 13:
                phone = phone.lstrip("0")

            if phone[:2] == "55" and (len(phone) == 12 or len(phone) == 13):
                return "+" + phone
            elif len(phone) == 10 or len(phone) == 11:
                return "+55" + phone
            else:
                return phone
        except:
            pass

    @classmethod
    def get_priority_address(cls, address_data, type_address):
        try:
            address_data = json.loads(address_data)
            for address in address_data:
                if (address.get("Type") == type_address) and (address.get("Priority") in (1, 2, 3)):
                    return json.dumps(address)
        except Exception as e:
            pass

    @classmethod
    def type_return(cls, address):
        try:
            return address[-1]

        except Exception as e:
            pass

    type_udf = udf(lambda x: exporterAliansceService.type_return(x))

    @classmethod
    def remove_type(cls, address):
        try:
            return address[:-2]

        except Exception as e:
            pass

    @classmethod
    def make_trans(cls):
        matching_string = ""
        replace_string = ""
        for i in range(ord(" "), sys.maxunicode):
            name = unicodedata.name(chr(i), "")
            if "WITH" in name:
                try:
                    base = unicodedata.lookup(name.split(" WITH")[0])
                    matching_string += chr(i)
                    replace_string += base
                except KeyError:
                    pass
        return matching_string, replace_string

    @classmethod
    def clean_text(cls, c):
        matching_string, replace_string = cls.make_trans()
        return translate(
            regexp_replace(c, "\p{M}", ""),
            matching_string, replace_string
        ).alias(c)

    UFS_DICT = {'AC': 'ACRE',
                'AL': 'ALAGOAS',
                'AP': 'AMAPA',
                'AM': 'AMAZONAS',
                'BA': 'BAHIA',
                'CE': 'CEARA',
                'DF': 'DISTRITO FEDERAL',
                'ES': 'ESPIRITO SANTO',
                'GO': 'GOAIS',
                'MA': 'MARANHAO',
                'MT': 'MATO GROSSO',
                'MS': 'MATO GROSSO DO SUL',
                'MG': 'MINAS GERAIS',
                'PA': 'PARA',
                'PB': 'PARAIBA',
                'PR': 'PARANA',
                'PE': 'PERNAMBUCO',
                'PI': 'PIAUI',
                'RN': 'RIO GRANDO DO NORTE',
                'RS': 'RIO GRANDE DO SUL',
                'RJ': 'RIO DE JANEIRO',
                'RO': 'RONDONIA',
                'RR': 'RORAIMA',
                'SC': 'SANTA CATARINA',
                'SP': 'SAO PAULO',
                'SE': 'SERGIPE',
                'TO': 'TOCANTINS'}

    UFS_DICT_INV = {'ACRE': 'AC',
                    'ALAGOAS': 'AL',
                    'AMAPA': 'AP',
                    'AMAZONAS': 'AM',
                    'BAHIA': 'BA',
                    'CEARA': 'CE',
                    'DISTRITOFEDERAL': 'DF',
                    'ESPIRITOSANTO': 'ES',
                    'GOIAS': 'GO',
                    'MARANHAO': 'MA',
                    'MATOGROSSO': 'MT',
                    'MATOGROSSODOSUL': 'MS',
                    'MINASGERAIS': 'MG',
                    'PARA': 'PA',
                    'PARAIBA': 'PB',
                    'PARANA': 'PR',
                    'PERNAMBUCO': 'PE',
                    'PIAUI': 'PI',
                    'RIOGRANDEDONORTE': 'RN',
                    'RIOGRANDEDOSUL': 'RS',
                    'RIODEJANEIRO': 'RJ',
                    'RONDONIA': 'RO',
                    'RORAIMA': 'RR',
                    'SANTACATARINA': 'SC',
                    'SAOPAULO': 'SP',
                    'SERGIPE': 'SE',
                    'TOCANTINS': 'TO'}

    @classmethod
    def uf_return(cls, state):
        try:
            state = state.replace(" ", "")
            state = state.upper()
            return cls.clean_text(state)

        except:
            return state

    @classmethod
    def uf_zoox(cls, data_z):
        try:
            data_d = data_z.replace(" ", "")
            state = data_d.split(",")[-1]
            state = state.upper()

            state = cls.clean_text(state)

            UF = cls.UFS_DICT_INV[state]

            a = data_z.split(",")[0] + ", " + UF + ', Brasil'

            return a
        except:
            pass

    @classmethod
    def obit(cls, x):
        if x == "false":
            y = 0
        elif x == "true":
            y = 1
        else:
            y = None

        return y

    @classmethod
    def buildName(cls, aliansce):
        #aliansce = spark.read.load(path)

        aliansce = aliansce \
            .select("id", "email", "updated", "providers", "cpf_cry", "cpf",
                                   get_json_object(aliansce.providers, '$.zooxwifi.name').alias("name_zooxwifi"),
                                   get_json_object(aliansce.providers, '$.bigdata_corp.name').alias(
                                       "name_bigdata_corp"),
                                   get_json_object(aliansce.providers, '$.lemit.nome').alias("name_lemit"),
                                   get_json_object(aliansce.providers, '$.facebook.name').alias("name_facebook"),
                                   get_json_object(aliansce.providers, '$.twitter.name').alias("name_twitter"),
                                   get_json_object(aliansce.providers, '$.fnrh.name').alias("name_fnrh"),
                                   get_json_object(aliansce.providers, '$.instagram.name').alias("name_instagram"))
        aliansce = aliansce \
            .withColumn("name", upper(coalesce(aliansce["name_fnrh"],
                                          aliansce["name_bigdata_corp"],
                                          aliansce["name_lemit"],
                                          aliansce["name_zooxwifi"],
                                          aliansce["name_facebook"],
                                          aliansce["name_twitter"],
                                          aliansce["name_instagram"])))
        #        aliansce = aliansce.drop(
        #            *["name_fnrh", "name_bigdata_corp", "name_lemit", "name_zooxwifi", "name_facebook", "name_twitter",
        #              "name_instagram", "name"])
        #aliansce = aliansce.withColumn("name", upper(col("name")))
        # aliansce = aliansce.withColumnRenamed("name_trusted", "name")

        return aliansce

    @classmethod
    def buildBirth(cls, aliansce):
        aliansce = aliansce.select("id", "email", "updated", "providers", "cpf_cry", "cpf", "name", \
                                   get_json_object(aliansce.providers, '$.zooxwifi.birth_date').alias(
                                       "birth_date_zooxwifi"), \
                                   get_json_object(aliansce.providers, '$.bigdata_corp.birth_date').alias(
                                       "birth_date_bigdata_corp"), \
                                   get_json_object(aliansce.providers, '$.lemit.data_nascimento').alias(
                                       "birth_date_lemit"), \
                                   get_json_object(aliansce.providers, '$.fnrh.birth_date').alias("birth_date_fnrh"))
        aliansce = aliansce \
            .withColumn("birth_date_trusted", (coalesce(aliansce["birth_date_fnrh"],
                                                        aliansce["birth_date_bigdata_corp"],
                                                        aliansce["birth_date_lemit"],
                                                        aliansce["birth_date_zooxwifi"]))) \
            .withColumnRenamed("birth_date_trusted", "birth_date")
        #        aliansce = aliansce.drop(
        #            *["birth_date_fnrh", "birth_date_bigdata_corp", "birth_date_lemit", "birth_date_zooxwifi"])

        return aliansce

    @classmethod
    def buildGender(cls, aliansce):
        aliansce = aliansce.select("id", "email", "updated", "providers", "cpf_cry", "cpf", "name",
                                   "birth_date", \
                                   get_json_object(aliansce.providers, '$.zooxwifi.gender').alias("gender_zooxwifi"), \
                                   get_json_object(aliansce.providers, '$.lemit.sexo').alias("gender_lemit"), \
                                   get_json_object(aliansce.providers, '$.fnrh.gender').alias("gender_fnrh"), \
                                   get_json_object(aliansce.providers, '$.unisuam.gender').alias("gender_unisuam"), \
                                   get_json_object(aliansce.providers,
                                                   '$.bigdata_corp.extra_provider.BasicData.Gender').alias(
                                       "gender_bigdata_corp"))

        aliansce = aliansce \
            .withColumn("gender_trusted", (coalesce(aliansce["gender_fnrh"],
                                                    aliansce["gender_bigdata_corp"],
                                                    aliansce["gender_lemit"],
                                                    aliansce["gender_unisuam"],
                                                    aliansce["gender_zooxwifi"])))
        aliansce = aliansce.withColumn("gender_trusted", regexp_replace(regexp_replace(regexp_replace(col("gender_trusted"), "M", "2"), "F", "1"), "U", "0"))                                                                
        aliansce = aliansce.withColumnRenamed("gender_trusted", "gender")
        #        aliansce = aliansce.drop(
        #            *["gender_fnrh", "gender_bigdata_corp", "gender_lemit", "gender_unisuam", "gender_zooxwifi"])

        return aliansce

    @classmethod
    def buildParents(cls, aliansce):
        aliansce = aliansce.select("id", "email", "updated", "providers", "cpf_cry", "cpf", "name",
                                   "birth_date",
                                   "gender", \
                                   get_json_object(aliansce.providers,
                                                   '$.bigdata_corp.extra_provider.BasicData.MotherName').alias(
                                       "mother_name_bigdata_corp"), \
                                   get_json_object(aliansce.providers, '$.lemit.nome_mae').alias("monther_name_lemit"))
#        aliansce = aliansce.drop(*["mother_name_bigdata_corp", "monther_name_lemit"])
        aliansce = aliansce \
            .withColumn("mother_trusted", upper(coalesce(aliansce["mother_name_bigdata_corp"],
                                                    aliansce["monther_name_lemit"])))        
        aliansce = aliansce.withColumnRenamed("mother_trusted", "relationship_mother")

        aliansce = aliansce.select("id", "email", "updated", "providers", "cpf_cry", "cpf", "name",
                                   "birth_date",
                                   "gender", "relationship_mother", \
                                   get_json_object(aliansce.providers,
                                                   '$.bigdata_corp.extra_provider.BasicData.FatherName').alias(
                                       "father_trusted"))

        aliansce = aliansce.withColumn("father_trusted", upper(col("father_trusted")))
        aliansce = aliansce.withColumnRenamed("father_trusted", "relationship_father")

        return aliansce

    @classmethod
    def buildNationality(cls, aliansce):
        aliansce = aliansce.select("id", "email", "updated", "providers", "cpf_cry", "cpf", "name",
                                   "birth_date",
                                   "gender", "relationship_mother", "relationship_father", \
                                   get_json_object(aliansce.providers,
                                                   '$.bigdata_corp.extra_provider.BasicData.BirthCountry').alias(
                                       "nationality_bigdata_corp"), \
                                   get_json_object(aliansce.providers, '$.fnrh.nationalityName').alias(
                                       "nationality_fnrh"))
        #        aliansce = aliansce.drop(*["nationality_fnrh", "nationality_bigdata_corp"])
        aliansce = aliansce \
            .withColumn("nationality_trusted", upper(coalesce(aliansce["nationality_fnrh"],
                                                         aliansce["nationality_bigdata_corp"])))                
        aliansce = aliansce.withColumnRenamed("nationality_trusted", "nationality")

        return aliansce

    @classmethod
    def buildEmails(cls, aliansce):
        aliansce = aliansce.select("id", "email", "updated", "providers", "cpf_cry", "cpf", "name",
                                   "birth_date",
                                   "gender", "relationship_mother", "relationship_father", "nationality", \
                                   get_json_object(aliansce.providers, '$.bigdata_corp.extra_provider.Emails').alias(
                                       "emails_extra_bigdata_corp"), \
                                   get_json_object(aliansce.providers, '$.bigdata_corp.email').alias(
                                       "emails_bigdata_corp"), \
                                   get_json_object(aliansce.providers, '$.lemit.emails').alias("emails_lemit"), \
                                   get_json_object(aliansce.providers, '$.zooxwifi.email').alias("emails_zooxwifi"), \
                                   get_json_object(aliansce.providers, '$.fnrh.email').alias("emails_fnrh"), \
                                   get_json_object(aliansce.providers, '$.facebook.email').alias("emails_facebook"))

        email_udf = udf(lambda x: cls.wrangle_email(x))
        aliansce = aliansce.withColumn("emails_extra_bigdata_corp1",
                                       email_udf(col("emails_extra_bigdata_corp").isNotNull()))
        aliansce = aliansce.withColumn("emails_extra_bigdata_corp1",
                                       get_json_object(aliansce.emails_extra_bigdata_corp1, '$.EmailAddress'))
        aliansce = aliansce. \
            withColumn("emails_extra_bigdata_corp1",
                       when(col("emails_extra_bigdata_corp1") == "", None).otherwise(
                           col("emails_extra_bigdata_corp1"))). \
            withColumn("emails_bigdata_corp",
                       when(col("emails_bigdata_corp") == "", None).otherwise(col("emails_bigdata_corp"))). \
            withColumn("emails_lemit", when(col("emails_lemit") == "", None).otherwise(col("emails_lemit"))). \
            withColumn("emails_fnrh", when(col("emails_fnrh") == "", None).otherwise(col("emails_fnrh"))). \
            withColumn("emails_zooxwifi", when(col("emails_zooxwifi") == "", None).otherwise(col("emails_zooxwifi"))). \
            withColumn("emails_facebook", when(col("emails_facebook") == "", None).otherwise(col("emails_facebook"))). \
            withColumn("email", when(col("email") == "", None).otherwise(col("email")))

        aliansce = aliansce \
            .withColumn("email_trusted", (coalesce(aliansce["emails_fnrh"],
                                                   aliansce["emails_bigdata_corp"],
                                                   aliansce["emails_lemit"],
                                                   aliansce["emails_extra_bigdata_corp1"],
                                                   aliansce["emails_zooxwifi"],
                                                   aliansce["emails_facebook"],
                                                   aliansce["email"])))

        aliansce = aliansce \
            .drop(*["emails_fnrh", "emails_bigdata_corp", "emails_lemit", "emails_extra_bigdata_corp1",
                                   "emails_extra_bigdata_corp", "emails_zooxwifi", "emails_facebook", "email"]) \
            .withColumnRenamed("email_trusted", "email")

        return aliansce

    @classmethod
    def buildPhone(cls, aliansce):
        aliansce = aliansce.select("id", "updated", "providers", "cpf_cry", "cpf", "name", "birth_date",
                                   "gender",
                                   "relationship_mother", "relationship_father", "nationality", "email", \
                                   get_json_object(aliansce.providers, '$.zooxwifi.phone').alias("zoox_phone"), \
                                   get_json_object(aliansce.providers, '$.bigdata_corp.phone').alias("bigdata_phone"), \
                                   get_json_object(aliansce.providers, '$.bigdata_corp.extra_provider.Phones').alias(
                                       "bigdata_extra_phone_all"), \
                                   get_json_object(aliansce.providers, '$.lemit.celulares').alias("lemit_mobile_all"), \
                                   get_json_object(aliansce.providers, '$.lemit.fixos').alias("lemit_fixo_all"), \
                                   get_json_object(aliansce.providers, '$.fnrh.ddd').alias("fnrh_ddd"), \
                                   get_json_object(aliansce.providers, '$.fnrh.phoneNumber').alias("fnrh_phone"), \
                                   get_json_object(aliansce.providers, '$.fnrh.updatedAt').alias("fnrh_updated"))

        phone_udf = udf(lambda x: cls.wrangle_phone_bigdata(x[0], x[1]))
        aliansce = aliansce \
            .withColumn("bigdata_extra_mobile_dict",
                                       phone_udf(struct(col("bigdata_extra_phone_all").isNotNull(), lit("MOBILE")))) \
            .withColumn("bigdata_extra_fixo_dict",
                                       phone_udf(struct(col("bigdata_extra_phone_all").isNotNull(), lit("HOME"))))

        phone_udf = udf(lambda x: cls.wrangle_phone_lemit(x))
        aliansce = aliansce \
            .withColumn("lemit_mobile_dict", phone_udf(col("lemit_mobile_all").isNotNull())) \
            .withColumn("lemit_fixo_dict", phone_udf(col("lemit_fixo_all").isNotNull()))

        aliansce = aliansce.select("id", "updated", "providers", "cpf_cry", "cpf", "name", "birth_date",
                                   "gender",
                                   "relationship_mother", "relationship_father", "nationality", "email", "zoox_phone",
                                   "fnrh_phone", "bigdata_phone", \
                                   "bigdata_extra_mobile_dict", "bigdata_extra_fixo_dict", "lemit_mobile_dict",
                                   "lemit_fixo_dict", \
                                   get_json_object(aliansce.bigdata_extra_mobile_dict, '$.AreaCode').alias(
                                       "bigdata_extra_mobileDDD_priority"), \
                                   get_json_object(aliansce.bigdata_extra_mobile_dict, '$.Number').alias(
                                       "bigdata_extra_mobile_priority"), \
                                   get_json_object(aliansce.bigdata_extra_fixo_dict, '$.AreaCode').alias(
                                       "bigdata_extra_fixoDDD_priority"), \
                                   get_json_object(aliansce.bigdata_extra_fixo_dict, '$.Number').alias(
                                       "bigdata_extra_fixo_priority"), \
                                   get_json_object(aliansce.lemit_mobile_dict, '$.ddd').alias(
                                       "lemit_mobileDDD_priority"), \
                                   get_json_object(aliansce.lemit_mobile_dict, '$.numero').alias(
                                       "lemit_mobile_priority"), \
                                   get_json_object(aliansce.lemit_fixo_dict, '$.ddd').alias("lemit_fixoDDD_priority"), \
                                   get_json_object(aliansce.lemit_fixo_dict, '$.numero').alias("lemit_fixo_priority"))

        aliansce = aliansce \
            .withColumn("bigdata_extra_mobile_priority", concat(col("bigdata_extra_mobileDDD_priority"),
                                                                               col("bigdata_extra_mobile_priority"))) \
            .withColumn("bigdata_extra_fixo_priority",
                                       concat(col("bigdata_extra_fixoDDD_priority"),
                                              col("bigdata_extra_fixo_priority"))) \
            .withColumn("lemit_mobile_priority",
                                       concat(col("lemit_mobileDDD_priority"), col("lemit_mobile_priority"))) \
            .withColumn("lemit_fixo_priority",
                                       concat(col("lemit_fixoDDD_priority"), col("lemit_fixo_priority")))
        aliansce = aliansce.select("id", "updated", "providers", "cpf_cry", "cpf", "name", "birth_date",
                                   "gender",
                                   "relationship_mother", "relationship_father", "nationality", "email", "zoox_phone",
                                   "fnrh_phone", "bigdata_phone", "bigdata_extra_mobile_priority",
                                   "bigdata_extra_fixo_priority", "lemit_mobile_priority", "lemit_fixo_priority") \
            .withColumn("phone_trusted", (coalesce(aliansce["fnrh_phone"], \
                                                   aliansce["bigdata_extra_mobile_priority"], \
                                                   aliansce["bigdata_phone"], \
                                                   aliansce["lemit_mobile_priority"], \
                                                   aliansce["zoox_phone"], \
                                                   aliansce["bigdata_extra_fixo_priority"], \
                                                   aliansce["lemit_fixo_priority"])))

        aliansce = aliansce.withColumn("phone_trusted", regexp_replace(col("phone_trusted"), r"\D", ""))

        type_udf = udf(lambda x: cls.get_type(x))
        check_udf = udf(lambda x: cls.check_phone_trusted(x))
        aliansce = aliansce \
            .withColumn("phone_type_trusted", type_udf(col("phone_trusted").isNotNull())) \
            .withColumn("phone_trusted", check_udf(col("phone_trusted").isNotNull()))

        #        aliansce = aliansce.drop(
        #            *['zoox_phone', 'fnrh_phone', 'bigdata_phone', 'bigdata_extra_mobile_priority',
        #              'bigdata_extra_fixo_priority',
        #              'lemit_mobile_priority', 'lemit_fixo_priority'])
        aliansce = aliansce \
            .withColumnRenamed("phone_trusted", "telephone_number") \
            .withColumnRenamed("phone_type_trusted", "telephone_type")

        return aliansce

    @classmethod
    def buildAdress(cls, aliansce):
        aliansce = aliansce.select("id", "updated", "providers", "cpf_cry", "cpf", "name", "birth_date",
                                   "gender",
                                   "relationship_mother", "relationship_father", "nationality", "email",
                                   "telephone_number",
                                   "telephone_type",
                                   get_json_object(aliansce.providers, '$.bigdata_corp.extra_provider.Addresses').alias(
                                       "all_addresses_bigdata_corp"),
                                   get_json_object(aliansce.providers, '$.bigdata_corp.main_address.address').alias(
                                       "address_home_street_bigdata_corp"),
                                   get_json_object(aliansce.providers, '$.lemit.enderecos').alias(
                                       "all_addresses_lemit"),
                                   get_json_object(aliansce.providers, '$.zooxwifi.city').alias(
                                       "address_home_city_zoox"),
                                   get_json_object(aliansce.providers, '$.fnrh.address').alias("address_home_street_f"),
                                   get_json_object(aliansce.providers, '$.fnrh.addressNumber').alias(
                                       "address_home_number_f"),
                                   get_json_object(aliansce.providers, '$.fnrh.neighborhood').alias(
                                       "address_home_neighborhood_f"),
                                   get_json_object(aliansce.providers, '$.fnrh.cityName').alias("address_home_city_f"),
                                   get_json_object(aliansce.providers, '$.fnrh.stateName').alias(
                                       "address_home_state_f"),
                                   get_json_object(aliansce.providers, '$.fnrh.countryId').alias(
                                       "address_home_country_f"),
                                   get_json_object(aliansce.providers, '$.fnrh.zipCode').alias(
                                       "address_home_zipcode_f"),
                                   get_json_object(aliansce.providers, '$.fnrh.addressComplement').alias(
                                       "address_home_complement_f"))
        uf = udf(lambda x: cls.uf_return(x))
        aliansce = aliansce \
            .withColumn("address_home_zipcode_f",
                                       regexp_replace(col("address_home_zipcode_f"), r"\D", "")) \
            .withColumn("uf_f", uf(col("address_home_state_f").isNotNull())) \
            .withColumn("fnrh_address",
                                       concat(col("address_home_street_f"), lit(", "), col("address_home_number_f"),
                                              lit(", "), col("address_home_complement_f"), lit(", "),
                                              col("address_home_neighborhood_f"), lit(", "), col("address_home_city_f"),
                                              lit(", "), col("uf_f"), lit(", "), col("address_home_zipcode_f"),
                                              lit(",0"))) \
            .withColumn("fnrh_address_location",
                                       concat(col("address_home_city_f"), lit(", "), col("uf_f"), lit(", "),
                                              lit("Brasil"),
                                              lit(",0")))
        # aliansce = aliansce.drop(
        #     *["address_home_street_f", "address_home_number_f", "address_home_complement_f",
        #       "address_home_neighborhood_f",
        #       "address_home_city_f", "uf_f", "address_home_zipcode_f", "address_home_state_f",
        #       "address_home_country_f"])

        addrs_udf = udf(lambda x, y: cls.get_priority_address(x, y))

        aliansce = aliansce \
            .withColumn("address_home", addrs_udf(col("all_addresses_bigdata_corp").isNotNull(), lit("HOME"))) \
            .withColumn("address_work", addrs_udf(col("all_addresses_bigdata_corp").isNotNull(), lit("WORK")))
        
        aliansce = aliansce \
            .withColumn("address_home_street", get_json_object(aliansce.address_home, '$.AddressMain')) \
            .withColumn("address_home_number", get_json_object(aliansce.address_home, '$.Number')) \
            .withColumn("address_home_neighborhood", get_json_object(aliansce.address_home, '$.Neighborhood')) \
            .withColumn("address_home_city", get_json_object(aliansce.address_home, '$.City')) \
            .withColumn("address_home_state", get_json_object(aliansce.address_home, '$.State')) \
            .withColumn("address_home_country", get_json_object(aliansce.address_home, '$.Country')) \
            .withColumn("address_home_zipcode", get_json_object(aliansce.address_home, '$.ZipCode')) \
            .withColumn("address_home_complement", get_json_object(aliansce.address_home, '$.Complement')) \
            .withColumn("address_work_street", get_json_object(aliansce.address_work, '$.AddressMain')) \
            .withColumn("address_work_number", get_json_object(aliansce.address_work, '$.Number')) \
            .withColumn("address_work_neighborhood", get_json_object(aliansce.address_work, '$.Neighborhood')) \
            .withColumn("address_work_city", get_json_object(aliansce.address_work, '$.City')) \
            .withColumn("address_work_state", get_json_object(aliansce.address_work, '$.State')) \
            .withColumn("address_work_country", get_json_object(aliansce.address_work, '$.Country')) \
            .withColumn("address_work_zipcode", get_json_object(aliansce.address_work, '$.ZipCode')) \
            .withColumn("address_work_complement", get_json_object(aliansce.address_work, '$.Complement'))
        
        aliansce = aliansce \
            .withColumn("uf", uf("address_home_state")) \
            .withColumn("uf_w", uf("address_work_state")) \
            .withColumn("bigdata_address_home",
                                       concat(col("address_home_street"), lit(", "), col("address_home_number"),
                                              lit(", "),
                                              col("address_home_complement"), lit(", "),
                                              col("address_home_neighborhood"),
                                              lit(", "), col("address_home_city"),
                                              lit(", "), col("uf"), lit(", "), col("address_home_zipcode"), lit(",0"))) \
            .withColumn("bigdata_address_work",
                                       concat(col("address_work_street"), lit(", "), col("address_work_number"),
                                              lit(", "),
                                              col("address_work_complement"), lit(", "),
                                              col("address_work_neighborhood"),
                                              lit(", "), col("address_work_city"),
                                              lit(", "), col("uf"), lit(", "), col("address_work_zipcode"), lit(",1"))) \
            .withColumn("bigdata_address_home_location",
                                       concat(col("address_home_city"), lit(", "), col("uf"), lit(", "), lit("Brasil"),
                                              lit(",0"))) \
            .withColumn("bigdata_address_work_location",
                                       concat(col("address_work_city"), lit(", "), col("uf_w"), lit(", "),
                                              lit("Brasil"),
                                              lit(",1")))

        # aliansce = aliansce.drop(
        #     *["all_addresses_bigdata_corp", "address_home", "address_home_street", "address_home_number",
        #       "address_home_complement", "address_home_neighborhood", "address_home_city", "uf", "address_home_zipcode",
        #       "address_home_state", "address_home_country"])
        # aliansce = aliansce.drop(
        #     *["address_work", "address_work_street", "address_work_number", "address_work_complement",
        #       "address_work_neighborhood", "address_work_city", "uf_w", "address_work_zipcode",
        #       "address_work_state", "address_work_country"])

        aliansce = aliansce \
            .withColumn("address_home", addrs_udf(col("all_addresses_lemit").isNotNull(), lit("residencial"))) \
            .withColumn("address_work", addrs_udf(col("all_addresses_lemit").isNotNull(), lit("comercial")))

        aliansce = aliansce \
            .withColumn("address_home_street", get_json_object(aliansce.address_home, '$.endereco')) \
            .withColumn("address_home_number", get_json_object(aliansce.address_home, '$.numero')) \
            .withColumn("address_home_neighborhood", get_json_object(aliansce.address_home, '$.bairro')) \
            .withColumn("address_home_city", get_json_object(aliansce.address_home, '$.cidade')) \
            .withColumn("address_home_state", get_json_object(aliansce.address_home, '$.uf')) \
            .withColumn("address_home_zipcode", get_json_object(aliansce.address_home, '$.cep')) \
            .withColumn("address_home_complement", get_json_object(aliansce.address_home, '$.complemento')) \
            .withColumn("address_work_street", get_json_object(aliansce.address_work, '$.endereco')) \
            .withColumn("address_work_number", get_json_object(aliansce.address_work, '$.numero')) \
            .withColumn("address_work_neighborhood", get_json_object(aliansce.address_work, '$.bairro')) \
            .withColumn("address_work_city", get_json_object(aliansce.address_work, '$.cidade')) \
            .withColumn("address_work_state", get_json_object(aliansce.address_work, '$.uf')) \
            .withColumn("address_work_zipcode", get_json_object(aliansce.address_work, '$.cep')) \
            .withColumn("address_work_complement", get_json_object(aliansce.address_work, '$.complemento'))

        aliansce = aliansce \
            .withColumn("uf", uf(col("address_home_state").isNotNull())) \
            .withColumn("uf_w", uf(col("address_work_state").isNotNull())) \
            .withColumn("lemit_address_home",
                                       concat(col("address_home_street"), lit(", "), col("address_home_number"),
                                              lit(", "),
                                              col("address_home_complement"), lit(", "),
                                              col("address_home_neighborhood"),
                                              lit(", "), col("address_home_city"),
                                              lit(", "), col("uf"), lit(", "), col("address_home_zipcode"), lit(",0"))) \
            .withColumn("lemit_address_work",
                                       concat(col("address_work_street"), lit(", "), col("address_work_number"),
                                              lit(", "),
                                              col("address_work_complement"), lit(", "),
                                              col("address_work_neighborhood"),
                                              lit(", "), col("address_work_city"),
                                              lit(", "), col("uf"), lit(", "), col("address_work_zipcode"), lit(",1"))) \
            .withColumn("lemit_address_home_location",
                                       concat(col("address_home_city"), lit(", "), col("uf"), lit(", "), lit("Brasil"),
                                              lit(",0"))) \
            .withColumn("lemit_address_work_location",
                                       concat(col("address_work_city"), lit(", "), col("uf_w"), lit(", "),
                                              lit("Brasil"),
                                              lit(",1")))

        # aliansce = aliansce.drop(*["all_addresses_lemit", "address_home", "address_home_street", "address_home_number",
        #                            "address_home_complement", "address_home_neighborhood", "address_home_city", "uf",
        #                            "address_home_zipcode", "address_home_state"])
        # aliansce = aliansce.drop(
        #     *["address_work", "address_work_street", "address_work_number", "address_work_complement",
        #       "address_work_neighborhood", "address_work_city", "uf_w", "address_work_zipcode",
        #       "address_work_state"])

        uf_z = udf(lambda x: cls.uf_zoox(x))
        aliansce = aliansce \
            .withColumn("address_home_city_zoox_2", uf_z(col("address_home_city_zoox").isNotNull()))
        
        aliansce = aliansce \
            .withColumn("address_home_city_zoox_2", concat(col("address_home_city_zoox_2"), lit(",0"))) \
            .withColumn("address_trusted", (coalesce(aliansce['fnrh_address'],
                                                     aliansce['bigdata_address_home'],
                                                     aliansce['bigdata_address_work'],
                                                     aliansce['lemit_address_home'],
                                                     aliansce['lemit_address_work'])))
        
        aliansce = aliansce \
            .withColumn("address_location_trusted", (coalesce(aliansce['fnrh_address_location'],
                                                              aliansce['bigdata_address_home_location'],
                                                              aliansce['bigdata_address_work_location'],
                                                              aliansce['lemit_address_home_location'],
                                                              aliansce['lemit_address_work_location'],
                                                              aliansce['address_home_city_zoox_2'])))

        # aliansce = aliansce.drop(
        #     *['fnrh_address', 'bigdata_address_home', 'bigdata_address_work', 'lemit_address_home',
        #       'lemit_address_work',
        #       'address_home_street_bigdata_corp', 'address_home_city_zoox', 'address_home_city_zoox_2'])
        # aliansce = aliansce.drop(
        #     *['fnrh_address_location', 'bigdata_address_home_location', 'bigdata_address_work_location',
        #       'lemit_address_home_location', 'lemit_address_work_location'])

        remove_udf = udf(lambda x: cls.remove_type(x))
        
        aliansce = aliansce \
            .withColumn("address_type_trusted", cls.type_udf(col("address_trusted").isNotNull())) \
            .withColumn("address_trusted", remove_udf(col("address_trusted").isNotNull())) \
            .withColumn("address_location_trusted",
                                       remove_udf(col("address_location_trusted").isNotNull()))

        aliansce = aliansce \
            .withColumnRenamed("address_trusted", "address") \
            .withColumnRenamed("address_type_trusted", "address_type") \
            .withColumnRenamed("address_location_trusted", "address_location")

        return aliansce

    @classmethod
    def buildPhoto(cls, aliansce):
        remove_udf = udf(lambda x: cls.remove_type(x))
        aliansce = aliansce.select("id", "updated", "providers", "cpf_cry", "cpf", "name", "birth_date",
                                   "gender",
                                   "relationship_mother", "relationship_father", "nationality", "email",
                                   "telephone_number",
                                   "telephone_type", "address", "address_type", "address_location",
                                   get_json_object(aliansce.providers, '$.facebook.cover_photo_url').alias(
                                       "cover_photo_url_face"),
                                   get_json_object(aliansce.providers, '$.zooxwifi.cover_photo_url').alias(
                                       "cover_photo_url_zoox"))

        aliansce = aliansce \
            .withColumn("cover_photo_url_trusted", (coalesce(aliansce["cover_photo_url_zoox"],
                                                             aliansce["cover_photo_url_face"])))

        # aliansce = aliansce.drop(*["cover_photo_url_zoox", "cover_photo_url_face"])
        aliansce = aliansce.withColumnRenamed("cover_photo_url_trusted", "cover_photo_url")

        aliansce = aliansce.select("id", "updated", "providers", "cpf_cry", "cpf", "name", "birth_date",
                                   "gender",
                                   "relationship_mother", "relationship_father", "nationality", "email",
                                   "telephone_number",
                                   "telephone_type", "address", "address_type", "address_location", "cover_photo_url", \
                                   get_json_object(aliansce.providers, '$.facebook.profile_url').alias(
                                       "profile_url_face"),
                                   get_json_object(aliansce.providers, '$.zooxwifi.profile_url').alias(
                                       "profile_url_zoox"),
                                   get_json_object(aliansce.providers, '$.google.profile_url').alias(
                                       "profile_url_google"),
                                   get_json_object(aliansce.providers, '$.twitter.profile_url').alias(
                                       "profile_url_twitter"),
                                   get_json_object(aliansce.providers, '$.linkedin.profile_url').alias(
                                       "profile_url_linkedin"))

        aliansce = aliansce \
            .withColumn("profile_url_zoox", concat(col("profile_url_zoox"), lit(" ,3"))) \
            .withColumn("profile_url_face", concat(col("profile_url_face"), lit(" ,1"))) \
            .withColumn("profile_url_google", concat(col("profile_url_google"), lit(" ,2"))) \
            .withColumn("profile_url_twitter", concat(col("profile_url_twitter"), lit(" ,4"))) \
            .withColumn("profile_url_linkedin", concat(col("profile_url_linkedin"), lit(" ,5")))

        aliansce = aliansce \
            .withColumn("profile_url_trusted", (coalesce(aliansce["profile_url_zoox"],
                                                         aliansce["profile_url_face"],
                                                         aliansce["profile_url_google"],
                                                         aliansce["profile_url_twitter"],
                                                         aliansce["profile_url_linkedin"])))

        aliansce = aliansce \
            .withColumn("social_media_type", cls.type_udf(col("profile_url_trusted").isNotNull())) \
            .withColumn("profile_url_trusted_2", remove_udf(col("profile_url_trusted").isNotNull()))

        aliansce = aliansce.withColumnRenamed("profile_url_trusted_2", "profile_url")
        return aliansce

    @classmethod
    def writeData(cls, aliansce, path):        
        aliansce = aliansce.select("id", "updated", "providers", "cpf_cry", "cpf", "name", "birth_date",
                                   "gender",
                                   "relationship_mother", "relationship_father", "nationality", "email",
                                   "telephone_number",
                                   "telephone_type", "address", "address_type", "address_location", "cover_photo_url",
                                   "social_media_type", "profile_url", \
                                   get_json_object(aliansce.providers,
                                                   '$.bigdata_corp.extra_provider.BasicData.HasObitIndication').alias(
                                       "big_deceased"),
                                   get_json_object(aliansce.providers, '$.lemit.falecido').alias("lemi_deceased"))

        obit_udf = udf(lambda x: cls.obit(x))

        aliansce = aliansce.select("id", "updated", "providers", "cpf_cry", "cpf", "name", "birth_date",
                                   "gender",
                                   "relationship_mother", "relationship_father", "nationality", "email",
                                   "telephone_number",
                                   "telephone_type", "address", "address_type", "address_location", "cover_photo_url",
                                   "social_media_type", "profile_url", "big_deceased", "lemi_deceased").withColumn(
            "deceased", (coalesce(aliansce["big_deceased"], aliansce["lemi_deceased"])))

        aliansce = aliansce.withColumn("HasObitIndication", obit_udf(col("deceased").isNotNull()))

        aliansce = aliansce.withColumn("ts", to_timestamp(col("updated"))).withColumn("data_ref", to_date(col("ts")))

        aliansce = aliansce.select("id", "cpf_cry", "cpf", "data_ref", "name", "birth_date", "gender",
                                   "relationship_mother", "relationship_father",
                                   "nationality", "email", "telephone_number", "telephone_type", "address",
                                   "address_type",
                                   "address_location", "social_media_type", "cover_photo_url", "profile_url",
                                   "HasObitIndication")

        aliansce = aliansce.withColumnRenamed("data_ref", "date_ref")        

        #aliansce.coalesce(1).write.mode("overwrite").save(path)
        aliansce.repartition(1).write.mode("overwrite").save(path)

    def decoding_udf(self, df, key):        
        decode_udf = udf(exporterAliansceService.decrypt)
        for field in df.columns:
            df = df.withColumn(field+"_dec", decode_udf(field, lit(key)))        
        return df

    def encoding_udf(self, df, key):                
        encode_udf = udf(exporterAliansceService.encrypt)
        for field in df.columns:
            df = df.withColumn(field, encode_udf(col(field), lit(key)))        
        return df
    
    def get_person(self, aliansce):
        list_cpfs = []
        
        list_rows = aliansce.select("cpf_dec").collect()
        for row in list_rows:
            list_cpfs.append(row["cpf_dec"])

        print("Lendo person")
        #person_raw = self.spark.read.load(
        #    "s3://zd-results/user/gabriel/serasa/aux/unique_id_2/")  # Depende da base de persons
        person = self.spark.read.format("hudi").load("s3://zd-datalake-hudi/raw/zooxwifi/schemas/person/person/*/*")
        person = person \
            .select(
                "id",
                "name",
                "email",
                "created",
                "updated",
                "providers",
                coalesce(
                    regexp_extract(get_json_object(person.providers, '$.zooxwifi.cpf'), "\\d+", 0),
                    regexp_extract(get_json_object(person.providers, '$.bigdata_corp.cpf'), "\\d+", 0),
                    regexp_extract(get_json_object(person.providers, '$.lemit.cpf'), "\\d+", 0),
                    regexp_extract(get_json_object(person.providers, '$.fnrh.cpf'), "\\d+", 0),
                    regexp_extract(get_json_object(person.providers, '$.ipiranga_kmv.cpf'), "\\d+", 0)).alias("cpf")) \
                .filter((col("cpf").isNotNull()))                    

        print("Tratamento no CPF")
        person = person \
            .withColumn("cpf",regexp_replace(col("cpf"), r"\D",""))            
            #.withColumn("cpf", regexp_extract(col("cpf"), "\\d+", 0))
        
        person = person.withColumn("cpf", lpad(person.cpf,11, '0'))
        person = person.filter((col("cpf").isin(list_cpfs)))
        return person.orderBy(col("updated").desc()).dropDuplicates(["cpf"])

    def set_log(self, message, new=False):        
        s3_client = boto3.resource('s3')
        s3_path = "s3://{}/".format(self.temp_bucket)        
        result = ""
        obj = s3_client.Object(self.temp_bucket, self.log)

        if new == False:
            log = str(obj.get()['Body'].read().splitlines()).replace("b'", "").replace("'", "").replace("[", "").replace("]", "")
            log = log.split(",")
            for line in log:
                if result == "":
                    result = line
                else:
                    result = result + "\n" + line.strip()
            log = result + "\n" + message
        else:
            log = message

        obj = s3_client.Object(self.temp_bucket, self.log)
        obj.put(Body=log)

    def parte1(self):
        aliansce = self.spark.read.format("json").load("s3://{}/{}".format(self.bucket, self.lastFile))
        
        print("Descriptografando o arquivo do dia {}".format(self.lastFile))                
        aliansce = self.decoding_udf(aliansce, self.pwd)
        
        self.set_log("Registros no arquivo da ALSO: {}".format(aliansce.count()), True)

        aliansce = aliansce \
            .withColumn("cpf_dec", lpad(aliansce.cpf_dec, 11, '0')) \
            .withColumnRenamed("cpf", "cpf_cry") \
            .withColumnRenamed("email", "email_cry") \
            .drop(*["shop_codigo", "shop_nome", "shop_nome_dec", "shop_codigo_dec"]) \
            .dropDuplicates(["cpf_dec"])        
        self.set_log("Registros unicos no arquivo da ALSO: {}".format(aliansce.count()))
        
        person_raw = self.get_person(aliansce)        
        
        aliansce = aliansce \
            .join(person_raw, aliansce.cpf_dec == person_raw.cpf, "inner")
        #aliansce.filter(col("cpf").isNull()).repartition(1).write.mode("overwrite").save(self.parte1_path+"tmp/")
        #aliansce = aliansce.filter(col("cpf").isNotNull())
        print("Aliansce e Person relacionados")
        
        aliansce.repartition(1).write.mode("overwrite").save(self.parte1_path)
        print("Arquivo salvo")

    def parte2(self):          
        aliansce = self.spark.read.load(self.parte1_path)
        self.set_log("Registros do relacionamento ALSO com Person: {}".format(aliansce.count()))
        print("Doing buildName: ")
        aliansce = self.buildName(aliansce)                
        print("Doing buildBirth: ")
        aliansce = self.buildBirth(aliansce)
        print("Doing buildGender: ")
        aliansce = self.buildGender(aliansce)
        print("Doing buildParents: ")
        aliansce = self.buildParents(aliansce)
        print("Doing buildNationality: ")
        aliansce = self.buildNationality(aliansce)
        print("Doing buildEmails: ")
        aliansce = self.buildEmails(aliansce)
        print("Doing buildPhone: ")
        aliansce = self.buildPhone(aliansce)
        print("Doing buildAdress: ")
        aliansce = self.buildAdress(aliansce)
        print("Doing buildPhoto: ")
        aliansce = self.buildPhoto(aliansce)    
        
        print("Finishing the process - writting data ")
        self.writeData(aliansce, self.parte2_path)
        print("The data has been written!")

    #PARTE 3
    def parte3(self):                
        #windowSpec = Window.partitionBy("person").orderBy(col("created").desc())
        #self.spark.read.load(self.parte2_path).createOrReplaceTempView("parte2_temp")
        aliansce = self.spark.read.load(self.parte2_path)
        list_rows = aliansce.select("id").collect()

        list_id_persons = []

        for row in list_rows:
            list_id_persons.append(row["id"])
        
        login = self.spark.sql("""
            SELECT created, expires, person, person_name, ip, mac, agent, company, company_name,
            hotspot, download, duration, social_network, country, age, device_type, device_os, new,
            'null' as client_id, groups, person_data, active, hotspot_name, group_id, term_accept
            FROM history.login_portal login
            WHERE person is not null
            """).filter(col("person").isin(list_id_persons))
        self.set_log("Registro da login portal filtrada pelos Ids da tabela Person: {}".format(login.count()))
        
        login.coalesce(5).write.mode("overwrite").save(self.parte3_path)
                            
    # Parte 4
    def parte4(self):
        login = self.spark.read.load(self.parte3_path) \
            .withColumn("hotspot_login", get_json_object(col("person_data"), '$.email')) \
            .withColumn("hotspot_last_login", col("created")) \
            .withColumn("hotspot_term_accept_date", col("created")) \
            .withColumnRenamed("duration", "hotspot_duration") \
            .withColumnRenamed("social_network", "hotspot_social_network") \
            .withColumnRenamed("country", "hotspot_country") \
            .withColumnRenamed("device_type", "hotspot_device_type") \
            .withColumnRenamed("device_os", "hotspot_device_os") \
            .withColumnRenamed("new", "hotspot_new") \
            .withColumnRenamed("term_accept", "hotspot_term_accept") \
            .withColumnRenamed("agent", "at")            

        part2 = self.spark.read.load(self.parte2_path)
        
        login_only_aliansce = login.filter(col("group_id") == "90edfe7e-67a4-496b-9fc0-7d164ad56da3")
                        
        windowSpec = Window.partitionBy("person").orderBy(col("created").desc())
        login_only_aliansce_last = login_only_aliansce.withColumn("row_number", row_number().over(windowSpec)).where(col('row_number') == 1)

        part2_only_aliansce = part2.join(login_only_aliansce_last, part2.id==login_only_aliansce_last.person, "left")
        #part2_only_aliansce = part2.join(login_only_aliansce, part2.id == login_only_aliansce.person, "left")

        part2_only_aliansce_match = part2_only_aliansce.filter(col("person").isNotNull())

        part2_not_aliansce = part2_only_aliansce.filter(col("person").isNull())

        part2_not_aliansce = part2_not_aliansce.select('id', 'cpf_cry', 'cpf', 'date_ref', 'name', 'birth_date',
                                                    'gender', 'relationship_mother', 'relationship_father', 'nationality',
                                                    'email', 'telephone_number', 'telephone_type', 'address', 'address_type',
                                                    'address_location', 'social_media_type', 'cover_photo_url',
                                                    'profile_url', 'HasObitIndication')

        login_all_last = login.withColumn("row_number", row_number().over(windowSpec)).where(col('row_number') == 1)
        #part2_not_aliansce_match = part2_not_aliansce.join(login, part2_not_aliansce.id == login.person,
        #                                                "left")        

        part2_not_aliansce_match = part2_not_aliansce.join(login_all_last, part2_not_aliansce.id==login_all_last.person, "left")
                
        part4 = part2_only_aliansce_match.union(part2_not_aliansce_match)
        
        part4 = part4.filter(col("person").isNotNull()).drop("id")
        self.set_log("Registro unico por CPFs encontrado na login portal: {}".format(part4.count()))
        self.set_log("Registros ALSO: {}".format(part2_only_aliansce_match.count()))        

        self.spark.catalog.refreshTable("history.login_portal")
        self.spark.catalog.clearCache()
        part4.unpersist()
        
        part4.repartition(1).write.mode("overwrite").save(self.parte4_path)
        
    # Parte 5
    def parte5(self):
        part4 = self.spark.read.load(self.parte4_path)        

        part4 = part4.drop("groups", "client_id", "row_number")

        cont = 0
        while cont < 3:
            try:
                company = self.spark.read.load("s3://zd-datalake/raw/zooxwifi/schemas/company/company/")

                company_type = self.spark.read.load("s3://zd-datalake/raw/zooxwifi/schemas/company/company_type/")
                cont = 3
            except Exception as e:
                cont = cont + 1
                print(e)
        
        company = company.select(col("id").alias("company_id"), "type_id")

        company_type = company_type.select(col("id").alias("id_type"), col("name").alias("hotspot_company_name"))

        comp = company.join(company_type, company.type_id == company_type.id_type, "left")

        comp = comp.select("company_id", "hotspot_company_name")

        part5 = part4.join(comp, part4.company == comp.company_id, "left")

        part5 = part5 \
            .select("cpf", "person_name", "email", "birth_date",
            "telephone_number", "telephone_type", "address", "address_type", "Relationship_mother",
            "Relationship_father", "gender", "nationality", "Name", "address_location",
            "social_media_type", col("cover_photo_url").alias("cover_photo"), "profile_url",
            "hotspot_company_name", "hotspot_name", "hotspot_last_login", "hotspot_duration",
            "hotspot_login", "hotspot_social_network", "hotspot_country", "hotspot_device_type",
            "hotspot_device_os", "hotspot_new", "hotspot_term_accept", "hotspot_term_accept_date",
            "created", "expires", "ip", "mac", "at", "company", "company_name", "hotspot")
        
        print("Criptografando o arquivo com {} e com {} ALSO".format(part5.count(), part5.filter(col("group_id")=="90edfe7e-67a4-496b-9fc0-7d164ad56da3").count()))
        part5 = self.encoding_udf(part5, self.pwd)
        print("Salvando na pasta final")
        #print("lastFile {}".format(self.lastFile))
        parte5_path = "s3://{}/filesout/{}/".format(self.bucket, self.lastFile.split("/")[1])
        
        part5.repartition(5).write.mode("overwrite").json(parte5_path)
        
        s3 = S3(boto3, 'us-east-1')
        print("Renomeando arquivos")
        s3.rename_files(self.bucket, parte5_path.replace("s3://{}/".format(self.bucket), ""), "aliansce_qualify_encry")
        #s3 = boto3.resource('s3')
        bucket = s3.s3_client.Bucket("zd-results")
        
        print("Excluindo arquivos intermediarios")
        #Excluindo arquivos intermediarios
        bucket.object_versions.filter(Prefix=self.parte1_path[16:]).delete()                
        bucket.object_versions.filter(Prefix=self.parte2_path[16:]).delete()        
        bucket.object_versions.filter(Prefix=self.parte3_path[16:]).delete()        
        bucket.object_versions.filter(Prefix=self.parte4_path[16:]).delete()

        log_path = "logs/{}/{}.txt".format(self.lastFile.split("/")[1], self.nameFile.split(".")[0])
        print(log_path)
        s3.move_objects(self.temp_bucket, self.log, log_path, False, False, self.bucket)
