from pyspark.sql.functions import struct, col, to_json, lit, current_timestamp, date_format, concat, upper, trim, substring, when
from emr_ingestion.jobs.fileIngestion import FileIngestion


class JobIngestSocio(FileIngestion):
    '''
    Classe para execução do JobIngestSocio
    '''
    def __init__(self, pex_file=None, is_full=False):     
        super().__init__(pex_file, is_full)        
        self.job_name = "jobIngestSocio"        
        self.provider = "socio"        
        self.prefix = 'external/to_process/socio_empresa/'                
        self.input_bucket = 'zd-ingestion'
        self.TABLE_OUTPUT = "socio_empresa"

    def do_ingestion(self, df, year, month, day):
        """
        Trata o dataframe de ingestão do socio empresa
        :param df: Dataframe a ser tratado
        :param year: Para criação da coluna year
        :param month: Para criação da coluna month
        :param day: Para criação da coluna day
        :return: Dataframe tratado
        """

        df = df.na.drop(subset=["documento_socio"])

        df = df.withColumn("nome_socio", upper(trim(col("nome_socio"))))
        df = df.withColumn("cpf", concat(substring(trim(col("documento_socio")), 1, 3), lit("."), substring(trim(col("documento_socio")), 4, 3), lit("."),   substring(trim(col("documento_socio")), 7, 3), lit("-"), substring(trim(col("documento_socio")), 10, 2)))
        df = df.withColumn("status", when(col("documento_socio").isNull(), "outros").otherwise("ativa"))

        df = df.select("nome_socio", "cpf", "uf", "cnpj", "status", "cnae_classificacao", "porte_empresa").dropDuplicates()
        df = df.withColumn("socio_empresa", struct("nome_socio", "cpf", "uf", "cnpj", "status", "cnae_classificacao", "porte_empresa"))
        df = df.withColumn("providers", to_json(struct("socio_empresa")))
        
        df = df.withColumn("updated", date_format(
                current_timestamp().cast("string"),
                "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
            ))
        df = df.withColumn("created", date_format(
                current_timestamp().cast("string"),
                "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
            ))
        df = df.withColumn("origem", lit(self.origin))
        df = df.withColumn("year", lit(year).cast("integer"))
        df = df.withColumn("month", lit(month).cast("integer"))
        df = df.withColumn("day", lit(day).cast("integer"))
        df = df.withColumn("key_field", col("cpf"))
        
        return df
