from emr_ingestion.jobs.jobApplicationBase import JobApplicationBase
from datetime import datetime
import boto3
from zooxdata.mappers.hudiMapper import HudiMapper
from zooxdata.s3 import S3


class FileIngestion(JobApplicationBase):
    """
    Classe para tratar arquivos de ingestão não Opt In
    """
    def __init__(self, pex_file=None, is_full=False):    

        super().__init__(pex_file, is_full)        
        
        self.job_name = "FileIngestion"                        
        #Para se preenchido na herança    
        self.provider = None       
        self.prefix = None        
        self.input_bucket = None
        self.origin = None                      
        
    def validate_filename(self, key):
        """
        Valida o nome do arquivo
        :param key: Chave/prefixo do objeto S3
        :return: Origem, ano, mês e dia que constam no arquivo validado.
        """
        try:
            file = key.split("/")[-1]
            origin = file.split(".")[0]
            data = file.split(".")[2].split("_")[0]

            if len(data) == 8:
                if self.validate_date(data):
                    year = data[0:4]
                    month = data[4:6]
                    day = data[6:8]                
            
            return origin, year, month, day
        except:
            raise Exception("Arquivo com nome incorreto")
    
    def validate_date(self, data):
        """
        Validação de data
        :param data: Data a ser validada
        :return: Verdadeiro se for validado, caso contrário, falso.
        """
        try:
            datetime.strptime(data, "%Y%m%d")
            return True
        except:
            return False
    
    def run(self):
        super().run()

        s3_path = 's3://{}/'.format(self.input_bucket)

        s3_client = S3(boto3, 'us-east-1')        
        hudi_mapper = HudiMapper(self.spark)
        
        self.log_process("[PASSO 0] - Carregando {}".format(self.provider))
        
        list_keys = s3_client.list_directory(self.input_bucket, self.prefix)

        options = {
        'header': 'true'
        }
        
        if (len(list_keys) == 1 and list_keys[0] == self.prefix) or (len(list_keys) == 0):
            self.log_process("Sem arquivo para processar")
            return
        else:
            for key in list_keys:
                if key != self.prefix:
                    
                    df = self.query(s3_path + key, "csv", options)
                            
                    self.origin, year, month, day = self.validate_filename(key)
                    
                    self.log_process("[PASSO 1] - Formatando resultado")
                    
                    df = self.do_ingestion(df, year, month, day)                   
                    df = self.wrapDF(df, "key_field", ["origem", "id", "cpf", "year", "month", "day", "updated", "created", "providers"])        
                    
                    ############################################ nome da tabela                                                            
                            
                    self.log_process("[PASSO 2] - Inserindo no Hudi")                
                    self.hudi_path_output = self.hudi_path_output + "/" + self.TABLE_OUTPUT
                    hudi_mapper.write_dataset(df,
                        self.hudi_path_output,
                        self.TABLE_OUTPUT,
                        "id",
                        "updated",
                        "year",
                        'upsert',
                        'overwrite' if self.is_full == True else 'append')            
                    
                    self.log_process("[PASSO 3] - Inserido no Hudi com sucesso")
                                       
                    s3_client.move_objects(self.input_bucket, key, key.replace("to_process", "processed"), False, False)                    

    def do_ingestion(self, df, year, month, day):
        raise Exception("Not been implemented!")
