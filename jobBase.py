from os.path import join, dirname
from dotenv import load_dotenv
from datetime import date, datetime

from pyspark.sql import SparkSession, HiveContext
from zooxdata.envManager import EnvManager
from zooxdata.slackNotifier import SlackNotifier

class JobBase:
    """
        Classe base para os jobs a serem executados no EMR
    """

    def __init__(self, pex_file=None):
        self.pex_file = pex_file
        self.spark = None
        self.job_name = ""
        self.submit_args = "--packages=org.apache.hadoop:hadoop-aws:2.7.3 pyspark-shell"
        self.application_name = None
        self.slack_webhook_url = None
        self.slack_user_name = None
        self.debug = False

    def init_job(self):
        # Loading local settings
        dotenv_path = join(dirname(__file__), '.env')
        load_dotenv(dotenv_path)

    def log_process(self, msg):
        now = datetime.now()
        # self.logger.info('%s - %s' % (now.strftime('%Y-%m-%d-%H:%M:%S'), msg))
        print('%s - %s' % (now.strftime('%Y-%m-%d-%H:%M:%S'), msg))

    def run(self):
        self.init_job()

        envManager = EnvManager.get_instance()
        envManager.init_application(self.submit_args)

        if self.pex_file:
            self.spark = SparkSession \
                .builder \
                .master("yarn") \
                .config("spark.submit.deployMode", "client") \
                .config("spark.yarn.dist.files", self.pex_file) \
                .config("spark.executorEnv.PEX_ROOT", "./.pex") \
                .config("spark.jars", "/tmp/zoox/RedshiftJDBC42-no-awssdk-1.2.45.1069.jar") \
                .appName(self.job_name) \
                .enableHiveSupport() \
                .getOrCreate()
        else:
            self.spark = SparkSession \
                .builder \
                .master("local[*]") \
                .appName(self.job_name) \
                .enableHiveSupport() \
                .getOrCreate()

        envManager.setting_to_run(self.spark)
        # self.logger = Log4j(self.spark)

        self.log_process("[START] - {}".format(self.job_name))

    def execute(self):
        try:
            self.log_process("[START] - {}".format(self.job_name))

            self.run()

        except Exception as e:
            self.log_process('[ERRO] - %s' % e)

            if not self.debug:
                slack = SlackNotifier(self.slack_webhook_url,
                                      self.application_name,
                                      self.slack_user_name,
                                      self.job_name)
                slack.send("Erro em job {}".format(self.job_name), str(e))

            #Re-raise exception to EMR can log
            raise
        else:
            self.log_process("[SUCCESS] - {}".format(self.job_name))
        finally:
            self.log_process("[FINISH] - {}".format(self.job_name))

