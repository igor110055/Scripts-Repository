######################################
######## -*- coding: utf-8 -*-########
########## MONITORA ARQUIVOS #########
########### POST API POWER BI ########
############# Main ###################
#############Versão - 1.0#############
######################################

import sys
import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import shlex, subprocess

class Watcher:

    #DIRECTORY_TO_WATCH="abfs://sparkazredhdi-dev@azredlake.dfs.core.windows.net/data/raw/FICO/AllCheckAnaliseFullOutput/streaming/"
    
    DIRECTORY_TO_WATCH = sys.argv[1]
    URL_API=sys.argv[2]
    CMD_CURL=sys.argv[3]
	
	
    def __init__(self):
        self.observer = Observer()
      
    def run(self):

        #URL_API = "https://api.powerbi.com/beta/651df868-aa8e-42fb-a98d-71aa2cb2974d/datasets/95fe392b-0b90-47b0-9f94-501b211840ec/rows?key=Io4ri6dqBUgHLDQIYCAQQ2Ini%2F4Fvr%2FSUb1mAd%2FRtbx7xMtQGNwi5yqA%2F1ToVa%2FTWbUJuFGz%2BmURrsn%2BzWCY2g%3D%3D"
        #CMD_CURL = 'curl --include --request POST --header "Content-Type: application/json" --data-binary'
        event_handler = Handler()
        self.observer.schedule(event_handler, self.DIRECTORY_TO_WATCH, recursive=True)
        #self.observer.start()
        print('inicio - ouvindo arquivos')

        try:
            while True:
                cmd_hdfs = "hdfs dfs -ls abfs://sparkazredhdi-dev@azredlake.dfs.core.windows.net/data/raw/FICO/AllCheckAnaliseFullOutput/streaming/  | grep -E '^-' | wc -l"
                print (cmd_hdfs)
                qtd_eventos = subprocess.check_output(cmd_hdfs, shell=True)
                print(qtd_eventos)
                cmd_post_pb =  CMD_CURL + ' "' + "[{\\" + '"' + "qtd" + '\\"' + ":" + str(qtd_eventos) + "}]" + '" ' + ' "' + URL_API + '" '
                print(cmd_post_pb)
                time.sleep(1)
                subprocess.call(cmd_post_pb, shell=True)

        except Exception as ex:
            df3 = 'ERRO: {}'.format(str(ex))
            self.observer.stop()
            print(df3)

       # self.observer.join()
        print('saiu while')



class Handler(FileSystemEventHandler):

    @staticmethod
    def on_any_event(event):
        if event.is_directory:
            return None

        elif event.event_type == 'created':
            # Take any action here when a file is first created.
            print("Received created event - %s." % event.src_path)

        elif event.event_type == 'modified':
            # Taken any action here when a file is modified.
            print("Received modified event - %s." % event.src_path)

if __name__ == '__main__':
    w = Watcher()
    w.run()