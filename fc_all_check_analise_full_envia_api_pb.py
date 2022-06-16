######################################
######## -*- coding: utf-8 -*-########
############## ALL CHECK #############
########### POST API POWER BI ########
############# Main ###################
#############Versão - 1.0#############
######################################
import sys
import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import shlex, subprocess
from watchgod import watch


class Watcher:

    # DIRECTORY_TO_WATCH = "https://azredlake.dfs.core.windows.net/sparkazredhdi-dev/data/raw/FICO/AllCheckAnaliseFullOutput/streaming/"
    URL_API="https://api.powerbi.com/beta/651df868-aa8e-42fb-a98d-71aa2cb2974d/datasets/95fe392b-0b90-47b0-9f94-501b211840ec/rows?key=Io4ri6dqBUgHLDQIYCAQQ2Ini%2F4Fvr%2FSUb1mAd%2FRtbx7xMtQGNwi5yqA%2F1ToVa%2FTWbUJuFGz%2BmURrsn%2BzWCY2g%3D%3D"
    CMD_CURL= 'curl --include --request POST --header "Content-Type: application/json" --data-binary'

    DIRECTORY_TO_WATCH= "/Users/ana.paula.segatelli/teste/"
    DIRECTORY_TO_WATCH = sys.argv[1]
    URL_API=sys.argv[2]
    CMD_CURL=sys.argv[3]

    def __init__(self):
        self.observer = Observer()

    def run(self):
        event_handler = Handler()
        self.observer.schedule(event_handler, self.DIRECTORY_TO_WATCH, recursive=True)
        self.observer.start()

        print('inicio - ouvindo arquivos')

        try:
            # while True:
            for changes in watch(self.DIRECTORY_TO_WATCH):
                time.sleep(1)
                qtd_eventos = int(subprocess.check_output('ls -l ' + self.DIRECTORY_TO_WATCH + ' |  grep "^-" -c', shell=True))
                cmd_post_pb =  self.CMD_CURL + ' "' + "[{\\" + '"' + "qtd" + '\\"' + ":" + str(qtd_eventos) + "}]" + '" ' + ' "' + self.URL_API + '" '
                print(cmd_post_pb)
                # args_post = shlex.split(cmd_post_pb)
                subprocess.call(cmd_post_pb, shell=True)

        except:
            self.observer.stop()
            print("Error")

        self.observer.join()


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