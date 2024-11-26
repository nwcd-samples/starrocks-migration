import exporter
import importer
import os

def export():
    table_names = os.getenv("TABLE_NAMES")
    for tb in table_names:
        exporter.run(tb)

def import1():
    importer.run()

def run():
    
    thread1 = threading.Thread(target=export)
    thread2 = threading.Thread(target=import1)

    thread1.start()
    thread2.start()

    thread1.join()
    thread2.join()
