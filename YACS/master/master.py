import threading
import socket
import logging
import json
import time
import queue

a = 0

logging.basicConfig(filename="newfile.log",
                    format='%(asctime)s %(message)s',
                    filemode='w',
                    level=logging.DEBUG
                    )
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()

lock = threading.Lock()


def listen_for_jobs():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('localhost', 5000))
    s.listen(5)
    while True:
        clientsocket, address = s.accept()
        print(f"Connection from {address} has been established.")
        msg = clientsocket.recv(1024)
        print(msg.decode("utf-8"))


listen_for_jobs()

def inc_by_10(lock):
    global a
    logging.info(a)
    for i in range(100000):
        with lock:
            a += 1


t1 = threading.Thread(target=inc_by_10, args=(lock,))
t2 = threading.Thread(target=inc_by_10, args=(lock,))

t1.start()
t2.start()

t1.join()
t2.join()

print(a)


