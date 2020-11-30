import threading
import socket
import logging
import json
import sys


class Worker:
    def __init__(self, worker_id, port):
        self.worker_id = worker_id
        self.port  = port
        self.execution_pool = []
        self.completed = []
        self.execution_pool_lock = threading.Lock()
        t1 = threading.Thread(target=self.send_task_updates)
        t2 = threading.Thread(target=self.listen_for_task_launch, args=(self.port, self.worker_id))
        t1.start()
        t2.start()
        t1.join()
        t2.join()

    def listen_for_task_launch(self, port, worker_id):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind(('localhost', port))
            s.listen(5)
            while True:
                master, address = s.accept()
                msg = master.recv(1024).decode("utf-8")
                print(msg)

    def send_task_updates(self):
        if self.completed != []:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect(('localhost', 5000))
                s.sendall(b'Hello, world')
                data = s.recv(1024)
                print('Received', repr(data))


if __name__ == "__main__":
    port, worker_id = list(map(int, [sys.argv[1], sys.argv[2]]))
    worker = Worker(worker_id, port)
