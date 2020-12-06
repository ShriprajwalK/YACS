"""
This file contains the source code for the worker of the YACS scheduler.
It takes the port and the worker id as arguments.
It then listens to task launch requests, executes tasks and sends task updates
to the master.
"""

import threading
import socket
import logging
import json
import sys
import time
from queue import Queue


#Initialising logging with the required format.
logging.basicConfig(filename=f"log_worker_{sys.argv[2]}_RR.log",
                    format='%(asctime)s %(message)s',
                    filemode='w',
                    level=logging.DEBUG
                    )
logging.basicConfig(level=logging.DEBUG)


class Worker:
    """
    This class is the worker of the YACS scheduler. It takes the worker id and
    the port it opeates on as input and launches 3 threads: one for listening
    to task launches, one for executing tasks, and one more to send task updates
    to master.
    """
    def __init__(self, worker_id, port):
        self.worker_id = worker_id
        self.port  = port
        self.logger = logging.getLogger()

        self.execution_pool = []
        self.arrival_list = []
        self.completed_queue = Queue()
        self.arrival_list_lock = threading.Lock()
        self.completed_queue_lock = threading.Lock()
        self.tasks_received = 0
        self.tasks_completed = 0
        self.tasks_running = 0
        self.tasks_updated_to_master = 0

        t1 = threading.Thread(target=self.send_task_updates)
        t2 = threading.Thread(target=self.listen_for_task_launch, args=(self.port, self.worker_id))
        t3 = threading.Thread(target=self.execute_tasks)
        t1.start()
        t2.start()
        t3.start()
        t1.join()
        t2.join()
        t3.join()


    def listen_for_task_launch(self, port, worker_id):
        "This function listens for task launches form the master"
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind(('localhost', port))
            s.listen(5)
            while True:
                master, address = s.accept()
                msg = master.recv(1024).decode("utf-8")
                with self.arrival_list_lock:
                    logging.info(f'%TASK RECEIVED%{msg}')
                    self.tasks_received += 1
                    self.arrival_list.append(json.loads(str(msg)))


    def execute_tasks(self):
        "This function deducts one from the duration of all tasks in the execution pool"
        while True:
            to_remove = []
            with self.arrival_list_lock:
                self.execution_pool.extend(self.arrival_list)
                self.arrival_list = []

            time.sleep(1)
            for i in self.execution_pool:
                i['duration'] -= 1
                if i['duration'] == 0:
                    self.tasks_completed += 1
                    i['status'] = 2
                    logging.info(f'%TASK COMPLETED%{i}')
                    to_remove.append(i)
                    with self.completed_queue_lock:
                        self.completed_queue.put(i)
            for i in to_remove:
                self.execution_pool.remove(i)
            self.tasks_running = len(self.execution_pool)
            logging.info(f'%number of tasks running=% {self.tasks_running} %tasks completed=%{self.tasks_completed}')


    def send_task_updates(self):
        ct = 0
        while True:
            with self.completed_queue_lock:
                if not self.completed_queue.empty():
                    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                        s.connect(('localhost', 5001))
                        to_send = self.completed_queue.get()
                        to_send['worker_id'] = self.worker_id
                        msg = bytes(json.dumps(to_send), 'utf-8')
                        self.tasks_updated_to_master += 1
                        logging.info(f'%MESSAGE TO MASTER {msg}')
                        s.sendall(msg)
            ct += 1
            if ct%10000000000 == 0:
                logging.info(f'%tasks SENT TO MASTER%{self.tasks_updated_to_master}')


if __name__ == "__main__":
    port, worker_id = list(map(int, [sys.argv[1], sys.argv[2]]))
    worker = Worker(worker_id, port)
