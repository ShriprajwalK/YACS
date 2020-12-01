import threading
import socket
import logging
import json
import sys
import time
from queue import Queue


class Worker:
    def __init__(self, worker_id, port):
        self.worker_id = worker_id
        self.port  = port
        self.execution_pool = []
        self.completed_queue = Queue()
        self.completed_queue_lock = threading.Lock()
        self.execution_pool_lock = threading.Lock()
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
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind(('localhost', port))
            s.listen(5)
            while True:
                master, address = s.accept()
                msg = master.recv(1024).decode("utf-8")
                with self.execution_pool_lock:
                    print('TASK RECEIVED', msg)
                    self.tasks_received += 1
                    self.execution_pool.append(json.loads(str(msg)))
                    print('pool', self.execution_pool)
                print('done listening')


    def execute_tasks(self):
        while True:
            to_remove = []
            print('executing')
            with self.execution_pool_lock:
                print('have execution locks')
                for i in self.execution_pool:
                    i['duration'] -= 1
                    if i['duration'] == 0:
                        self.tasks_completed += 1
                        i['status'] = 2
                        to_remove.append(i)
                        with self.completed_queue_lock:
                            self.completed_queue.put(i)
                    print('In execution')
                    print(i, type(i))
                for i in to_remove:
                    self.execution_pool.remove(i)
                self.tasks_running = len(self.execution_pool)
            print('gave up exeution lock, running tasks no=', self.tasks_running, 'completed tasks =', self.tasks_completed)
            time.sleep(1)


    def send_task_updates(self):

        ct = 0
        while True:
            with self.completed_queue_lock:
                if not self.completed_queue.empty():
                    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                        print('sending task updates to port 5001')
                        s.connect(('localhost', 5001))
                        msg = bytes(json.dumps(self.completed_queue.get()), 'utf-8')
                        self.tasks_updated_to_master += 1
                        print('MESSAGE TO MASTER', msg)
                        s.sendall(msg)
            ct += 1
            if ct%100000000000000 == 0:
                print('SENT TO MASTER', self.tasks_updated_to_master)


if __name__ == "__main__":
    port, worker_id = list(map(int, [sys.argv[1], sys.argv[2]]))
    worker = Worker(worker_id, port)
