import threading
import socket
import sys
import logging
import json
import time
from queue import Queue

logging.basicConfig(filename="newfile.log",
                    format='%(asctime)s %(message)s',
                    filemode='w',
                    level=logging.DEBUG
                    )
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()

lock = threading.Lock()

class Scheduler():
    def __init__(self, config):
        self.config = config
        self.number_of_workers = len(config)

        self.map_tasks_lock = threading.Lock()
        self.reduce_tasks_lock = threading.Lock()
        self.slots_lock = threading.Lock()
        self.slots = {}
        self.map_tasks = {}
        self.reduce_tasks ={}
        self.ready_queue = Queue()
        self.running_queue = Queue()

        for i in config:
            self.slots[i['worker_id']] = {'port': i['port'], 'slots': i['slots']}

        print(self.slots)
        t1 = threading.Thread(target=self.listen_for_jobs)
        t2 = threading.Thread(target=self.listen_for_worker_updates)
        t3 = threading.Thread(target=self.schedule)
        t1.start()
        print('started t1')
        t2.start()
        print('started t2')
        t3.start()
        print('started t3')
        t1.join()
        t2.join()
        t3.join()


    def parse_request(self, request):
        job = json.loads(request)
        with self.map_tasks_lock:
            for i in job['map_tasks']:
                i['status'] = 0
            self.map_tasks[job['job_id']] = job['map_tasks']
        #print("given up lock 1")
        with self.reduce_tasks_lock:
            for i in job['reduce_tasks']:
                i['status'] = 0
            self.reduce_tasks[job['job_id']] = job['reduce_tasks']
        #print("given up lock 2")
        #print('tasks right now:')
        #print(map_tasks)
        #print(reduce_tasks)

    def allocate_tasks(self):
        print('allocating')
        if not self.ready_queue.empty():
            print('not empty')
            with self.slots_lock:
                for i in self.slots:
                    if self.slots[i]['slots'] != 0:
                        self.slots[i]['slots'] -= 1
                        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                            s.connect(('localhost', 4001))
                            #print(f"SENDING WORKER the job {msg}")
                            s.sendall(bytes(str(self.ready_queue.get()), 'utf-8'))


    def schedule(self):
        print("scheduling")
        while True:
            reducers_to_be_run = []
            time.sleep(2)
            with self.map_tasks_lock:
                for i in self.map_tasks:
                    print()
                    print("NYAHAHAHAHAHHAHAHAHAHAHAHAH")
                    print(i)
                    for map_task in self.map_tasks[i]:
                        print('map task', map_task)
                        if map_task['status'] == 0:
                            map_task['status'] = 1
                            self.ready_queue.put(map_task)
                            break
                print(self.map_tasks)
            print(f'ready_queue {self.ready_queue.queue}')
            with self.reduce_tasks_lock:
                pass
                #print(self.reduce_tasks)
            self.allocate_tasks()



    def listen_for_jobs(self):
        print('listeing')
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind(('localhost', 5000))
            s.listen(5)
            while True:
                request_socket, address = s.accept()
                #print(f"Connection from {address} to 5000(requests) has been established.")
                msg = request_socket.recv(1024).decode("utf-8")
                #print(f"MASTER RECEIVED JOB:{msg}")
                self.parse_request(msg)


    def listen_for_worker_updates(self):
        print('worker')
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind(('localhost', 5001))
            s.listen(5)
            while True:
                worker_socket, address = s.accept()
                #print(f"Connection from {address} to port 5001(worker)  has been established.")
                msg = worker_socket.recv(1024).decode("utf-8")
                #print(msg)



def main():
    with open(sys.argv[1]) as f:
        config = json.load(f)
    print(config['workers'])
    sched = Scheduler(config['workers'])

if __name__ == "__main__":
    main()

