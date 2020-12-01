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
        print("given up lock 1")
        with self.reduce_tasks_lock:
            for i in job['reduce_tasks']:
                i['status'] = 0
            self.reduce_tasks[job['job_id']] = job['reduce_tasks']


    def listen_for_jobs(self):
        print('listening')
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind(('localhost', 5000))
            s.listen(5)
            while True:
                print('listening for request')
                request_socket, address = s.accept()
                print(f"Connection from {address} to 5000(requests) has been established.")
                msg = request_socket.recv(1024).decode("utf-8")
                print(f"MASTER RECEIVED JOB:{msg}")
                self.parse_request(msg)



    def allocate_tasks(self):
        print('allocating')
        print('not empty', self.ready_queue.queue)
        for i in self.slots:
            if not self.ready_queue.empty():
                    if self.slots[i]['slots'] != 0:
                        #self.slots[i]['slots'] -= 1
                        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                            print('using sockets')
                            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                            s.connect(('localhost', 4001))
                            #print(f"SENDING WORKER the job {msg}")
                            s.sendall(bytes(json.dumps(self.ready_queue.get()), 'utf-8'))
                            print('end of socket use')
            print('end of allocation')

    def schedule(self):
        print("scheduling")
        done_map_jobs = []
        while True:
            print('done_map_jobs', done_map_jobs)
            print('iterating')
            reducers_to_be_run = []
            time.sleep(2)
            with self.map_tasks_lock:
                print('got map_task lock')

                for i in self.map_tasks:
                    for map_task in self.map_tasks[i]:
                        if map_task['status'] == 0:
                            map_task['status'] = 1
                            self.ready_queue.put(map_task)
                            break

                    ct = 0
                    for map_task in self.map_tasks[i]:
                        if map_task['status'] == 2:
                            ct += 1
                        if ct == len(self.map_tasks[i]) and  i not in done_map_jobs:
                            done_map_jobs.append(i)
                            reducers_to_be_run.append(i)

                print('map tasks', self.map_tasks)

            with self.reduce_tasks_lock:
                print('got reduce lock')
                for i in reducers_to_be_run:
                    for reduce_task in self.reduce_tasks[i]:
                        print('REDUCE TASK TO RUN', reduce_task)
                        self.ready_queue.put(reduce_task)
                print('reduce tasks', self.reduce_tasks)

            if not self.ready_queue.empty():
                self.allocate_tasks()

            print('scheduler gave up on locks')


    def listen_for_worker_updates(self):
        print('worker')
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind(('localhost', 5001))
            s.listen(5)
            while True:
                worker_socket, address = s.accept()
                #print(f"Connection from {address} to port 5001(worker)  has been established.")
                msg = json.loads(worker_socket.recv(1024).decode("utf-8"))
                print('WORKER SENT', msg)
                with self.map_tasks_lock:
                    print(msg['task_id'])
                    for i in  self.map_tasks[msg['task_id'][0]]:
                        if i['task_id'] == msg['task_id']:
                            print(i['task_id'], msg['task_id'])
                            i['status'] = 2
                with self.reduce_tasks_lock:
                    for i in self.reduce_tasks[msg['task_id'][0]]:
                        if i['task_id'] == msg['task_id']:
                            i['status'] = 2
                    print('REDUCE TASKS',self.reduce_tasks)




def main():
    with open(sys.argv[1]) as f:
        config = json.load(f)
    print(config['workers'])
    sched = Scheduler(config['workers'])

if __name__ == "__main__":
    main()

