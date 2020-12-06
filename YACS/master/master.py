"""
This file contains the source code for the master of the YACS scheduler.
It takes the config file and the scheduling algirthm(RR, RD, LL) as arguments.
It then listens to job requests, schedules tasks and listens for worker updates.
"""
import threading
import socket
import sys
import logging
import random
import json
import time
from queue import Queue

#Initialising the logger with required format
logging.basicConfig(filename=f"log_master_{sys.argv[2]}.log",
                    format='%(asctime)s %(message)s',
                    filemode='w',
                    level=logging.DEBUG
                    )
logging.basicConfig(level=logging.DEBUG)

logger = logging.getLogger()

class Scheduler():
    """
    This is class meant to be the master of YACS. It takes in the config and policy
    and initialises the data structes the the scheduler uses. It also launches 3 threads,
    one each for listening for job requests, listening to worker updates and scheduling tasks
    """

    def __init__(self, config, policy):
        self.config = config
        self.policy = policy
        self.number_of_workers = len(config)

        self.map_tasks_lock = threading.Lock()
        self.reduce_tasks_lock = threading.Lock()
        self.slots_lock = threading.Lock()
        self.slots = {}
        self.map_tasks = {}
        self.reduce_tasks ={}
        # This queue is the crux of the scheduler. All tasks that are ready to
        # be run are added to this queue
        self.ready_queue = Queue()

        for i in config:
            self.slots[i['worker_id']] = {'port': i['port'], 'slots': i['slots']}

        t1 = threading.Thread(target=self.listen_for_jobs)
        t2 = threading.Thread(target=self.listen_for_worker_updates)
        t3 = threading.Thread(target=self.schedule)
        t1.start()
        t2.start()
        t3.start()
        t1.join()
        t2.join()
        t3.join()


    def parse_request(self, request):
        "This function parses incoming requests and adds them to the appropriate data structures."
        job = json.loads(request)
        with self.map_tasks_lock:
            for i in job['map_tasks']:
                i['status'] = 0
            self.map_tasks[job['job_id']] = job['map_tasks']
        with self.reduce_tasks_lock:
            for i in job['reduce_tasks']:
                i['status'] = 0
            self.reduce_tasks[job['job_id']] = job['reduce_tasks']


    def listen_for_jobs(self):
        "This function is meant to listen for job requests"
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind(('localhost', 5000))
            s.listen(5)
            while True:
                request_socket, address = s.accept()
                msg = request_socket.recv(1024).decode("utf-8")
                logging.info(f"%MASTER RECEIVED JOB%{msg}")
                self.parse_request(msg)



    def allocate_tasks(self):
        """
        This function checks what policy the scheduler is using and sends task
        launch messages to the appropriate worker
        """
        with self.slots_lock:
            if self.policy == 'RR':
                for i in self.slots:
                    if not self.ready_queue.empty():
                            if self.slots[i]['slots'] != 0:
                                self.slots[i]['slots'] -= 1
                                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                                    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                                    s.connect(('localhost', self.slots[i]['port']))
                                    msg = json.dumps(self.ready_queue.get())
                                    logging.info(f"%SENDING WORKER WITH ID {i} and slots {self.slots[i]['slots']} the task%{msg}")
                                    s.sendall(bytes(msg, 'utf-8'))

            elif self.policy == 'RD':
                k = random.choice(list(self.slots.keys()))
                if not self.ready_queue.empty():
                    if self.slots[k]['slots'] != 0:
                        self.slots[k]['slots'] -= 1
                        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                            s.connect(('localhost', self.slots[k]['port']))
                            msg = json.dumps(self.ready_queue.get())
                            logging.info(f"%SENDING WORKER WITH ID {k} and slots {self.slots[k]['slots']} the task%{msg}")
                            s.sendall(bytes(msg, 'utf-8'))

            else:
                max_slots_free = 0
                worker = 0
                for i in self.slots:
                    if self.slots[i]['slots'] > max_slots_free:
                        max_slots_free = self.slots[i]['slots']
                        worker = i
                if max_slots_free > 0:
                    if self.slots[worker]['slots'] != 0:
                        self.slots[worker]['slots'] -= 1
                        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                            s.connect(('localhost', self.slots[worker]['port']))
                            msg = json.dumps(self.ready_queue.get())
                            logging.info(f"%SENDING WORKER WITH ID {worker} and slots {self.slots[worker]['slots']} the task%{msg}")
                            s.sendall(bytes(msg, 'utf-8'))


    def schedule(self):
        """
        This function adds the appropriate tasks to the ready queue.
        This function also takes care of the map reduce dependencies between
        tasks of a job before adding them to the ready queue.
        """
        done_map_jobs = []
        while True:
            reducers_to_be_run = []
            with self.map_tasks_lock:
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

            with self.reduce_tasks_lock:
                if reducers_to_be_run != []:
                    for i in reducers_to_be_run:
                        for reduce_task in self.reduce_tasks[i]:
                            self.ready_queue.put(reduce_task)

            empty_slots = 0
            with self.slots_lock:
                for i in self.slots:
                    empty_slots += self.slots[i]['slots']

            if not self.ready_queue.empty() and empty_slots != 0:
                self.allocate_tasks()

            if self.ready_queue.empty():
                time.sleep(0.1)

            if empty_slots == 0:
                time.sleep(1)


    def listen_for_worker_updates(self):
        """
        This function listens for task updates from workers
        """
        done_jobs = []
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind(('localhost', 5001))
            s.listen(5)
            while True:
                worker_socket, address = s.accept()
                msg = json.loads(worker_socket.recv(1024).decode("utf-8"))
                logging.info(f'%WORKER SENT%{msg}')

                job_id = msg['task_id'].split('_')[0]

                with self.map_tasks_lock:
                    for map_task in self.map_tasks[job_id]:
                        if map_task['task_id'] == msg['task_id']:
                            map_task['status'] = 2
                with self.reduce_tasks_lock:
                    ct = 0
                    for reduce_task  in self.reduce_tasks[job_id]:
                        if reduce_task['task_id'] == msg['task_id']:
                            reduce_task['status'] = 2

                        if reduce_task['status'] == 2:
                            ct += 1
                        if ct == len(self.reduce_tasks[job_id]):
                            logging.info(f'%JOB FINISHED WITH ID:%{job_id}% and reduce tasks {ct}')
                            done_jobs.append(job_id)
                            if len(done_jobs) == 20:
                                print('done')

                with self.slots_lock:
                    self.slots[msg['worker_id']]['slots'] += 1


def main():
    with open(sys.argv[1]) as f:
        config = json.load(f)
    sched = Scheduler(config['workers'], sys.argv[2])

if __name__ == "__main__":
    main()

