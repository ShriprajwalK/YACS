import threading
import socket
import logging
import json
import time
import queue

logging.basicConfig(filename="newfile.log",
                    format='%(asctime)s %(message)s',
                    filemode='w',
                    level=logging.DEBUG
                    )
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()

lock = threading.Lock()
map_tasks = []
reduce_tasks = []
dependency = []

def parse_request(request):
    job = json.loads(request)
    print()
    print(f"JOB_ID: {job['job_id']}")
    print()

def schedule(msg):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect(('localhost', 4001))
        print(f"SENDING WORKER the job {msg}")
        s.sendall(bytes(msg, 'utf-8'))

def listen_for_worker_updates(lock):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(('localhost', 5001))
        s.listen(5)
        while True:
            worker_socket, address = s.accept()
            print(f"Connection from {address} to port 5001(worker)  has been established.")
            msg = worker_socket.recv(1024).decode("utf-8")
            print(msg)


def listen_for_jobs(lock):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(('localhost', 5000))
        s.listen(5)
        while True:
            request_socket, address = s.accept()
            print(f"Connection from {address} to 5000(requests) has been established.")
            msg = request_socket.recv(1024).decode("utf-8")
            print(f"MASTER RECEIVED JOB:{msg}")
            parse_request(msg)
            schedule(msg)


def main():
    t1 = threading.Thread(target=listen_for_jobs, args=(lock,))
    t2 = threading.Thread(target=listen_for_worker_updates, args=(lock,))

    t1.start()
    t2.start()

    t1.join()
    t2.join()

if __name__ == "__main__":
    main()

