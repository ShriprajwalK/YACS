import threading
import socket
import logging
import json
import sys

def listen_for_task_launch(port, worker_id):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(('localhost', port))
        s.listen(5)
        while True:
            master, address = s.accept()
            msg = master.recv(1024).decode("utf-8")
            print(msg)

def send_task_updates():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect(('localhost', 5000))
        s.sendall(b'Hello, world')
        data = s.recv(1024)
        print('Received', repr(data))

def main():
    port, worker_id = list(map(int, [sys.argv[1], sys.argv[2]]))
    listen_for_task_launch(port, worker_id)
    t1 = threading.Thread(target=send_task_updates)
    t2 = threading.Thread(target=listen_for_task_launch, args=(port, worker_id))


if __name__ == "__main__":
    main()
