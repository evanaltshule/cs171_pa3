import socket
import sys
from datetime import datetime
import time
from _thread import *
import threading
import json
import os

CURRENT_LEADER = None

def do_exit(server_sock):
    server_sock.close()
    os._exit(0)

def handle_input(client_pid, server_sock):
    while True:
        try:
            inp = input()
            if inp == 'exit':
                do_exit(server_sock)
            if inp[0:4] == 'send':
                print(f"Sending message from client {client_pid} to server 1")
                sentence = inp[6:len(inp) - 1]
                server_sock.sendall(bytes(str(client_pid) + ": " + sentence, 'utf-8'))
        except EOFError:
            pass

def server_listen(sock):
    data = (sock.recv(1024))
    message = data.decode('utf-8')
    print(f"Received message from server {message}")


if __name__ == "__main__":
    pid = int(sys.argv[1]) 
    PORT = sys.argv[2]

    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    address = (socket.gethostname(), int(PORT))
    server_sock.connect(address)
    server_sock.send(b"client")
    print(f"Connected to leader")

    (threading.Thread(target=server_listen, args=(server_sock,))).start()
    handle_input(pid, server_sock)