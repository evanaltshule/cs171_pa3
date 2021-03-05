import socket
import sys
from datetime import datetime
import time
from _thread import *
import threading
import json
import os
import pickle

CURRENT_LEADER = None

# def do_exit():
#     server_sock.close()
#     os._exit(0)

def handle_input(client_pid, server_info):
    while True:
        try:
            inp = input()
            # if inp == 'exit':
            #     do_exit()
            if inp == 'connect':
                for pid, info in server_info.items():
                    port = info["port"]
                    (threading.Thread(target=server_connect, args=(int(pid),port))).start()
            if inp[0:6] == "leader":
                print("inputted leader")
                leader_id = int(inp[7])
                threading.Thread(target= init_leader, args = (leader_id,)).start()
        except EOFError:
            pass

def init_leader(leader_id):
    global SERVERS
    leader_sock = SERVERS[leader_id]
    message = {}
    message["type"] = "leader_request"
    encoded_message = pickle.dumps(message)
    print(f"Sending leader request to server {leader_id}")
    leader_sock.sendall(encoded_message)

def server_connect(server_pid,port):
    global SERVERS
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    address = (socket.gethostname(), int(port))
    server_sock.connect(address)
    server_sock.send(b"client")
    SERVERS[server_pid] = server_sock
    print(f"Connected to server {server_pid}")
    (threading.Thread(target=server_listen, args=(server_sock,))).start()

def server_listen(sock):
    data = (sock.recv(1024))
    message = pickle.loads(data)
    if message["type"] == "leader_broadcast":
        CURRENT_LEADER = message["leader"]
        print(f"Server {CURRENT_LEADER} elected leader")


if __name__ == "__main__":
    pid = int(sys.argv[1])

    with open('server_config.json') as conf:
        server_info = json.load(conf)
    SERVERS = {}

    handle_input(pid, server_info)

    #Change for client later

