import socket
import sys
from datetime import datetime
import time
from _thread import *
import threading
import json
import os
import pickle
import collections

CURRENT_LEADER = None
WAITING = False
OP_DEQ = collections.deque()

# def do_exit():
#     server_sock.close()
#     os._exit(0)

def do_exit():
    global SERVERS
    sys.stdout.flush()
    for conn in servers:
        conn.close()
    os._exit(0)

def handle_input(client_pid, server_info):
    while True:
        try:
            inp = input()
            inp = inp.split()
            # if inp == 'exit':
            #     do_exit()
            if inp[0] == 'c':
                for pid, info in server_info.items():
                    port = info["port"]
                    (threading.Thread(target=server_connect, args=(int(pid),port))).start()
            elif inp[0] == 'e':
                do_exit()
            if inp[0] == "leader":
                leader_id = int(inp[1])
                threading.Thread(target = init_leader, args = (leader_id,)).start()
            if inp[0] == "put":
                threading.Thread(target = send_put, args = (inp,)).start()
            if inp[0] == "get":
                threading.Thread(target = send_get, args = (inp,)).start()

        except EOFError:
            pass

def init_leader(leader_id):
    global SERVERS
    global MY_PID
    leader_sock = SERVERS[leader_id]
    message = {}
    message["type"] = "leader_request"
    message["client_id"] = MY_PID
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

def send_put(inp):
    global OP_DEQ

    message = {}
    message["type"] = "put"
    message["id"] = inp[1]
    message["value"] = inp[2]
    message["client_id"] = MY_PID

    OP_DEQ.append(message)

    while(OP_DEQ[0] != message):
        pass
    encoded_message = pickle.dumps(message)
    print(f"Sending put operation to server {CURRENT_LEADER}")
    conn = SERVERS[CURRENT_LEADER]
    conn.sendall(encoded_message)
    WAITING = True
    # time = datetime.now()
    #Wait to hear back for server
    while(WAITING == True):
        # if(datetime.now() - time > TIMEOUT):
        #     new_leader()
        pass
    OP_DEQ.popleft()

def send_get(inp):
    global OP_DEQ

    message = {}
    message["type"] = "get"
    message["id"] = inp[1]
    message["value"] = ""
    message["client_id"] = MY_PID

    OP_DEQ.append(message)

    while(OP_DEQ[0] != message):
        pass
    encoded_message = pickle.dumps(message)
    print(f"Sending get operation to server {CURRENT_LEADER}")
    conn = SERVERS[CURRENT_LEADER]
    conn.sendall(encoded_message)
    WAITING = True
    # time = datetime.now()
    #Wait to hear back for server
    while(WAITING == True):
        # if(datetime.now() - time > TIMEOUT):
        #     new_leader()
        pass
    OP_DEQ.popleft()



def server_listen(sock):
    global CURRENT_LEADER
    while True:
        data = (sock.recv(1024))
        message = pickle.loads(data)
        if message["type"] == "leader_broadcast":
            CURRENT_LEADER = message["leader"]
            print(f"Server {CURRENT_LEADER} elected leader")
        if message["type"] == "put_ack":
            print(f"Got acknowledgment from server {CURRENT_LEADER}") 
            WAITING = False
        if message["type"] == "get_ack":
            value = message["value"]
            print(f"Got get acknowledgment from server {CURRENT_LEADER} for value {value}")
            WAITING = False
        if message["type"] == "server_response":
            print(message["response"])

if __name__ == "__main__":
    MY_PID = int(sys.argv[1])

    with open('server_config.json') as conf:
        server_info = json.load(conf)
    SERVERS = {}

    handle_input(MY_PID, server_info)

    #Change for client later
