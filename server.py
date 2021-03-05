import socket
import threading
import time
import sys
import os
import json
import hashlib
import collections
import pickle

"""
Queue data structure: deque
--Able to append and remove from both sides

key/value data structure: normal python dictionary
"""

#QUEUE = collections.deque()
#STUDENTS = dict()

LOCK = threading.Lock()

#Paxos globals
LEADER = None
CURR_BAL_NUM = (0,0,0)
#For ballot num
SEQ_NUM = 0
MY_PID = 0
DEPTH = 0
#For acceptor phase
NUM_PROMISES = 0
ENOUGH_PROMISES = False
#For promise phase
ACCEPTED_NUM = 0
ACCEPTED_VAL = None
ACCEPTED_PID = 0


ACCEPT_COUNT = 0

#Blockchain variables
BLOCKCHAIN = []


def do_exit(server_socket):
    sys.stdout.flush()
    server_socket.close()
    os._exit(0)


def handle_input(listen_for_servers, server_pids, client_pids):
    global MY_PID
    global OTHER_SERVERS
    while True:
        try:
            inp = input()
            if inp == 'exit':
                do_exit(listen_for_servers)
            #connect to all servers
            if inp == 'connect':
                for pid, port in server_pids.items():
                    if MY_PID != int(pid):
                        (threading.Thread(target=server_connect, args=(int(pid),port))).start()
                # for pid, port in client_pids.items():
                #     (threading.Thread(target=client_connect, args=(int(pid),port))).start()
            #Broadcast message to all servers
            if inp == 'leader':
                threading.Thread(target = leader_request, args = (server_pids,)).start()
        except EOFError:
            pass


def server_connect(server_pid,port):
    global OTHER_SERVERS
    server_to_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_to_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_to_server.connect((socket.gethostname(),port))
    server_to_server.sendall(b"server")
    OTHER_SERVERS[server_pid] = server_to_server
    print(f"Connected to Server{server_pid}")

def leader_request(server_pids):
    global MY_PID
    global OTHER_SERVERS
    global NUM_PROMISES
    global CURR_BAL_NUM

    increment_seq_num()
    ballot = get_ballot_num()
    CURR_BAL_NUM = ballot
    message = {}
    message["type"] = "prepare"
    message["ballot"] = ballot
    message["sender_pid"] = MY_PID
    encoded_message = pickle.dumps(message)
    time.sleep(4)
    for s_pid, conn in OTHER_SERVERS.items():
        print(f"Sending prepare from server {MY_PID} to server {s_pid}")
        conn.sendall(encoded_message)
    while(ENOUGH_PROMISES != True):
        pass
    leader_broadcast()


def increment_seq_num():
    global SEQ_NUM
    LOCK.acquire()
    SEQ_NUM += 1
    LOCK.release()

def get_ballot_num():
    global SEQ_NUM
    global MY_PID
    global DEPTH
    return (SEQ_NUM, MY_PID, DEPTH)

# def client_connect(client_pid,port):
#     global CLIENTS
#     server_to_client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#     server_to_client.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
#     server_to_client.connect((socket.gethostname(),port))
#     CLIENTS[client_pid] = server_to_client
#     print(f"Connected to Client{client_pid}")


# def server_listen(pid, data, server_sock):
#     print(f'Client {pid} listening on port {PORT}.')
#     while True:
#         sock, address = client_socket.accept()
#         sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
#         print(f'Client {pid} listen_for_serversected to {address}')
#         #start new thread to handle_event ?
#         threading.Thread(target=handle_event, args=(sock, address, pid, server_sock)).start()
#     client_socket.close()

def handle_server(stream):
    global NUM_PROMISES
    global LEADER
    global MY_PID
    global ENOUGH_PROMISES

    while(True):
        data = stream.recv(1024)
        if not data:
            break
        message = pickle.loads(data)
        #Handle prepare message
        increment_seq_num()
        if message["type"] == "prepare":
            sender = message["sender_pid"]
            print(f"Received prepare message from server {sender}")
            threading.Thread(target=handle_prepare, args = (message,)).start()
        #Handle promise message
        elif message["type"] == "promise":
            LOCK.acquire()
            NUM_PROMISES += 1
            if NUM_PROMISES >=2:
                ENOUGH_PROMISES = True
            LOCK.release()
        elif message["type"] == "leader_broadcast":
            LEADER = message["leader"]
            print(f"Assigned leader to {LEADER}")



def handle_prepare(message):
    global OTHER_SERVERS
    global DEPTH
    global ACCEPTED_NUM
    global ACCEPTED_PID

    sender_pid = int(message["sender_pid"])
    conn = OTHER_SERVERS[sender_pid]
    reply = {}

    proposer_ballot = message["ballot"]
    print(f"proposer ballot: {proposer_ballot}")
    #compare depth, seq_num, andpid of proposer to currently held values
    if(proposer_ballot[2] < DEPTH or proposer_ballot[0] < ACCEPTED_NUM  or (proposer_ballot[0] == ACCEPTED_NUM and proposer_ballot[1] < ACCEPTED_PID)):
        print(f"Proposal no good")
        reply["type"] = "nack"
        reply["ballot"] = proposer_ballot
        reply["error"] = "prepare rejected"
        reply_encoded = pickle.dumps(reply)
        conn.sendall(reply_encoded)
    #All clear to send promise
    else:
        CURR_BAL_NUM = proposer_ballot         
        reply["type"] = "promise"
        reply["promise_values"] = {"bal": CURR_BAL_NUM, "accepted_num": ACCEPTED_NUM, "accepted_val": ACCEPTED_VAL, "depth": DEPTH}
        reply_encoded = pickle.dumps(reply)
        print(f"Sending promise back to leader")
        conn.sendall(reply_encoded)

def leader_broadcast():
    global MY_PID 

    message = {}
    message["type"] = "leader_broadcast"
    message["leader"] = MY_PID
    encoded_message = pickle.dumps(message)
    increment_seq_num()
    print(f"I am the leader!")
    time.sleep(4)
    for s_pid, conn in OTHER_SERVERS.items():
        conn.sendall(encoded_message)

def handle_client(stream):
    while(True):
        data = stream.recv(1024)
        if not data:
            break
        message = data.decode('utf-8')

        print(f"Received message from client {message}")



if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(f'Usage: python {sys.argv[0]} <serverID> <serverPort>')
        sys.exit()

    #Set up listening socket for current server
    MY_PID = int(sys.argv[1])
    PORT = sys.argv[2]
    listen_for_servers = socket.socket()
    listen_for_servers.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listen_for_servers.bind((socket.gethostname(), int(PORT)))
    listen_for_servers.listen(32)
    print(f'Server listening on port {PORT}.')
    #Holds server id and connection to that server
    #Will map sever id to list with port and whether it is an active connection or not
    OTHER_SERVERS = {}
    #Holds client id and connection to that client
    CLIENTS = {}
    # output_file = open(sys.argv[1], 'w')

    #Load server json file with port nums
    with open('server_config.json') as conf:
        server_pids = json.load(conf)

    #Load client json file with port nums
    with open('client_config.json') as conf:
        client_pids = json.load(conf)

    #Start to listen for other servers trying to listen_for_serversect
    #threading.Thread(target=server_listen, args=(pid, data, server_sock)).start()

    threading.Thread(target=handle_input, args=(
        listen_for_servers, server_pids, client_pids)).start()

    while True:
        try:
            stream, address = listen_for_servers.accept()
            data = stream.recv(1024)
            if not data:
                break
            message = data.decode('utf-8')
            if message == "server":
                threading.Thread(target=handle_server, args=(stream,)).start()
            elif message == "client":
                threading.Thread(target=handle_client, args=(stream,)).start()
        except KeyboardInterrupt:
            do_exit(output_file, listen_for_servers)

