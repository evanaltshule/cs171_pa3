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

#Paxos globals
SEQ_NUM = 0
PROC_ID = 0
DEPTH = 0
#make function that returns depth (length) of blockchain
#leader = server_id
#accept_count = 0

#Blockchain variables
#BLOCKCHAIN = []


def do_exit(server_socket):
    sys.stdout.flush()
    server_socket.close()
    os._exit(0)


def handle_input(listen_for_servers, server_pids, client_pids):
    global OUR_PID
    global OTHER_SERVERS
    while True:
        try:
            inp = input()
            if inp == 'exit':
                do_exit(listen_for_servers)
            #connect to all servers
            if inp == 'connect':
                for pid, port in server_pids.items():
                    if int(OUR_PID) != int(pid):
                        (threading.Thread(target=server_connect, args=(int(pid),port))).start()
                # for pid, port in client_pids.items():
                #     (threading.Thread(target=client_connect, args=(int(pid),port))).start()
            #Broadcast message to all servers
            if inp == 'leader':
                threading.Thread(target = leader_request, args = (server_pids,)).start()
            if inp[0:4] == 'send':
                sentence = inp[6:len(inp) - 1]
                for s_pid, conn in OTHER_SERVERS.items():
                    print(f"Sending message from server {OUR_PID} to server {s_pid}")
                    conn.sendall(bytes(OUR_PID + ": " + sentence, 'utf-8'))
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
    global OUR_PID
    global OTHER_SERVERS
    ballot = get_ballot_num()
    message = {}
    message["type"] = "prepare"
    message["ballot"] = get_ballot_num()
    encoded_message = pickle.dumps(message).encode('utf-8')
    for s_pid, conn in OTHER_SERVERS.items():
        printf("Sending prepare from server {OUR_PID} to server {s_pid}")
        conn.sendall(encoded_message)

def get_ballot_num():
    global SEQ_NUM
    global PROC_ID
    global DEPTH
    return (SEQ_NUM, PROC_ID, DEPTH)

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
    while(True):
        data = stream.recv(1024)
        if not data:
            break
        message = pickle.loads(data.decode('utf-8'))
        print(f"Received message") #from server {message}")
        print(message)

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
    OUR_PID = sys.argv[1]
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

