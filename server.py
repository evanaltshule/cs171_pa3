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

QUEUE = collections.deque()
STUDENTS = dict()

LOCK = threading.Lock()
MY_QUORUM = {}
#Paxos globals
LEADER = None
CURR_BAL_NUM = (0,0,0)
NACKED = False
#For ballot num
SEQ_NUM = 0
MY_PID = 0
DEPTH = 0
#For acceptor phase
NUM_PROMISES = 0
ENOUGH_PROMISES = False
PROMISED_VALS =[]
#For promise phase
ACCEPTED_NUM = 0
ACCEPTED_VAL = None
ACCEPTED_PID = 0
#For accept phase
NUM_ACCEPTED = 0
ENOUGH_ACCEPTED = False


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
                quorum = server_pids[str(MY_PID)]["quorum"]
                for pid, info in server_pids.items():
                    if MY_PID != int(pid):
                        port = info["port"]
                        (threading.Thread(target=server_connect, args=(int(pid),port,quorum))).start()
                # for pid, port in client_pids.items():
                #     (threading.Thread(target=client_connect, args=(int(pid),port))).start()
            #Broadcast message to all servers
            #if inp == 'leader':
            #    threading.Thread(target = leader_request, args = ()).start()
        except EOFError:
            pass


def server_connect(server_pid,port,quorum):
    global OTHER_SERVERS
    global MY_QUORUM
    server_to_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_to_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_to_server.connect((socket.gethostname(),port))
    server_to_server.sendall(b"server")
    OTHER_SERVERS[server_pid] = server_to_server
    if (server_pid in quorum):
        MY_QUORUM[server_pid] = server_to_server
    print(f"Connected to Server{server_pid}")

def leader_accept():
    global MY_PID
    global LEADER
    global QUEUE
    global ENOUGH_ACCEPTED

    while(LEADER != MY_PID or QUEUE[0] == None):
        pass

    message = {}
    message["type"] = "accept"
    message["ballot"] = CURR_BAL_NUM
    #make block by finding nonce
    #find_nonce()
    message["value"] = QUEUE[0]
    encoded_message = pickle.dumps(message)
    for s_pid, conn in OTHER_SERVERS.items():
        print(f"Sending accept request from server {MY_PID} to server {s_pid}")
        conn.sendall(encoded_message)

    while(ENOUGH_ACCEPTED != True):
        pass

    decide(QUEUE[0])

def decide(block):
    #Add block to blockchain
    pass
    #Notify servers and clients about the block


def leader_request():
    global MY_PID
    global OTHER_SERVERS
    global NUM_PROMISES
    global CURR_BAL_NUM
    global NACKED

    increment_seq_num()
    ballot = get_ballot_num()
    CURR_BAL_NUM = ballot
    message = {}
    message["type"] = "prepare"
    message["ballot"] = ballot
    message["sender_pid"] = MY_PID
    encoded_message = pickle.dumps(message)
    time.sleep(4)
    for s_pid, conn in MY_QUORUM.items():
        print(f"Sending prepare from server {MY_PID} to server {s_pid}")
        conn.sendall(encoded_message)
    while(ENOUGH_PROMISES != True):
        #CHECK IF YOU GET A NACK, try again after some time 
        if(NACKED == True):
            #retry prepare: set promises to zero, set NACKED to False, sleep 5 seconds, call leader_request
            print("I got nacked")
            threading.Thread(target = retry_prepare, args=()).start()
            break
        pass
    if(NACKED == False):
        leader_broadcast()

def leader_broadcast():
    global MY_PID 
    global CLIENTS
    global LEADER

    message = {}
    message["type"] = "leader_broadcast"
    message["leader"] = MY_PID
    encoded_message = pickle.dumps(message)
    increment_seq_num()
    print(f"I am the leader!")
    time.sleep(4)
    for s_pid, conn in OTHER_SERVERS.items():
        conn.sendall(encoded_message)
    client_stream = CLIENTS[1]
    client_stream.sendall(encoded_message)
    LEADER = MY_PID
    NUM_PROMISES = 0

def retry_prepare():
    global NUM_PROMISES
    global NACKED

    NUM_PROMISES = 0
    time.sleep(5)
    NACKED = False
    leader_request()

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

def handle_server(stream):
    global NUM_PROMISES
    global LEADER
    global MY_PID
    global ENOUGH_PROMISES
    global NACKED

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
            PROMISED_VALS.append(message["promise_values"])
            if NUM_PROMISES >=2:
                #Check to see if accepted vals sent are null
                highest_accepted_num = 0
                new_val = None
                for prom_val in PROMISED_VALS:
                    if prom_val["accepted_val"]:
                        if prom_val["accepted_num"] > highest_accepted_num:
                            new_val = prom_val["accepted_val"]
                if new_val:
                    QUEUE.appendleft(new_val)
                ENOUGH_PROMISES = True
            LOCK.release()
        elif message["type"] == "leader_broadcast":
            LEADER = message["leader"]
            print(f"Assigned leader to {LEADER}")
        elif message["type"] == "nack":
            NACKED = True



def handle_prepare(message):
    global OTHER_SERVERS
    global DEPTH
    global ACCEPTED_NUM
    global ACCEPTED_PID
    global CURR_BAL_NUM

    sender_pid = int(message["sender_pid"])
    conn = OTHER_SERVERS[sender_pid]
    reply = {}

    proposer_ballot = message["ballot"]
    print(f"proposer ballot: {proposer_ballot}")
    #compare depth, seq_num, andpid of proposer to currently held values
    if(proposer_ballot[2] < DEPTH or proposer_ballot[0] < CURR_BAL_NUM[0]  or (proposer_ballot[0] == CURR_BAL_NUM[0] and proposer_ballot[1] < CURR_BAL_NUM[1])):
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


def handle_client(stream):
    while(True):
        data = stream.recv(1024)
        if not data:
            break
        message = pickle.loads(data)
        if message["type"] == "leader_request":
            threading.Thread(target = leader_request, args = ()).start()
        elif message["type"] == "put" or message["type"] == "get":
            LOCK.acquire()
            QUEUE.append(message)
            LOCK.release()



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

    threading.Thread(target = leader_accept).start()

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
                print("Connected to client")
                CLIENTS[1] = stream
                threading.Thread(target=handle_client, args=(stream,)).start()
        except KeyboardInterrupt:
            do_exit(output_file, listen_for_servers)

