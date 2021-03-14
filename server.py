import socket
import threading
import time
import sys
import os
import json
import hashlib
import collections
import pickle
import random
import string
import hashlib

"""
Queue data structure: deque
--Able to append and remove from both sides

key/value data structure: normal python dictionary
"""

#TO-DOs
#1. Implement server disconnects
#2. fix blockchains if depths are uneven

QUEUE = collections.deque()
STUDENTS = dict()

LOCK = threading.Lock()
MY_QUORUM = {}
#Paxos globals
LEADER = None
CURR_BAL_NUM = (0,0,0)
PREPARE_NACKED = False
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
    global OTHER_SERVERS
    global CLIENTS
    sys.stdout.flush()
    for sid, sock in OTHER_SERVERS.items():
        sock.close()

    for cid, sock in CLIENTS.items():
        sock.close()
    os._exit(0)


def handle_input(listen_for_servers, server_pids):
    global MY_PID
    global OTHER_SERVERS

    while True:
        try:
            inp = input()
            inp = inp.split()
            if inp[0] == 'e':
                do_exit(listen_for_servers)
            #connect to all servers
            elif inp[0] == 'c':
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
            elif inp[0] == "printblockchain":
                threading.Thread(target=print_blockchain).start()
            elif inp[0] == "printkv":
                threading.Thread(target=print_kv).start()
            elif inp[0] == "printqueue":
                threading.Thread(target=print_queue).start()

        except EOFError:
            pass

def print_blockchain():
    global BLOCKCHAIN
    for block_num in range(len(BLOCKCHAIN)):
        print(BLOCKCHAIN[block_num])

def print_kv():
    global STUDENTS
    for key, value in STUDENTS.items():
        print(f"{key}: {value}")

def print_queue():
    global QUEUE
    print(QUEUE)


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

def generate_nonce():
    nonce = ''.join(random.choice(string.ascii_letters) for i in range(10))
    return nonce

def create_block(op_info):
    global DEPTH
    global init_hash
    nonce = generate_nonce()

    hashable = op_info["op"] + op_info["key"] + op_info["value"] + nonce
    curr_hash = hashlib.sha256(hashable.encode()).hexdigest()

    while (curr_hash[-1] != "0" and curr_hash[-1] != "1" and curr_hash[-1] != "2"):
        nonce = generate_nonce()
        hashable = op_info["op"] + op_info["key"] + op_info["value"] + nonce
        curr_hash = hashlib.sha256(hashable.encode()).hexdigest()

    print(f"Found nonce: {nonce} for hash value: {curr_hash}")

    hash_ptr = None
    if len(BLOCKCHAIN) != 0:
        hashable = BLOCKCHAIN[-1]["operation"]["key"] + BLOCKCHAIN[-1]["operation"]["value"] +  BLOCKCHAIN[-1]["header"]["nonce"] + BLOCKCHAIN[-1]["header"]["hash"]
        hash_ptr = hashlib.sha256(hashable.encode()).hexdigest()
    else:
        hash_ptr = init_hash

    block = {
    "header": {"nonce": nonce, "hash": hash_ptr}, 
    "operation": {"op": op_info["op"], "key": op_info["key"], "value": op_info["value"]}
    }

    return block

def leader_accept():
    global MY_PID
    global LEADER
    global QUEUE

    while True:
        while(LEADER != MY_PID or len(QUEUE) == 0):
            pass

        message = {}
        message["type"] = "accept"
        message["ballot"] = CURR_BAL_NUM
        message["sender_pid"] = MY_PID
        #make block by finding nonce
        block = create_block(QUEUE[0])
        message["value"] = block
        encoded_message = pickle.dumps(message)
        increment_seq_num()
        time.sleep(5)
        for s_pid, conn in OTHER_SERVERS.items():
            print(f"Sending accept request from server {MY_PID} to server {s_pid}")
            conn.sendall(encoded_message)

        while(ENOUGH_ACCEPTED == False):
            pass

        print("The people have accepted my value")
        decide(block)

def decide(block):
    global BLOCKCHAIN
    global STUDENTS
    global QUEUE
    #Add block to blockchain
    BLOCKCHAIN.append(block)
    #Update key-value store
    key = block["operation"]["key"]
    if block["operation"]["op"] == "put":
        value = block["operation"]["value"]
        STUDENTS[key] = value
    client_id = QUEUE[0]["client_id"]
    #Pop operation from queue
    QUEUE.popleft()

    client_message = {}
    client_message["type"] = "server_response"
    if block["operation"]["op"] == "put":
        client_message["response"] = "ack"
    else:
        value = STUDENTS[key]
        client_message["response"] = "Value for key {} is {}".format(key,value)
    print(f"Sending response to client {client_id}")
    conn = CLIENTS[str(client_id)]
    encoded_message = pickle.dumps(client_message)
    time.sleep(5)
    increment_seq_num()
    conn.sendall(encoded_message)

    server_message = {}
    server_message["type"] = "decide"
    server_message["value"] = block

    encoded_message = pickle.dumps(server_message)
    for s_pid, conn in OTHER_SERVERS.items():
        print(f"Sending decide from server {MY_PID} to server {s_pid}")
        conn.sendall(encoded_message)
    #Notify servers and clients about the block


def leader_request(client_id):
    global MY_PID
    global OTHER_SERVERS
    global NUM_PROMISES
    global CURR_BAL_NUM
    global PREPARE_NACKED

    increment_seq_num()
    ballot = get_ballot_num()
    CURR_BAL_NUM = ballot
    message = {}
    message["type"] = "prepare"
    message["ballot"] = ballot
    message["sender_pid"] = MY_PID
    message["client_id"] = client_id
    encoded_message = pickle.dumps(message)
    time.sleep(5)
    for s_pid, conn in MY_QUORUM.items():
        print(f"Sending prepare from server {MY_PID} to server {s_pid}")
        conn.sendall(encoded_message)
    while(ENOUGH_PROMISES != True):
        #CHECK IF YOU GET A NACK, try again after some time 
        if(PREPARE_NACKED == True):
            #retry prepare: set promises to zero, set PREPARE_NACKED to False, sleep 5 seconds, call leader_request
            print("I got PREPARE_NACKED on prepare request")
            threading.Thread(target = retry_prepare, args=()).start()
            break
        pass
    if(PREPARE_NACKED == False):
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
    time.sleep(5)
    for s_pid, conn in OTHER_SERVERS.items():
        conn.sendall(encoded_message)
    for c_pid, conn in CLIENTS.items():
        conn.sendall(encoded_message)
    LEADER = MY_PID
    NUM_PROMISES = 0

def retry_prepare():
    global NUM_PROMISES
    global PREPARE_NACKED

    NUM_PROMISES = 0
    time.sleep(5)
    PREPARE_NACKED = False
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
    global PREPARE_NACKED
    global NUM_ACCEPTED
    global ENOUGH_ACCEPTED
    global BLOCKCHAIN
    global STUDENTS

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
                            new_val = prom_val["accepted_val"]["operation"]
                            new_val["client_id"] = prom_val["client_id"]
                if new_val:
                    QUEUE.appendleft(new_val)
                ENOUGH_PROMISES = True
            LOCK.release()
        elif message["type"] == "leader_broadcast":
            LEADER = message["leader"]
            print(f"Assigned leader to {LEADER}")
        elif message["type"] == "prepare_nack":
            PREPARE_NACKED = True
        elif message["type"] == "accept":
            sender = message["sender_pid"]
            print(f"Received accept from server {sender}")
            threading.Thread(target=handle_accept, args = (message,)).start()
        elif message["type"] == "accepted":
            LOCK.acquire()
            NUM_ACCEPTED += 1
            if NUM_ACCEPTED >= 2:
                ENOUGH_ACCEPTED = True
            LOCK.release()
        elif message["type"] == "decide":
            print(f"Received decision from leader")
            block = message["value"]
            BLOCKCHAIN.append(block)
            key = block["operation"]["key"]
            value = block["operation"]["value"] 
            STUDENTS[key] = value



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
    client_id = message["client_id"]
    print(f"proposer ballot: {proposer_ballot}")
    #compare depth, seq_num, andpid of proposer to currently held values
    if(proposer_ballot[2] < DEPTH or proposer_ballot[0] < CURR_BAL_NUM[0]  or (proposer_ballot[0] == CURR_BAL_NUM[0] and proposer_ballot[1] < CURR_BAL_NUM[1])):
        print(f"Proposal no good")
        reply["type"] = "prepare_nack"
        reply["ballot"] = proposer_ballot
        reply["error"] = "prepare rejected"
        reply_encoded = pickle.dumps(reply)
        time.sleep(5)
        increment_seq_num()
        conn.sendall(reply_encoded)
    #All clear to send promise
    else:
        CURR_BAL_NUM = proposer_ballot         
        reply["type"] = "promise"
        reply["promise_values"] = {"bal": CURR_BAL_NUM, "accepted_num": ACCEPTED_NUM, "accepted_val": ACCEPTED_VAL, "depth": DEPTH, "client_id": client_id}
        reply_encoded = pickle.dumps(reply)
        print(f"Sending promise back to leader")
        time.sleep(5)
        increment_seq_num()
        conn.sendall(reply_encoded)

def handle_accept(message):
    global OTHER_SERVERS
    global DEPTH
    global ACCEPTED_NUM
    global ACCEPTED_PID
    global CURR_BAL_NUM

    sender_pid = int(message["sender_pid"])
    conn = OTHER_SERVERS[sender_pid]
    reply = {}

    sender_ballot = message["ballot"]
    if(sender_ballot[2] < DEPTH or sender_ballot[0] < CURR_BAL_NUM[0]  or (sender_ballot[0] == CURR_BAL_NUM[0] and sender_ballot[1] < CURR_BAL_NUM[1])):
        print(f"Accept no good")
        reply["type"] = "accept_nack"
        reply["ballot"] = sender_ballot
        reply["error"] = "accept rejected"
        reply_encoded = pickle.dumps(reply)
        time.sleep(5)
        increment_seq_num()
        conn.sendall(reply_encoded)
    else:
        CURR_BAL_NUM = sender_ballot 
        ACCEPTED_NUM = sender_ballot  
        ACCEPTED_VAL = message["value"]      
        reply["type"] = "accepted"
        reply["accepted_vals"] = {"bal": CURR_BAL_NUM, "accepted_num": ACCEPTED_NUM, "accepted_val": ACCEPTED_VAL, "depth": DEPTH}
        reply_encoded = pickle.dumps(reply)
        print(f"Sending accepted back to leader")
        time.sleep(5)
        increment_seq_num()
        conn.sendall(reply_encoded)


def handle_client(stream):
    while(True):
        data = stream.recv(1024)
        if not data:
            break
        message = pickle.loads(data)
        if message["type"] == "leader_request":
            client_id = message["client_id"]
            threading.Thread(target = leader_request, args = (client_id,)).start()
        elif message["type"] == "put" or message["type"] == "get":
            client_id = message["client_id"]
            mtype = message["type"]
            print(f"received {mtype} request from client {client_id}")
            op = {}
            LOCK.acquire()
            QUEUE.append({"op": message["type"], "key": message["id"], "value":message["value"], "client_id": message["client_id"]})
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
    init_string ="I am Warhol"
    init_hash = hashlib.sha256(init_string.encode()).hexdigest()
    # output_file = open(sys.argv[1], 'w')
    #Load server json file with port nums
    with open('server_config.json') as conf:
        server_pids = json.load(conf)

    #Load client json file with port nums
    # with open('client_config.json') as conf:
    #     client_pids = json.load(conf)

    #Start to listen for other servers trying to listen_for_serversect
    #threading.Thread(target=server_listen, args=(pid, data, server_sock)).start()
    threading.Thread(target=handle_input, args=(
        listen_for_servers, server_pids)).start()

    threading.Thread(target = leader_accept).start()

    while True:
        try:
            stream, address = listen_for_servers.accept()
            data = stream.recv(1024)
            if not data:
                break
            message = data.decode('utf-8')
            message = message.split()
            if message[0] == "server":
                threading.Thread(target=handle_server, args=(stream,)).start()
            elif message[0] == "client":
                print("Connected to client")
                CLIENTS[message[1]] = stream
                client_id = message[1]
                print(f"Starting client listen for client {client_id}")
                threading.Thread(target=handle_client, args=(stream,)).start()
        except KeyboardInterrupt:
            do_exit(output_file, listen_for_servers)

