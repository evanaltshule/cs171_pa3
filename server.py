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
LEADER_ELECTION = False
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

FAILED_LINKS = {}


def do_exit():
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
                do_exit()
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
            elif inp[0] == "faillink":
                src = int(inp[1])
                dest = int(inp[2])
                threading.Thread(target=fail_link, args=(src, dest)).start()
            elif inp[0] == "fixlink":
                src = int(inp[1])
                dest = int(inp[2])
                threading.Thread(target=fix_link, args=(src, dest)).start()
            elif inp[0] == "failprocess":
                threading.Thread(target=fail_process).start()

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

def fail_process():
    global OTHER_SERVERS
    global MY_PID
    global FAILED_LINKS

    message = {}
    message["type"] = "faillink"
    message["sender"] = MY_PID
    encoded_message = pickle.dumps(message)
    print(f"Killing process...")
    for pid, conn in OTHER_SERVERS.items():
        conn.sendall(encoded_message)
    do_exit()

def fail_link(src, dest):
    global OTHER_SERVERS
    global MY_QUORUM
    global MY_PID
    global FAILED_LINKS

    conn = OTHER_SERVERS[dest]
    message = {}
    message["type"] = "faillink"
    message["sender"] = MY_PID
    encoded_message = pickle.dumps(message)
    time.sleep(5)
    conn.sendall(encoded_message)

    FAILED_LINKS[dest] = conn
    del OTHER_SERVERS[dest]

    print(f"Link failed with server {dest}")

    if MY_QUORUM.get(dest, -1) != -1:
        del MY_QUORUM[dest]
        for pid, conn in OTHER_SERVERS.items():
            if pid != dest and MY_QUORUM.get(pid, -1) == -1:
                print(f"Added server {pid} to my quorum")
                MY_QUORUM[pid] = conn
                break

def fix_link(src, dest):
    global OTHER_SERVERS
    global FAILED_LINKS

    conn = FAILED_LINKS[dest]
    OTHER_SERVERS[dest] = conn
    del FAILED_LINKS[dest]
    message = {}
    message["type"] = "fixlink"
    message["sender"] = MY_PID
    encoded_message = pickle.dumps(message)
    time.sleep(5)
    conn.sendall(encoded_message)

    print(f"Link fixed with server {dest}")

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
    global NUM_ACCEPTED
    global ENOUGH_ACCEPTED

    while True:
        while(LEADER != MY_PID or len(QUEUE) == 0):
            pass
        increment_seq_num()
        ballot = get_ballot_num()
        message = {}
        message["type"] = "accept"
        message["ballot"] = ballot
        message["sender_pid"] = MY_PID
        #make block by finding nonce
        block = create_block(QUEUE[0])
        message["value"] = block
        encoded_message = pickle.dumps(message)
        time.sleep(5)
        for s_pid, conn in OTHER_SERVERS.items():
            print(f"Sending accept request to server {s_pid}")
            conn.sendall(encoded_message)

        while(ENOUGH_ACCEPTED == False):
            pass
        increment_seq_num()
        print("The people have accepted my value")
        decide(block)
        NUM_ACCEPTED = 0
        ENOUGH_ACCEPTED = False

def decide(block):
    global BLOCKCHAIN
    global STUDENTS
    global QUEUE
    global DEPTH
    #Add block to blockchain
    increment_seq_num()
    BLOCKCHAIN.append(block)
    DEPTH += 1
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
    global PREPARE_NACKED
    global LEADER_ELECTION

    if LEADER_ELECTION == False:
        LEADER_ELECTION = True
        increment_seq_num()
        ballot = get_ballot_num()
        message = {}
        message["type"] = "prepare"
        message["ballot"] = ballot
        message["sender_pid"] = MY_PID
        message["client_id"] = client_id
        encoded_message = pickle.dumps(message)
        time.sleep(5)
        for s_pid, conn in MY_QUORUM.items():
            print(f"Sending prepare with ballot {ballot} to server {s_pid}")
            conn.sendall(encoded_message)
        while(ENOUGH_PROMISES != True):
            #CHECK IF YOU GET A NACK, try again after some time 
            if(PREPARE_NACKED == True):
                #retry prepare: set promises to zero, set PREPARE_NACKED to False, sleep 5 seconds, call leader_request
                print("I got PREPARE_NACKED on prepare request")
                LEADER_ELECTION = False
                threading.Thread(target = retry_prepare, args=(client_id,)).start()
                break
            pass
        increment_seq_num()
        if(PREPARE_NACKED == False):
            leader_broadcast()

def leader_broadcast():
    global MY_PID 
    global CLIENTS
    global LEADER
    global LEADER_ELECTION

    message = {}
    message["type"] = "leader_broadcast"
    message["leader"] = MY_PID
    encoded_message = pickle.dumps(message)
    print(f"I am the leader!")
    time.sleep(5)
    for s_pid, conn in OTHER_SERVERS.items():
        conn.sendall(encoded_message)
    for c_pid, conn in CLIENTS.items():
        conn.sendall(encoded_message)
    LEADER = MY_PID
    NUM_PROMISES = 0
    ENOUGH_PROMISES = False
    LEADER_ELECTION = False


def retry_prepare(client_id):
    global NUM_PROMISES
    global PREPARE_NACKED

    NUM_PROMISES = 0
    time.sleep(5)
    PREPARE_NACKED = False
    increment_seq_num()
    leader_request(client_id)

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
    global DEPTH

    while(True):
        data = stream.recv(4096)
        if not data:
            break
        message = pickle.loads(data)
        #Handle prepare message
        if message["type"] == "prepare":
            sender = message["sender_pid"]
            print(f"Received prepare message from server {sender}")
            threading.Thread(target=handle_prepare, args = (message,)).start()
        #Handle promise message
        elif message["type"] == "promise":
            LOCK.acquire()
            NUM_PROMISES += 1
            PROMISED_VALS.append(message["promise_values"])
            if message["promise_values"]["depth"] < DEPTH:
                serv = message["sender"]
                print("Updating server {sender}")
                reply = {}
                reply["type"] = "update"
                reply["blockchain"] = BLOCKCHAIN
                reply["students"] = STUDENTS
                conn = OTHER_SERVERS[message["sender"]]
                encoded_reply = pickle.dumps(reply)
                conn.sendall(encoded_reply)
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
            if message["behind"] == True:
                print("I was behind, updating now.")
                BLOCKCHAIN = message["blockchain"]
                STUDENTS = message["students"]
                DEPTH = len(BLOCKCHAIN)
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
            DEPTH += 1
            block = message["value"]
            BLOCKCHAIN.append(block)
            key = block["operation"]["key"]
            value = block["operation"]["value"] 
            STUDENTS[key] = value
            increment_seq_num()
        elif message["type"] == "faillink":
            sender = message["sender"]
            print(f"Received faillink from {sender}")
            threading.Thread(target=handle_fail_link, args=(sender,)).start()
        elif message["type"] == "fixlink":
            sender = message["sender"]
            print(f"Received fixlink from {sender}")
            threading.Thread(target=handle_fix_link, args=(sender,)).start()
        elif message["type"] == "update":
            print("updating values and blockchain")
            BLOCKCHAIN = message["blockchain"]
            STUDENTS = message["students"]
            DEPTH =len(BLOCKCHAIN)

def handle_prepare(message):
    global OTHER_SERVERS
    global DEPTH
    global ACCEPTED_NUM
    global ACCEPTED_PID
    global BLOCKCHAIN
    global STUDENTS
    global MY_PID
    global SEQ_NUM

    sender_pid = int(message["sender_pid"])
    conn = OTHER_SERVERS[sender_pid]
    reply = {}

    proposer_ballot = message["ballot"]
    client_id = message["client_id"]
    print(f"proposer ballot: {proposer_ballot}")
    #compare depth, seq_num, andpid of proposer to currently held values
    if(proposer_ballot[2] < DEPTH or proposer_ballot[0] < SEQ_NUM  or (proposer_ballot[0] == SEQ_NUM and proposer_ballot[1] < MY_PID)):
        my_bal = get_ballot_num()
        print(f"Proposal no good, smaller than {my_bal}")
        reply["type"] = "prepare_nack"
        reply["ballot"] = proposer_ballot
        reply["error"] = "prepare rejected"
        if proposer_ballot[2] < DEPTH:
            reply["behind"] = True
            reply["blockchain"] = BLOCKCHAIN
            reply["students"] = STUDENTS
        else:
            reply["behind"] = False
        reply_encoded = pickle.dumps(reply)
        time.sleep(5)
        conn.sendall(reply_encoded)
    #All clear to send promise
    else:
        SEQ_NUM = proposer_ballot[0]
        my_bal = get_ballot_num()         
        reply["type"] = "promise"
        reply["sender"] = MY_PID
        reply["promise_values"] = {"bal": my_bal, "accepted_num": ACCEPTED_NUM, "accepted_val": ACCEPTED_VAL, "depth": DEPTH, "client_id": client_id}
        reply_encoded = pickle.dumps(reply)
        print(f"Sending promise back to leader")
        time.sleep(5)
        increment_seq_num()
        conn.sendall(reply_encoded)
    increment_seq_num()

def handle_accept(message):
    global OTHER_SERVERS
    global DEPTH
    global ACCEPTED_NUM
    global ACCEPTED_PID
    global SEQ_NUM
    global MY_PID

    sender_pid = int(message["sender_pid"])
    conn = OTHER_SERVERS[sender_pid]
    reply = {}

    sender_ballot = message["ballot"]
    my_bal = get_ballot_num()
    print(f"Sender ballot: {sender_ballot} My current ballot: {my_bal}")
    if(sender_ballot[2] < DEPTH or sender_ballot[0] < SEQ_NUM  or (sender_ballot[0] == SEQ_NUM and sender_ballot[1] < MY_PID)):
        print(f"Accept no good")
        reply["type"] = "accept_nack"
        reply["ballot"] = sender_ballot
        reply["error"] = "accept rejected"
        reply_encoded = pickle.dumps(reply)
        time.sleep(5)
        conn.sendall(reply_encoded)
    else:
        SEQ_NUM = sender_ballot[0] 
        my_bal = get_ballot_num()
        ACCEPTED_NUM = my_bal  
        ACCEPTED_VAL = message["value"]      
        reply["type"] = "accepted"
        reply["accepted_vals"] = {"bal": my_bal, "accepted_num": ACCEPTED_NUM, "accepted_val": ACCEPTED_VAL, "depth": DEPTH}
        reply_encoded = pickle.dumps(reply)
        print(f"Sending accepted back to leader")
        time.sleep(5)
        increment_seq_num()
        conn.sendall(reply_encoded)
    increment_seq_num()

def handle_fail_link(sender):
    global FAILED_LINKS
    global OTHER_SERVERS
    global MY_QUORUM

    conn = OTHER_SERVERS[sender]
    FAILED_LINKS[sender] = conn
    del OTHER_SERVERS[sender]

    print(f"Link failed with server {sender}")

    if MY_QUORUM.get(sender, -1) != -1:
        del MY_QUORUM[sender]
        for pid, conn in OTHER_SERVERS.items():
            if pid != sender and MY_QUORUM.get(pid, -1) == -1:
                print(f"Added server {pid} to my quorum")
                MY_QUORUM[pid] = conn
                break

def handle_fix_link(sender):
    global FAILED_LINKS
    global OTHER_SERVERS

    conn = FAILED_LINKS[sender]
    OTHER_SERVERS[sender] = conn
    del FAILED_LINKS[sender]

    print(f"Link fixed with server {sender}")

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
            print(f"Received {mtype} request from client {client_id}")
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

