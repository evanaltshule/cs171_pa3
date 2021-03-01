import json
import hashlib
import random
import string
import os
"""
Block Data Structure:
Dictionary
{
    "header": [nonce, prev_hash]
    "operation": [op,key,value] //value is empty dict if put operation
}
"""

BLOCKCHAIN = []

def make_block(header, operation):
    block = {
    "header": [header[0], header[1]],
    "operation": [operation[0], operation[1], operation[2]]
    }
    return block

def find_nonce(operation):
    while(True):
        #Generate a random 10 letter word
        nonce = ''.join(random.choice(string.ascii_letters) for i in range(10))
        hashable = operation[0] + operation[1] + operation[2] + rand
        h = (hashlib.sha256(nonce.encode())).hexdigest()
        #Check if the last letter of the hash in in between 0 and 2
        check = h[-1]
        if(int(check) >= 0 and int(check) <=2):
            return nonce
        
#Fill blockchain with two dummy blocks
def make_test_blockchain():
    global BLOCKCHAIN
    #make block 1
    header = ["cDerLTddRjJ", 12324323243]
    operation = ["put","alice_netid"]
    value = {"phone_number": "111-222-3333"}
    operation.append(value)
    block1 = make_block(header, operation)
    #make block 2
    BLOCKCHAIN.append(block1)
    header = ["bRdDoKBqaZ", 8765432]
    operation = ["get","alice_netid"]
    value = {}
    operation.append(value)
    block2 = make_block(header, operation)
    BLOCKCHAIN.append(block2)

#Write current blockchain to disk
def write_to_disk(): 
    global BLOCKCHAIN
    for block in BLOCKCHAIN:
        append_block(block)
#helper function to append block to file
def append_block(block):
    with open('blockchain.txt', 'a') as f:
        json.dump(block, f)
        f.write(os.linesep)

#Read blockchain from file and update global state
def read_from_disk():
    global BLOCKCHAIN
    try:
        with open('blockchain.txt') as f:
            BLOCKCHAIN = [json.loads(line) for line in f]
    except FileNotFoundError:
        print("File being read does not exist")

#print blocks on blockchain
def print_blockchain():
    global BLOCKCHAIN
    for block_num in range(len(BLOCKCHAIN)):
        print(f"Block {block_num}: ")
        print(BLOCKCHAIN[block_num])


#MAIN
while(True):
    inp = input()
    if inp == "write":
        print("Writing two dummy blocks to blockchain.txt")
        make_test_blockchain()
        write_to_disk()
    if inp == "read":
        read_from_disk()
    if inp == "print":
        print_blockchain()
    if inp == "exit":
        os._exit(0)





# header = ("bRdDoKBqaZ", hashlib.sha256(block1.encode('ascii')).hexdigest())
