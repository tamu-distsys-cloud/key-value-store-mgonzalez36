import threading
# CHAT GPT GENERATED CODE - start
# Explanation: Program would hang, so I asked chatgpt to help with this problem
# Insertion of this snippet below allows for threads spun up by client.py to be 
# forced to shutdown upon program (unit test) completion.
# This prevents hanging and allows me to sequentially run tests without having to 
# open a new terminal instance.
_orig_start = threading.Thread.start
def _daemon_start(self, *args, **kwargs):
    self.daemon = True
    return _orig_start(self, *args, **kwargs)
threading.Thread.start = _daemon_start
# CHAT GPT GENERATED CODE - end
import random
from typing import Any, List

from labrpc.labrpc import ClientEnd
from server import GetArgs, GetReply, PutAppendArgs, PutAppendReply

def nrand() -> int:
    return random.getrandbits(62)

class Clerk:
    def __init__(self, servers: List[ClientEnd], cfg):
        self.servers = servers
        self.cfg = cfg

        ### Task 1 ###
        # Assuming that a server exists with index 0
        self.serverIdx = 0

        ### Task 2 ###
        # Generate random client ID to ensure unique reqs
        self.clientID = nrand()

        # Get server id
        self.requestID = 0

        ### Task 3 ###
        # number of shards equates to number of server instances
        self.numShards = getattr(cfg, 'nservers', len(servers))
        self.numReplicas = getattr(cfg, 'nreplicas', 1)

    ### Task 3 ###
    # 1 - fetch the shard in which a key resides in
    def fetchMainShard(self, key:str) -> int:
        # Having issues with hashing, so will manually handle non-numeric keys
        try:
            keyNumber = int(key)
        except ValueError as e:
            keyNumber = sum(ord(c) for c in key)
        # use the simple key % nshards method used to determine the shards
        return keyNumber % self.numShards
    
    # 2 - need to also fetch the k/v stored in the replicas
    def fetchReplicas(self, shard: int) -> List[int]:
        replicas = [] # initializing list of replicas
        # iterate over all replicas and append to the list of replicas
        for i in range(self.numReplicas):
            serverIdx = (shard + i) % len(self.servers)
            replicas.append(serverIdx)
        return replicas
        
    def getNextRequestID(self):
        # Generate the next incoming request ID
        self.requestID += 1
        return self.requestID

    # Fetch the current value for a key.
    # Returns "" if the key does not exist.
    # Keeps trying forever in the face of all other errors.
    #
    # You can send an RPC with code like this:
    # reply = self.server[i].call("KVServer.Get", args)
    # assuming that you are connecting to the i-th server.
    #
    # The types of args and reply (including whether they are pointers)
    # must match the declared types of the RPC handler function's
    # arguments in server.py.

    def get(self, key: str) -> str:
        # You will have to modify this function.
        # Fetching the value in the face of no errors
        requestID = self.getNextRequestID()
        args = GetArgs(key, self.clientID, requestID)

        ### Task 3 ###
        # With the introduction of shards, we first need to find which shard the key belongs to
        shard = self.fetchMainShard(key)
        replicas = self.fetchReplicas(shard)
        ### Task 3 ###

        # Must try each replica server until client receives a response
        while True:
            for serverIdx in replicas:
                # Keep trying forever until a response is received
                try:
                    # Make a call to the available server
                    reply = self.servers[serverIdx].call("KVServer.Get", args)
                    if reply is not None:
                        # If the reply from the server is not empty then return the fetched val
                        return reply.value
                except Exception:
                    # If an error is caught, then continue trying
                    continue
                # loop forever until a response is achieved

    # Shared by Put and Append.
    #
    # You can send an RPC with code like this:
    # reply = self.servers[i].call("KVServer."+op, args)
    # assuming that you are connecting to the i-th server.
    #
    # The types of args and reply (including whether they are pointers)
    # must match the declared types of the RPC handler function's
    # arguments in server.py.

    def put_append(self, key: str, value: str, op: str) -> str:
        # You will have to modify this function.
        requestID = self.getNextRequestID()
        args = PutAppendArgs(key, value, self.clientID, requestID)

        ### Task 3 ###
        # As in get(), we must find which shard corresponds to each key
        shard = self.fetchMainShard(key)
        replicas = self.fetchReplicas(shard)

        # Must try each replica server until client receives a response
        while True:
            for serverIdx in replicas:
                # Keep trying forever until a response is achieved
                try:
                    reply = self.servers[serverIdx].call("KVServer." + op, args)
                    if reply is not None:
                        return reply.value
                except Exception:
                    continue
                # loop again

    def put(self, key: str, value: str):
        self.put_append(key, value, "Put")

    # Append value to key's value and return that value
    def append(self, key: str, value: str) -> str:
        return self.put_append(key, value, "Append")