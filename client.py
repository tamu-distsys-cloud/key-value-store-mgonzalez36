import random
import threading
from typing import Any, List

from labrpc.labrpc import ClientEnd
from server import GetArgs, GetReply, PutAppendArgs, PutAppendReply

def nrand() -> int:
    return random.getrandbits(62)

class Clerk:
    def __init__(self, servers: List[ClientEnd], cfg):
        self.servers = servers
        self.cfg = cfg
        self.serverIdx = 0
        # Your definitions here

        ### Task 2 ###
        # Generate random client ID to ensure unique reqs
        self.clientID = nrand()
        # Get server id
        self.reqID = 0
    def getNextReqID(self):
        # Generate the next incoming request ID
        self.reqID += 1
        return self.reqID
        ### Task 2 ###

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
        requestID = self.getNextReqID()
        args = GetArgs(key, self.clientID, requestID)

        # Keep trying forever until a response is received
        while True:
            try:
                # Make a call to the available server
                reply = self.servers[self.serverIdx].call("KVServer.Get", args)
                if reply is not None and hasattr(reply, 'value'):
                    # If the reply from the server is not empty then return the fetched val
                    return reply.value
            except Exception as e:
                # If an error is caught, then continue trying
                pass
        return ""

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
        requestID = self.getNextReqID()
        args = PutAppendArgs(key, value, self.clientID, requestID)
        # Keep trying forever until a response is achieved
        while True:
            try:
                reply = self.servers[self.serverIdx].call("KVServer." + op, args)
                if reply is not None and hasattr(reply, 'value'):
                    return reply.value
            except Exception as e:
                pass
        return ""

    def put(self, key: str, value: str):
        self.put_append(key, value, "Put")

    # Append value to key's value and return that value
    def append(self, key: str, value: str) -> str:
        return self.put_append(key, value, "Append")