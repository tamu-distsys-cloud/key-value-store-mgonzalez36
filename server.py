import logging
import threading
from typing import Tuple, Any

debugging = False

# Use this function for debugging
def debug(format, *args):
    if debugging:
        logging.info(format % args)

# Put or Append
class PutAppendArgs:
    # Add definitions here if needed
    def __init__(self, key, value, clientID = None, requestID = None):
        self.key = key
        self.value = value
        self.clientID = clientID
        self.requestID = requestID

class PutAppendReply:
    # Add definitions here if needed
    def __init__(self, value):
        self.value = value

class GetArgs:
    # Add definitions here if needed
    def __init__(self, key, clientID = None, requestID = None):
        self.key = key
        self.clientID = clientID
        self.requestID = requestID

class GetReply:
    # Add definitions here if needed
    def __init__(self, value):
        self.value = value

class KVServer:
    def __init__(self, cfg):
        self.mu = threading.Lock()
        self.cfg = cfg
        # Your definitions here.
        self.kv_store = {}

        ### Task 2 ###
        self.completedRequests = {}
        ### Task 2 ###

    def Get(self, args: GetArgs):
        with self.mu:
            ### Task 2 ###
            # For task 2, the use of unique client and request IDs will be required
            # so that we are able to uniquely identify them for single executions
            if args.clientID is not None and args.requestID is not None:
                requestKey = (args.clientID, args.requestID)
                # If both client and req keys are not empty, then package then as request key obj
                if requestKey in self.completedRequests:
                    # If the request key has already been completed, then reply with the cached value
                    # this ensures single execution
                    cachedVal = self.completedRequests[requestKey]
                    reply = GetReply("")
                    reply.value = cachedVal
                    return reply
                
            # If the request ID is unique and not found in cache, then fulfill the request
            value = self.kv_store.get(args.key, "")

            # Cache request key to ensure single execution
            if args.clientID is not None and args.requestID is not None:
                requestKey = (args.clientID, args.requestID)
                self.completedRequests[requestKey] = value

            # Generate reply and return
            reply = GetReply("")
            reply.value = value
            return reply
        

            # value = self.kv_store.get(args.key, "")
            # reply = GetReply("")
            # reply.value = value
            # debug("Get:(%s, %s)", args.key, value)
            # # Your code here.
            # return reply

    def Put(self, args: PutAppendArgs):
        with self.mu:
            # Check for duplicate request
            if args.clientID is not None and args.requestID is not None:
                # Construct requestKey
                requestKey = (args.clientID, args.requestID)
                if requestKey in self.completedRequests:
                    # Fetch the cached value and return the reply
                    cachedVal = self.completedRequests[requestKey]
                    reply = PutAppendReply("")
                    reply.value = cachedVal
                    return reply
                
            # If the request key is unique, then cache it, append and reply
            self.kv_store[args.key] = args.value
            appendResult = ""

            if args.clientID is not None and args.requestID is not None:
                requestKey = (args.clientID, args.requestID)
                self.completedRequests[requestKey] = appendResult
            
            reply = PutAppendReply("")
            reply.value = appendResult
            return reply

            # self.kv_store[args.key] = args.value
            # reply = PutAppendReply("")
            # reply.value = ""
            # debug("Put(%s, %s)", args.key, args.value)
            # return reply

    def Append(self, args: PutAppendArgs):
        with self.mu:
            if args.clientID is not None and args.requestID is not None:
                requestKey = (args.clientID, args.requestID)
                if requestKey in self.completedRequests:
                    cachedVal = self.completedRequests[requestKey]
                    reply = PutAppendReply("")
                    reply.value = cachedVal
                    return reply

            # Get  current val and put empty string placeholder
            oldVal = self.kv_store.get(args.key, "")

            # Append newVal to oldVal
            self.kv_store[args.key] = oldVal + args.value

            # Return oldVal

            if args.clientID is not None and args.requestID is not None:
                requestKey = (args.clientID, args.requestID)
                self.completedRequests[requestKey] = oldVal

            reply = PutAppendReply("")
            reply.value = oldVal
            return reply

            # reply = PutAppendReply("")
            # reply.value = oldVal
            # debug("Append: key %s, oldVal %s, newVal %s", args.key, oldVal, self.kv_store[args.key])
            # # Your code here.
            # return reply