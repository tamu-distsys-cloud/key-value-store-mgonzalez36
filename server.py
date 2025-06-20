import logging
import threading
from typing import List

from labrpc.labrpc import ClientEnd

debugging = True

# Use this function for debugging
def debug(fmt, *args):
    if debugging:
        logging.info(fmt % args)

# Put or Append
class PutAppendArgs:
    # Add definitions here if needed
    def __init__(self, key: str, value: str, clientID=None, requestID=None):
        self.key = key
        self.value = value
        self.clientID = clientID
        self.requestID = requestID

class PutAppendReply:
    # Add definitions here if needed
    def __init__(self, value: str):
        self.value = value

class GetArgs:
    # Add definitions here if needed
    def __init__(self, key: str, clientID=None, requestID=None):
        self.key = key
        self.clientID = clientID
        self.requestID = requestID

class GetReply:
    # Add definitions here if needed
    def __init__(self, value: str):
        self.value = value

class KVServer:
    def __init__(self, cfg):
        # Thread lock to protect shared states
        self.mu = threading.Lock()
        # 'cfg' reference 
        self.cfg = cfg

        # Your definitions here.
        # In-memory kv store
        self.kv_store = {}

        # In-memory cache for deduplication
        # maps(clientID, requestID) -> previous value
        self.completedRequests = {}

        # Total number of shards across the cluster
        self.numShards = getattr(cfg, 'nservers', len(getattr(cfg, 'kvservers', [])))

    @property
    def me(self) -> int:
        # Return this server's index via cfg method
        return self.cfg.kvservers.index(self)

    @property
    def numReplicas(self) -> int:
        # Total number of replicas per shard via cfg method
        return getattr(self.cfg, 'nreplicas', 1)
    

    # Fetching the shard which contains a key
    def fetchShard(self, key: str) -> int:
        try:
            kn = int(key)
        # If key has non-numeric code, fallback to using sum of character codes
        except ValueError:
            kn = sum(ord(c) for c in key)
        return kn % self.numShards

    # Checking if a specific (self) is one of the replicas for the key's shard
    def replicaHasKey(self, key: str) -> bool:
        shard = self.fetchShard(key)
        for i in range(self.numReplicas):
            if (shard + i) % self.numShards == self.me:
                return True
        return False

    # Function to check if this server is the primary keyholder (first) replica for the key's shard
    def isFirstServer(self, key: str) -> bool:
        return self.me == self.fetchShard(key)

    # Function that forwards an RPC message to the primary replica
    def forwardToFirstServer(self, method: str, args):
        primary = self.cfg.kvservers[self.fetchShard(args.key)]
        return getattr(primary, method)(args)

    # Upon primary updating its state, mirror changes to the primary's replicas (mirror state)
    def replicateToOtherServers(self, method: str, args: PutAppendArgs):
        shard = self.fetchShard(args.key)
        # Iterate through numReplicas excluding self (i = 1)
        for i in range(1, self.numReplicas):
            # De hashing
            idx = (shard + i) % self.numShards
            if idx == self.me:
                continue
            # Once a replica of the primary is found, fetch that server idx
            replica = self.cfg.kvservers[idx]
            with replica.mu:
                if method == "Put":
                    # Perform "Put" op on the replica found above
                    replica.kv_store[args.key] = args.value
                else: # method == "Append"
                    # Fetch current (old) value
                    old = replica.kv_store.get(args.key, "")
                    # Perform "append" op (old + new val)
                    replica.kv_store[args.key] = old + args.value
                # Copy de-duplication cache to replica
                if args.clientID is not None and args.requestID is not None:
                    k = (args.clientID, args.requestID)
                    replica.completedRequests[k] = self.completedRequests.get(k)

    def Get(self, args: GetArgs):
        # If the replica does not have key, then return None
        if not self.replicaHasKey(args.key):
            return None
        
        with self.mu:
            k = (args.clientID, args.requestID) if args.clientID is not None else None
            # Checking the cache (for de-duplication)
            if k and k in self.completedRequests:
                # If a key is found in the completedRequests cache
                # Then return a GetReply of said key
                return GetReply(self.completedRequests[k])
            
            # Proceed if new request (return blank)
            val = self.kv_store.get(args.key, "")

            # If a key is found, then cache the response for future de-duplication
            if k:
                self.completedRequests[k] = val
            
            # Return a GetReply of val (empty string since key is novel)
            return GetReply(val)

    def Put(self, args: PutAppendArgs):
        if not self.replicaHasKey(args.key):
            return None
        
        # Only the primary should fulfill Put requests, so check to make sure
        # current server is the primary, if not then forward to the primary server
        if not self.isFirstServer(args.key):
            return self.forwardToFirstServer("Put", args)

        # 
        k = (args.clientID, args.requestID) if args.clientID is not None else None
        with self.mu:
            # Return cached old value if duplicate
            old = self.kv_store.get(args.key, "")

            # As in Get(), cache request and return a reply
            if k and k in self.completedRequests:
                return PutAppendReply(self.completedRequests[k])
            self.kv_store[args.key] = args.value
            if k:
                self.completedRequests[k] = old

        # Mirror to backups
        if k:
            self.replicateToOtherServers("Put", args)
        return PutAppendReply(old)

    def Append(self, args: PutAppendArgs):
        # Checking if the current server has a key
        if not self.replicaHasKey(args.key):
            return None
        
        # Ensuring only the primary fulfulls request
        if not self.isFirstServer(args.key):
            return self.forwardToFirstServer("Append", args)

        # Apply append + deduplicate
        k = (args.clientID, args.requestID) if args.clientID is not None else None
        with self.mu:
            old = self.kv_store.get(args.key, "")
            if k and k in self.completedRequests:
                return PutAppendReply(self.completedRequests[k])
            self.kv_store[args.key] = old + args.value
            if k:
                self.completedRequests[k] = old

        # Mirror to backups
        if k:
            self.replicateToOtherServers("Append", args)
        return PutAppendReply(old)
