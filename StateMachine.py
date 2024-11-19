from abc import ABC, abstractmethod
import select, json, time, socket

BROADCAST = "FFFF"
RECV_WAIT = 0.1
SEND_WAIT = 0.01
LEADER_TIMEOUT = 0.15

class Node(ABC):
    def __init__(self):
        raise TypeError('State cannot be instantiated and must be used statically.')
    
    def execOnTimeout():
        pass

    ##########################
    # Message type executors #
    ##########################
    def put(): # from client
        pass

    def get(): # from client
        pass

    def appendEntriesRPC():
        pass

    def requestVoteRPC():
        pass

    def voteReceived():
        pass

class Leader(Node):
    pass


class Follower(Node):
    pass

class Candidate(Node):
    pass