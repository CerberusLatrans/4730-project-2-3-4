from abc import ABC, abstractmethod
import time, random
from collections import defaultdict, namedtuple

BROADCAST = "FFFF"
RECV_WAIT = 0.1
SEND_WAIT = 0.01
TIMEOUT_CENTER = 250 # ms 
TIMEOUT_RANGE = 50 # ms
HEARTBEAT_TIME = 0.15

# represents a RAFT log entry
Entry = namedtuple('Entry', ['term', 'key', 'value'])

# represents the state of a replica machine
# stores the persistent, volatile, and leader-specific data outlined by RAFT protocol
# also stores other miscellaneous data such as ids, random timeouts, and voting status
class State:
    def __init__(self, id, others):
        self.role = Follower

        # persistent
        self.data = defaultdict(lambda: "")
        self.term = 0
        self.log: list[Entry] = []
        self.voted_for = None

        # volatile
        self.commit_index = 0
        self.last_applied_index = 0

        # volatile leader
        self.next_indices = []
        self.match_indices = []

        #misc
        self.id = id
        self.others: list[str] = others
        self.leader_id = BROADCAST

        self.last_log_term = 0
        self.voting = False
        self.votes = None
        self.last_heartbeat = None
        self.timeout_sec = State._make_timeout()

    # returns a random timeout used for initialization of replicas
    @staticmethod
    def _make_timeout():
        return random.randrange(TIMEOUT_CENTER-TIMEOUT_RANGE, TIMEOUT_CENTER+TIMEOUT_RANGE)/1000
    
    # changes the role of the replica which holds this state
    # in the case of changing to a candidate, it returns RequestVoteRPCs to be broadcast
    def change_role(self, role) -> list[dict]:
        self.role = role
        return self.role.initState(self)

# represents the abstract functionality of a replica (one of Follower, Candidate, Leader)
class Role(ABC):
    def __init__(self):
        raise TypeError('State cannot be instantiated and must be used statically.')
    
    # helper for creating messages to be sent back to clients
    # raises an error if the leader is unknown and the type is not a fail
    @staticmethod
    def _make_client_msg(src: str, dst: str, mid: str, leader_id: str, type: str) -> dict:
        if leader_id == BROADCAST and type != 'fail':
            raise ValueError
        return {'src': src, 'dst': dst, 'leader': leader_id, 'type': type, "MID": mid}

    # helper for executing role-specific timeout actions
    @staticmethod
    def execOnTimeout(state: State) -> list:
        print("Role timeout")
        return []

    # helper for retreiving data from messages
    @staticmethod
    def _parse_msg(msg: dict, keys: list[str]) -> list[str]:
        values = []
        for k in keys:
            try:
                values.append(msg[k])
            except:
                raise ValueError(f"key {k} does not exist in {msg}")

        return values

    # helper for changing relavant state variables when changing state
    # i.e.: if becoming leader, set leader ID to ourselves and change timeout
    @staticmethod
    @abstractmethod
    def initState(state: State) -> list[dict]:
        return []
    
    # routes functionality depending on the type of message given
    # dispatched to a Follower, Candidate, or Leader method to mutate the state
    @staticmethod
    def handle(state: State, msg: dict) -> list[dict]:
        [msg_type] = Role._parse_msg(msg, ["type"])
        match msg_type:
            case 'Append':
                state.last_heartbeat = time.time()
                return state.role.appendEntriesRPC(msg, state)
            case 'RequestVote':
                return state.role.requestVoteRPC(msg, state)
            case 'Vote':
                return state.role.voteReceived(msg, state)
            case 'get':
                return state.role.get(msg, state) 
            case 'put':
                return state.role.put(msg, state)
            case _:
                raise Exception(f"BADDY TYPE: {msg_type}")

    ##########################
    # Message type executors #
    ##########################
    
    # handles a put request from a client
    @staticmethod
    @abstractmethod
    def put(msg: dict, state: State) -> list[dict]:
        return []

    # handles a get request from a client
    @staticmethod
    @abstractmethod
    def get(msg: dict, state: State) -> list[dict]:
        return []

    # handles a message to append entries to the log
    # if leader: throw error should never happen
    # if candidate: cancel election, set leader to leader, then do normal stuff
    # if follower: reset heartbeat timer, do normal appendentry client stuff
    @staticmethod
    def appendEntriesRPC(msg: dict, state: State) -> list[dict]:
        return []

    # responds to a request to vote for a candidate
    # currently, will grant a vote if the replica is 0001 and will deny a vote otherwise
    @staticmethod
    def requestVoteRPC(msg: dict, state: State) -> list[dict]:
        src, dst = Role._parse_msg(msg, ['src', 'dst'])
        state.leader = BROADCAST # leader is unknown
        if (src != '0001'):
            return [{'src': dst, 'dst': src, 'leader': state.leader_id, 'type': 'Vote', 'voteGranted': False}]
        
        state.voted_for = src
        return [{'src': dst, 'dst': src, 'leader': state.leader_id, 'type': 'Vote', 'voteGranted': True}]
    
    # handles a vote response (either granted or denied) as a candidate
    # if leader: this will only happen i nerror cases and when we become the new leader before the vote reaches us
    # if candidate: add vote if granted
    # if follower: throw error??
    @staticmethod
    def voteReceived(msg: dict, state: State) -> list[dict]:
        return []
    
    # commits a log entry for 2pc
    # not used right now
    @staticmethod
    def commit(msg: dict, state: State) -> list[dict]:
        """ 
            def commit(self):
                entry = self.log[self.num_comitted]
                self.data[entry.key] = entry.value
                self.commit_index += 1
        """
        return []

# represents the functionality for a Leader replica
class Leader(Role):
    # assigns state's leader to be own id
    @staticmethod
    def initState(state: State) -> list[dict]:
        state.leader_id = state.id
        state.last_heartbeat = time.time()
        state.timeout_sec = HEARTBEAT_TIME
        return []
    
    # helper for creating AppendEntriesMessages
    # entries is list of key,value pairs for now
    @staticmethod
    def _makeAppendEntriesMessage(id: str, term: int, entries: list, dst: str = BROADCAST) -> dict: 
        # TODO rest of this message
        return {'src': id, 'dst': dst, 'leader': id, 'type': 'Append', 'term': term, 'entries': entries}

    # responds to the client with the given data found by the key
    @staticmethod
    def get(msg: dict, state: State) -> list[dict]:
        src, dst, mid, key = Role._parse_msg(msg, ["src", "dst", "MID", "key"])
        ok_message = Role._make_client_msg(dst, src, mid, state.leader_id, 'ok')
        ok_message["value"] = state.data[key]
        return [ok_message]
    
    # appends the key+value pair to the log, asks followers to append, responds to the client upon success
    @staticmethod
    def put(msg: dict, state: State) -> list[dict]:
        try:
            src, dst, mid, key, value = Role._parse_msg(msg, ['src', 'dst', 'MID', 'key', 'value'])
            #TODO: add to log instead of mutating dict
            state.data[key] = value
            return [
                Role._make_client_msg(dst, src, mid, state.leader_id, 'ok'),
                Leader._makeAppendEntriesMessage(state.id, state.term, [(key, value)])
            ]
        except ValueError:
            return [Role._make_client_msg(dst, src, mid, state.leader_id, 'fail')]

    # if the leader times out, it should immediately send a heartbeat to notify the other replicas
    @staticmethod
    def execOnTimeout(state: State) -> list[dict]:
        state.last_heartbeat = time.time() 
        # TODO optimization: only send heartbeat if TIME SINCE LAST BROADCAST > timeout, this would use the normal 2pc messages as heartbeat messages too
        return [Leader._makeAppendEntriesMessage(state.id, state.term, [])]
    
    # if a candidate replica is requesting the leader's vote, immediately notify with heartbeat to stop election
    @staticmethod
    def requestVoteRPC(ms: dict, state: State) -> list[dict]: # send heartbeat
        return [Leader._makeAppendEntriesMessage(state.id, state.term, [])]

# represents the functionality for a Follower replica
class Follower(Role):
    
    # if timeout, become a candidate to start an election
    @staticmethod
    def execOnTimeout(state: State) -> list[dict]:
        print("Follower timeout")
        state.last_heartbeat = time.time()
        if not state.voting and state.leader_id == BROADCAST: # TODO: this is just for startup
            print('Change state to candidate')
            return state.change_role(Candidate)
        return []

    # redirect client to leader unless leader is unknown
    @staticmethod
    def get(msg: dict, state: State) -> list[dict]:
        if(state.leader_id == BROADCAST):
            return [Role._make_client_msg(dst, src, mid, state.leader_id, 'fail')]
        src, dst, mid, key = Role._parse_msg(msg, ["src", "dst", "MID", "key"])
        redirect_msg = Role._make_client_msg(dst, src, mid, state.leader_id, 'redirect')
        return [redirect_msg]
    
    # redirect client to leader unless leader is unknown
    @staticmethod
    def put(msg: dict, state: State) -> list[dict]:
        return Follower.get(msg, state)
    
    # handles a leader order to append entries to the log
    # updates the state's leader id if necessary
    @staticmethod
    def appendEntriesRPC(msg: dict, state: State) -> list[dict]:
        leader, entries = Role._parse_msg(msg, ['leader', 'entries'])
        state.last_heartbeat = time.time()
        
        if(state.leader_id != leader):
            print(f'Replica {state.id} changing leader to {leader}')
            state.leader_id = leader
            state.voting = False
        
        for key, value in entries: #TODO: append to log instead of mutating datastore
            state.data[key] = value
        
        return []

    # when becoming follower (from leader or candidate), re-randomize timeout and reset voting status
    @staticmethod
    def initState(state: State) -> list[dict]:
        state.timeout_sec = State._make_timeout()
        state.last_heartbeat = time.time()
        state.voting = False
        state.voted_for = None

        return []

# represents the functionality for a Candidate replica
class Candidate(Role):
    # increment term, vote for self, and broadcast RequestVoteRPC to other replicas
    @staticmethod
    def _startElection(state: State) -> list[dict]:
        state.leader_id = BROADCAST
        state.term += 1
        state.voted_for = state.id
        state.votes = 1
        return [{ # no 'vote' field, so this is a request and not a response
            'src': state.id, 
            'dst': BROADCAST, 
            'leader': state.id, # NOTE since DST is broadcast, the replicas need to know who to send their vote back to
            'type': 'RequestVote', 
            'term': state.term, 
            'last_log_index': len(state.log)-1, 
            'last_log_term': state.log[-1].term if state.log else -1,
        }]
    
    # redirect client to leader unless leader is unknown
    @staticmethod
    def get(msg: dict, state: State) -> list[dict]:
        return Follower.get(msg, state)
    
    # redirect client to leader unless leader is unknown
    @staticmethod
    def put(msg: dict, state: State) -> list[dict]:
        return Candidate.get(msg, state)
    
    # tally votes and if quorum is reached then promote self to leader
    @staticmethod
    def voteReceived(msg: dict, state: State) -> list[dict]:
        if(msg['voteGranted']):
            state.votes += 1
            
        # we've reached quorum TODO make this tolerant to replica failures (change in others count)
        if(state.votes > len(state.others)/2): 
            print(f'Replica {state.id} setting state to leader')  
            return state.change_role(Leader)
        
        return []

    # when becoming a candidate (from follower), it needs to start an election
    @staticmethod
    def initState(state: State) -> list[dict]:
        state.timeout_sec = State._make_timeout()
        state.last_heartbeat = time.time()
        return Candidate._startElection(state)
        
    # cancel the election and set leader
    @staticmethod
    def appendEntriesRPC(msg: dict, state: State) -> list[dict]:
        src, leader = Role._parse_msg(msg, ['src', 'leader'])
        
        state.leader_id = leader
        state.voting = False
        
        print('Change role to follower')
        state.change_role(Follower)
        
        return []
        
    @staticmethod
    def execOnTimeout(state: State) -> list[dict]:
        print("Candidate timeout")
        return []