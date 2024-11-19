from abc import ABC, abstractmethod
import select, json, time, socket, random
from collections import defaultdict, namedtuple

BROADCAST = "FFFF"
RECV_WAIT = 0.1
SEND_WAIT = 0.01
TIMEOUT_CENTER = 250 # ms 
TIMEOUT_RANGE = 50 # ms
HEARTBEAT_TIME = 0.15

# Message types between replicas:
# 

Entry = namedtuple('Entry', ['term', 'key', 'value'])

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

    @staticmethod
    def _make_timeout():
        return random.randrange(TIMEOUT_CENTER-TIMEOUT_RANGE, TIMEOUT_CENTER+TIMEOUT_RANGE)/1000
    
    def change_role(self, role):
        self.role = role
        return self.role.initState(self)

class Role(ABC):
    def __init__(self):
        raise TypeError('State cannot be instantiated and must be used statically.')
    
    @staticmethod
    def _make_client_msg(src, dst, mid, leader_id, type):
        if leader_id == BROADCAST and type != 'fail':
            raise ValueError
        return {'src': src, 'dst': dst, 'leader': leader_id, 'type': type, "MID": mid}

    @staticmethod
    def execOnTimeout(state: State):
        print("Role timeout")
        return []

    @staticmethod
    def _parse_msg(msg, keys):
        values = []
        for k in keys:
            try:
                values.append(msg[k])
            except:
                raise ValueError(f"key {k} does not exist in {msg}")

        return values

    @staticmethod
    @abstractmethod
    def initState(state):
        # change relavant state variables when changing state
        # i.e.: if becoming leader, set leader ID to ourselves and change timeout
        return []

    @staticmethod
    def handle(state, msg) -> dict:
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
    @staticmethod
    def put(msg, state): # from client
        # redirect to leader
        # if leader unknown (leader_id='FFFF') return fail
        return []

    @staticmethod
    @abstractmethod
    def get(msg, state): # from client
        # redirect to leader
        return []

    @staticmethod
    def appendEntriesRPC(msg, state):
        # if leader: throw error should never happen
        # if candidate: cancel election, set leader to leader, then do normal stuff
        # if follower: reset heartbeat timer, do normal appendentry client stuff
        return []

    @staticmethod
    def requestVoteRPC(msg, state: State):
        src, dst = Role._parse_msg(msg, ['src', 'dst'])
        state.leader = 'FFFF' # leader is unknown
        if (src != '0001'):
            return [{'src': dst, 'dst': src, 'leader': state.leader_id, 'type': 'Vote', 'voteGranted': False}]
        
        state.voted_for = src
        return [{'src': dst, 'dst': src, 'leader': state.leader_id, 'type': 'Vote', 'voteGranted': True}]
    
    @staticmethod
    def voteReceived(msg, state):
        # if leader: this will only happen i nerror cases and when we become the new leader before the vote reaches us
        # if candidate: add vote if granted
        # if follower: throw error??
        return []
    
    @staticmethod
    def commit(msg, state):
        # not used rn, will be used for 2pc
        return []

class Leader(Role):
    @staticmethod
    def initState(state: State):
        state.leader_id = state.id
        state.last_heartbeat = time.time()
        state.timeout_sec = HEARTBEAT_TIME
        return []
        
    @staticmethod
    def _makeAppendEntriesMessage(id, term, entries, dst='FFFF'): # entries is list of key,value pairs for now
        # TODO rest of this message
        return {'src': id, 'dst': dst, 'leader': id, 'type': 'Append', 'term': term, 'entries': entries}

    @staticmethod
    def put(msg, state: State):
        src, dst, mid, key, value = Role._parse_msg(msg, ['src', 'dst', 'MID', 'key', 'value'])
        state.data[key] = value
        return [{'src': dst, 'dst': src, 'leader': state.id, 'MID': mid, 'type': 'ok', 'value': key}]

    @staticmethod
    def get(msg, state):
        # do get stuff because we are the leader
        src, dst, mid, key = Role._parse_msg(msg, ["src", "dst", "MID", "key"])
        ok_message = Role._make_client_msg(dst, src, mid, state.leader_id, 'ok')
        ok_message["value"] = state.data[key]
        return [ok_message]
    
    @staticmethod
    def put(msg, state):
        # do get stuff because we are the leader
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

    @staticmethod
    def execOnTimeout(state: State):
        state.last_heartbeat = time.time() 
        # TODO optimization: only send heartbeat if TIME SINCE LAST BROADCAST > timeout, this would use the normal 2pc messages as heartbeat messages too
        return [Leader._makeAppendEntriesMessage(state.id, state.term, [])]
    
    @staticmethod
    def requestVoteRPC(msg, state: State): # send heartbeat
        return [Leader._makeAppendEntriesMessage(state.id, state.term, [])]

class Follower(Role):
    @staticmethod
    def execOnTimeout(state: State):
        print("Follower timeout")
        state.last_heartbeat = time.time()
        if not state.voting and state.leader_id == 'FFFF': # TODO: this is just for startup
            print('Change state to candidate')
            return state.change_role(Candidate)
        return []

    @staticmethod
    def get(msg, state):
        if(state.leader_id == 'FFFF'):
            return [Role._make_client_msg(dst, src, mid, state.leader_id, 'fail')]
        src, dst, mid, key = Role._parse_msg(msg, ["src", "dst", "MID", "key"])
        redirect_msg = Role._make_client_msg(dst, src, mid, state.leader_id, 'redirect')
        return [redirect_msg]
    
    @staticmethod
    def put(msg, state):
        return Follower.get(msg, state)
        
    @staticmethod
    def appendEntriesRPC(msg, state: State):
        leader, entries = Role._parse_msg(msg, ['leader', 'entries'])
        state.last_heartbeat = time.time()
        
        if(state.leader_id != leader):
            print(f'Replica {state.id} changing leader to {leader}')
            state.leader_id = leader
            state.voting = False
        
        for key, value in entries:
            state.data[key] = value
        
        return []

    @staticmethod
    def initState(state: State):
        state.timeout_sec = State._make_timeout()
        state.last_heartbeat = time.time()
        state.voting = False
        state.voted_for = None

        return []

class Candidate(Role):
    @staticmethod
    def _startElection(state: State):
        state.leader_id = 'FFFF'
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
        
    @staticmethod
    def get(msg, state):
        src, dst, mid= Role._parse_msg(msg, ["src", "dst", "MID", "key"])
        return [Role._make_client_msg(dst, src, mid, state.leader_id, 'fail')]

    @staticmethod
    def put(msg, state):
        return Candidate.get(msg, state)
        
    @staticmethod
    def voteReceived(msg, state: State):
        if(msg['voteGranted']):
            state.votes += 1
            
        # we've reached quorum TODO make this tolerant to replica failures (change in others count)
        if(state.votes > len(state.others)/2): 
            print(f'Replica {state.id} setting state to leader')  
            return state.change_role(Leader)
        
        return []

    @staticmethod
    def initState(state):
        state.timeout_sec = State._make_timeout()
        state.last_heartbeat = time.time()
        return Candidate._startElection(state)
        
    # cancel the election and set leader
    @staticmethod
    def appendEntriesRPC(msg, state: State):
        src, leader = Role._parse_msg(msg, ['src', 'leader'])
        
        state.leader_id = leader
        state.voting = False
        
        print('Change role to follower')
        state.change_role(Follower)
        
        return []
        
    @staticmethod
    def execOnTimeout(state: State):
        print("Candidate timeout")
        return []

""" 
    #????
    def commit(self):
        entry = self.log[self.num_comitted]
        self.data[entry.key] = entry.value
        self.commit_index += 1
"""