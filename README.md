high-level approach:

`4730kvstore`  contains the `Replica` class (has `run` method) which handles all socket level functionality. 
A replica holds a `State` (in `StateMachine.py`) which contains all info about a machine including its role, log, datastore, id, and other persistent and volatile storage specified in the RAFT protocol.
`StateMachine.py` also has the abstract class `Role` which organizes RAFT functionality and is extended by the `Leader`, `Candidate`, and `Follower` classes.
The `Replica.run` method continuously calls static `Role` methods by passing in the mutable `State`, and this gets dispatched to one of the subclasses (depending on the current role in the state) to mutate the state, and finally return a list of messages which `Replica` will send.


the challenges you faced:

As we wrote more code in the single `Replica` class, the length and complexity became difficult to manage and organize. This is why we decided to redesign the program as outlined above.


a list of properties/features of your design that you think is good:

The state of a machine is encapsulated in a single `State` object which enables it to easily be passed around and mutated when dispatching to various roles.
As a result of dispatching to `Role` in the `Replica` class, the `Replica` implementation remains a relatively simple with barely any RAFT protocol logic.
The subclassing of `Role` with `Leader`, `Candidate`, and `Follower` classes greatly improves organization by dispatching accordingly instead of relying on multi branch conditionals within each client and RAFT method.

Our leader implementation keeps track of which followers have timed out (not sent back an ack within a timeframe) in response to an AppendEntriesRPC. After each timeout, the leader will try to resend the RPC. If after 3 timeouts the follower has not acked, the leader will assume that the follower has sent back a fail. This way, the leader is more resilient to dropped packets and can commit entries more robustly.

The solution also includes the feature that when a new leader is elected and commits an entry of its term, it also commits every pending (uncommitted) entry before it.

an overview of how you tested your code:

We ran it on all of the configurations
Changed some configs (e.g. drop out rate) to first see it they would succeed on easier versions of the advanced versions.