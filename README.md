high-level approach:
`4730kvstore`  contains the `Replica` class (has `run` method) which handles all socket level functionality. 
A replica holds a `State` (in `StateMachine.py`) which contains all info about a machine including its role, log, datastore, id, and other persistent and volatile storage specified in the RAFT protocol.
`StateMachine.py` also has the abstract class `Role` which organizes static RAFT functionality and is extended by the `Leader`, `Candidate`, and `Follower` classes.
The `Replica.run` method continuously calls static `Role` methods by passing in the mutable `State`, and this gets dispatched to one of the subclasses to mutate the state and return messages which `Replica` will send.

the challenges you faced:
As we wrote more code in the single `Replica` class, the length and complexity became difficult to manage and organize. This is why we decided to redesign the program as outlined above.

a list of properties/features of your design that you think is good:

an overview of how you tested your code:
We ran it on several configurations
