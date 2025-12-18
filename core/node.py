import math
from copy import deepcopy
from typing import Dict, List, Optional
from core.message import Message
from core.utils import random_uniform

class RaftNode:
    FOLLOWER = "Follower"
    CANDIDATE = "Candidate"
    LEADER = "Leader"
    STOPPED = "Stopped"

    def __init__(self, node_id: int, peers: List[int], initial_time: float = 0.0):
        self.id = node_id
        self.peers = list(peers)

        # Raft state
        self.state: str = RaftNode.FOLLOWER
        self.current_term: int = 1
        self.voted_for: Optional[int] = None
        self.log: List[Dict] = []            # entries: {'term': t, 'value': v}
        self.commit_index: int = 0

        # Timing + RPC bookkeeping (keys indexed by peer id)
        self.election_timeout: float = self._make_election_timeout(initial_time)
        self.vote_granted: Dict[int, bool] = {p: False for p in self.peers}
        self.match_index: Dict[int, int] = {p: 0 for p in self.peers}
        self.next_index: Dict[int, int] = {p: 1 for p in self.peers}
        self.rpc_due: Dict[int, float] = {p: 0 for p in self.peers}
        self.heartbeat_due: Dict[int, float] = {p: 0 for p in self.peers}

        self.alive: bool = True

    # -------------------------
    # Log handling
    # -------------------------
    def last_log_index(self) -> int:
        return len(self.log)

    def last_log_term(self) -> int:
        if self.log:
            return self.log[-1]["term"]
        return 0

    def append_entry(self, term: int, value):
        self.log.append({"term": term, "value": value})

    def truncate_after(self, index_one_based: int):
        # keep up to index_one_based - 1
        while len(self.log) > index_one_based - 1:
            self.log.pop()

    # -------------------------
    # State handling
    # -------------------------
    def stop(self):
        self.state = RaftNode.STOPPED
        self.election_timeout = 0
        self.alive = False

    def resume(self, now_time: float):
        self.state = RaftNode.FOLLOWER
        self.election_timeout = self._make_election_timeout(now_time)
        self.alive = True

    # -------------------------
    # Internal utility
    # -------------------------
    @staticmethod
    def _make_election_timeout(now: float, base_timeout: float = 1.0):
        factor = 0.5 + 5 * random_uniform()
        return now + factor * base_timeout


    def to_dict(self) -> Dict:
        return {
            "id": self.id,
            "peers": list(self.peers),
            "state": self.state,
            "term": self.current_term,
            "votedFor": self.voted_for,
            "log": deepcopy(self.log),
            "commitIndex": self.commit_index,
            "electionAlarm": self.election_timeout,
            "voteGranted": dict(self.vote_granted),
            "matchIndex": dict(self.match_index),
            "nextIndex": dict(self.next_index),
            "rpcDue": dict(self.rpc_due),
            "heartbeatDue": dict(self.heartbeat_due),
            "alive": self.alive,
        }

    def __repr__(self):
        return f"<Node {self.id} {self.state} t={self.current_term} log={len(self.log)}>"
