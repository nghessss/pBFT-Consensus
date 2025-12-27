from core.state import RaftState
from core.raft import RaftLogic
from rpc import raft_pb2


class RaftNode:
    def __init__(self, node_id, peers, rpc_clients):
        self.state = RaftState(node_id, peers)
        self.rpc_clients = rpc_clients
        self.logic = RaftLogic(self)

    def start(self):
        self.logic.start()

    # RPC HANDLERS
    def on_request_vote(self, req):
        if req.term < self.state.current_term:
            return raft_pb2.RequestVoteResponse(
                term=self.state.current_term,
                vote_granted=False
            )

        self.state.current_term = req.term
        self.state.voted_for = req.candidate_id
        self.state.last_heartbeat = time.time()

        return raft_pb2.RequestVoteResponse(
            term=self.state.current_term,
            vote_granted=True
        )

    def on_append_entries(self, req):
        if req.term < self.state.current_term:
            return raft_pb2.AppendEntriesResponse(
                term=self.state.current_term,
                success=False
            )

        self.state.current_term = req.term
        self.state.role = "Follower"
        self.state.last_heartbeat = time.time()

        return raft_pb2.AppendEntriesResponse(
            term=self.state.current_term,
            success=True
        )
