import grpc
from rpc import raft_pb2, raft_pb2_grpc


class RaftRPCClient:
    def __init__(self, addr):
        channel = grpc.insecure_channel(addr)
        self.stub = raft_pb2_grpc.RaftServiceStub(channel)
        
    def ping(self, timeout=1.0):
        try:
            res = self.stub.Ping(
                raft_pb2.PingRequest(),
                timeout=timeout
            )
            return res.message
        except Exception as e:
            return f"ERROR: {e}"

    def request_vote(self, **kw):
        return self.stub.RequestVote(raft_pb2.RequestVoteRequest(**kw))

    def append_entries(self, **kw):
        return self.stub.AppendEntries(raft_pb2.AppendEntriesRequest(**kw))

    def get_status(self):
        return self.stub.GetStatus(raft_pb2.StatusRequest())

    def kill(self):
        return self.stub.KillNode(raft_pb2.Empty())
