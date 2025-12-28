import grpc
from rpc import raft_pb2, raft_pb2_grpc


class PBFTClient:
    def __init__(self, addr):
        channel = grpc.insecure_channel(addr)
        self.stub = raft_pb2_grpc.PBFTServiceStub(channel)
        
    def ping(self, timeout=1.0):
        try:
            res = self.stub.Ping(
                raft_pb2.PingRequest(),
                timeout=timeout
            )
            return res.message
        except Exception as e:
            return f"ERROR: {e}"

    def client_request(self, req: raft_pb2.ClientRequest, timeout=30.0):
        return self.stub.SubmitRequest(req, timeout=timeout)

    def pre_prepare(self, req: raft_pb2.PrePrepareRequest, timeout=5.0):
        return self.stub.PrePrepare(req, timeout=timeout)

    def prepare(self, req: raft_pb2.PrepareRequest, timeout=5.0):
        return self.stub.Prepare(req, timeout=timeout)

    def commit(self, req: raft_pb2.CommitRequest, timeout=5.0):
        return self.stub.Commit(req, timeout=timeout)

    def get_status(self):
        return self.stub.GetStatus(raft_pb2.StatusRequest())

    def kill(self):
        return self.stub.KillNode(raft_pb2.Empty())
