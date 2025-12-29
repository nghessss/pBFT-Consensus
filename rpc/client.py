import grpc
from rpc import pbft_pb2, pbft_pb2_grpc


class PBFTClient:
    def __init__(self, addr):
        channel = grpc.insecure_channel(addr)
        self.stub = pbft_pb2_grpc.PBFTServiceStub(channel)
        
    def ping(self, timeout=1.0):
        try:
            res = self.stub.Ping(
                pbft_pb2.PingRequest(),
                timeout=timeout
            )
            return res.message
        except Exception as e:
            return f"ERROR: {e}"

    def client_request(self, req: pbft_pb2.ClientRequest, timeout=30.0):
        return self.stub.SubmitRequest(req, timeout=timeout)

    def pre_prepare(self, req: pbft_pb2.PrePrepareRequest, timeout=5.0):
        return self.stub.PrePrepare(req, timeout=timeout)

    def prepare(self, req: pbft_pb2.PrepareRequest, timeout=5.0):
        return self.stub.Prepare(req, timeout=timeout)

    def commit(self, req: pbft_pb2.CommitRequest, timeout=5.0):
        return self.stub.Commit(req, timeout=timeout)

    def set_view(self, req: pbft_pb2.SetViewRequest, timeout=2.0):
        return self.stub.SetView(req, timeout=timeout)

    def get_status(self):
        return self.stub.GetStatus(pbft_pb2.StatusRequest())

    def kill(self):
        return self.stub.KillNode(pbft_pb2.Empty())
