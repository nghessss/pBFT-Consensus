import grpc
import time
from concurrent import futures
from rpc import raft_pb2, raft_pb2_grpc


class RaftRPCServer(raft_pb2_grpc.RaftServiceServicer):
    def __init__(self, node):
        self.node = node

    def RequestVote(self, req, ctx):
        return self.node.on_request_vote(req)

    def AppendEntries(self, req, ctx):
        return self.node.on_append_entries(req)

    def GetState(self, req, ctx):
        s = self.node.state
        return raft_pb2.NodeState(
            node_id=s.node_id,
            term=s.current_term,
            role=s.role,
            alive=s.alive
        )

    def KillNode(self, req, ctx):
        self.node.state.alive = False
        return raft_pb2.Empty()
    
    def Ping(self, request, context):
        print(f"[RPC] Node {self.node.state.node_id} received Ping!")
        return raft_pb2.PingReply(
            message=f"Node {self.node.state.node_id} alive"
        )


def serve(node, port):
    server = grpc.server(futures.ThreadPoolExecutor(10))
    raft_pb2_grpc.add_RaftServiceServicer_to_server(
        RaftRPCServer(node), server
    )
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    print(f"[RPC] Node {node.state.node_id} on {port}")

    server.wait_for_termination()
