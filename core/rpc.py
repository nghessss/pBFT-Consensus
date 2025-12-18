import random
from typing import List
from core.message import Message
from copy import deepcopy

MIN_RPC_LATENCY = 10000
MAX_RPC_LATENCY = 15000
RPC_TIMEOUT = 50000

class RpcLayer:
    """
    Simple message bus that schedules messages by recv_time relative to cluster.time.
    """

    def __init__(self, cluster):
        self.cluster = cluster
        self.inflight: List[Message] = []

    def send(self, msg: Message):
        m = deepcopy(msg)
        m.send_time = self.cluster.time
        latency = MIN_RPC_LATENCY + random.random() * (MAX_RPC_LATENCY - MIN_RPC_LATENCY)
        m.recv_time = self.cluster.time + latency
        self.inflight.append(m)

    def broadcast(self, frm: int, to_ids: List[int], typ: str, term: int, payload=None):
        payload = payload or {}
        for tid in to_ids:
            m = Message(type=typ, term=term, frm=frm, to=tid, payload=payload.copy(), direction="request")
            self.send(m)

    def deliver_ready(self):
        """Return list of messages ready to deliver and remove them from inflight."""
        ready = [m for m in self.inflight if m.recv_time <= self.cluster.time]
        self.inflight = [m for m in self.inflight if m.recv_time > self.cluster.time]
        return ready

    def drop(self, message: Message):
        self.inflight = [m for m in self.inflight if m is not message]

    def clear(self):
        self.inflight.clear()
