# core/cluster.py
import random
import math
import threading
import time
from copy import deepcopy
from typing import Dict, List, Optional
from core.node import RaftNode
from core.message import Message
from core.utils import log_term

# Tunable constants (kept as in earlier version but scaled)
RPC_TIMEOUT = 50000
MIN_RPC_LATENCY = 10000
MAX_RPC_LATENCY = 15000
ELECTION_TIMEOUT = 100000
BATCH_SIZE = 1

class Cluster:
    def __init__(self, num_nodes: int = 10):
        self.nodes = []
        self.time = 0.0                         # logical time
        self._running = False
        self._thread = None
        self._lock = threading.Lock()

        # real-time sync variables
        self._real_start = None
        self._logical_start = None
        self.speed = 1.0                        # 1.0 = real-time speed

        self.messages = []
        self.events = []
        
        self._create_nodes(num_nodes)

    def _create_nodes(self, n):
        self.nodes = []
        for i in range(n):
            peers = [p for p in range(n) if p != i]
            self.nodes.append(RaftNode(node_id=i, peers=peers, initial_time=self.time))
   
    def get(self):
        """Return a snapshot of the entire cluster state for UI."""
        return {
            "time": self.time,
            "leader": self.get_leader(),
            "nodes": {
                node_id: {
                    "state": node.state,
                    "current_term": node.current_term,
                    "voted_for": node.voted_for,
                    "log": node.log,
                    "commit_index": node.commit_index,
                    "last_applied": node.last_applied,
                }
                for node_id, node in self.nodes.items()
            }
        }
        
    def start_all(self):
        if self._running:
            return

        print("[Cluster] START")

        # mark start time
        self._real_start = time.time()
        self._logical_start = self.time      # usually = 0 at first start

        self._running = True

        def loop():
            while self._running:
                with self._lock:
                    self.tick()
                time.sleep(0.03)             # tick 30–35 times/s for smooth animation

        self._thread = threading.Thread(target=loop, daemon=True)
        self._thread.start()

    def stop_all(self):
        print("[Cluster] STOP")
        self._running = False

    def is_running(self):
        return self._running

    # -----------------------
    # Random/time helpers
    # -----------------------
    def now(self) -> float:
        return self.time

    def advance_time(self, delta: float):
        self.time += delta

    def randint_latency(self) -> float:
        return MIN_RPC_LATENCY + random.random() * (MAX_RPC_LATENCY - MIN_RPC_LATENCY)

    def rand_election_timeout(self) -> float:
        return self.time + (random.random() + 1) * ELECTION_TIMEOUT

    def _compute_cluster_time(self):
        """Return the logical time synchronized to real time."""
        real_dt = time.time() - self._real_start
        return self._logical_start + real_dt * self.speed
    
    # -----------------------
    # Message bus
    # -----------------------
    def send_message(self, msg: Message):
        m = deepcopy(msg)
        m.send_time = self.time
        m.recv_time = self.time + self.randint_latency()
        self.messages.append(m)

    def send_request(self, msg: Message):
        msg.direction = "request"
        self.send_message(msg)

    def send_reply(self, req: Message, reply_payload: dict):
        rep = req.to_reply(reply_payload)
        self.send_message(rep)

    # -----------------------
    # High-level RPC senders used by leader/candidate logic
    # -----------------------
    def broadcast_request_vote(self, candidate: RaftNode):
        for p in candidate.peers:
            req = Message(
                type="RequestVote",
                term=candidate.current_term,
                from_id=candidate.id,
                to_id=p,
                payload={
                    "lastLogTerm": candidate.last_log_term(),
                    "lastLogIndex": candidate.last_log_index(),
                }
            )
            self.send_request(req)
            candidate.rpc_due[p] = self.time + RPC_TIMEOUT

    def send_append_entries_to(self, leader: RaftNode, peer_id: int):
        prev_index = leader.next_index[peer_id] - 1
        prev_term = leader.log[prev_index - 1]["term"] if prev_index >= 1 and prev_index <= len(leader.log) else 0
        last_index = min(prev_index + BATCH_SIZE, len(leader.log))
        if leader.match_index[peer_id] + 1 < leader.next_index[peer_id]:
            # follower lagging badly -> send empty prev-only to backtrack
            last_index = prev_index
        entries = deepcopy(leader.log[prev_index:last_index]) if last_index > prev_index else []
        req = Message(
            type="AppendEntries",
            term=leader.current_term,
            sender=leader.id,
            receiver=peer_id,
            payload={
                "prevIndex": prev_index,
                "prevTerm": prev_term,
                "entries": entries,
                "commitIndex": min(leader.commit_index, last_index),
            }
        )
        self.send_request(req)
        leader.rpc_due[peer_id] = self.time + RPC_TIMEOUT
        leader.heartbeat_due[peer_id] = self.time + (ELECTION_TIMEOUT / 2)

    # -----------------------
    # Core rules 
    # -----------------------
    def start_new_election_if_due(self, node: RaftNode):
        if node.state in (RaftNode.FOLLOWER, RaftNode.CANDIDATE) and node.election_timeout <= self.time:
            node.current_term += 1
            node.state = RaftNode.CANDIDATE
            node.voted_for = node.id
            node.election_timeout = self.rand_election_timeout()
            node.vote_granted = {p: False for p in node.peers}
            node.match_index = {p: 0 for p in node.peers}
            node.next_index = {p: 1 for p in node.peers}
            node.rpc_due = {p: 0 for p in node.peers}
            node.heartbeat_due = {p: 0 for p in node.peers}
            self.events.append(f"[t={self.time}] Node {node.id} starts election term {node.current_term}")
            # broadcast
            self.broadcast_request_vote(node)

    def try_become_leader(self, node: RaftNode):
        if node.state != RaftNode.CANDIDATE:
            return
        votes = sum(1 for v in node.vote_granted.values() if v) + 1
        if votes > len(self.nodes) // 2:
            node.state = RaftNode.LEADER
            node.next_index = {p: node.last_log_index() + 1 for p in node.peers}
            node.rpc_due = {p: math.inf for p in node.peers}
            node.heartbeat_due = {p: 0 for p in node.peers}
            node.election_timeout = math.inf
            self.events.append(f"[t={self.time}] Node {node.id} becomes LEADER (term {node.current_term})")
            # send immediate heartbeats
            for p in node.peers:
                self.send_append_entries_to(node, p)

    def advance_commit_index(self, leader: RaftNode):
        if leader.state != RaftNode.LEADER:
            return
        match_list = list(leader.match_index.values()) + [leader.last_log_index()]
        match_list.sort()
        n = match_list[len(match_list)//2]
        if n >= 1 and (leader.log[n-1]["term"] == leader.current_term):
            leader.commit_index = max(leader.commit_index, n)

    # -----------------------
    # Message delivery (tick)
    # -----------------------
    def deliver_messages(self):
        deliver_now = [m for m in self.messages if m.recv_time <= self.time]
        self.messages = [m for m in self.messages if m.recv_time > self.time]
        for m in deliver_now:
            self._deliver(m)

    def _deliver(self, m: Message):
        # find receiver node and call handler
        node = self.nodes[m.receiver]
        if not node.alive or node.state == RaftNode.STOPPED:
            return
        # Route by type/direction
        if m.type == "RequestVote":
            if m.direction == "request":
                self._handle_request_vote_request(node, m)
            else:
                self._handle_request_vote_reply(node, m)
        elif m.type == "AppendEntries":
            if m.direction == "request":
                self._handle_append_entries_request(node, m)
            else:
                self._handle_append_entries_reply(node, m)

    # -----------------------
    # RPC Handlers 
    # -----------------------
    def _handle_request_vote_request(self, node: RaftNode, req: Message):
        # step down if term higher
        if node.current_term < req.term:
            node.current_term = req.term
            node.state = RaftNode.FOLLOWER
            node.voted_for = None

        granted = False
        if node.current_term == req.term:
            can_vote = (node.voted_for is None or node.voted_for == req.sender)
            our_term = node.last_log_term()
            our_index = node.last_log_index()
            rlterm = req.payload.get("lastLogTerm", 0)
            rlindex = req.payload.get("lastLogIndex", 0)
            up_to_date = (rlterm > our_term) or (rlterm == our_term and rlindex >= our_index)
            if can_vote and up_to_date:
                granted = True
                node.voted_for = req.sender
                node.election_timeout = self.rand_election_timeout()

        rev = req.to_reply({"term": node.current_term, "granted": granted})
        self.send_message(rev)

    def _handle_request_vote_reply(self, node: RaftNode, reply: Message):
        # This is processed by the candidate who receives replies
        # If reply wins majority, become leader via try_become_leader invoked elsewhere
        if node.current_term < reply.term:
            node.current_term = reply.term
            node.state = RaftNode.FOLLOWER
            node.voted_for = None
            return
        if node.state == RaftNode.CANDIDATE and node.current_term == reply.term:
            node.rpc_due[reply.sender] = math.inf
            node.vote_granted[reply.sender] = reply.payload.get("granted", False)

    def _handle_append_entries_request(self, node: RaftNode, req: Message):
        success = False
        match_index = 0
        if node.current_term < req.term:
            node.current_term = req.term
            node.state = RaftNode.FOLLOWER
            node.voted_for = None

        if node.current_term == req.term:
            node.state = RaftNode.FOLLOWER
            node.election_timeout = self.rand_election_timeout()
            prev_index = req.payload.get("prevIndex", 0)
            prev_term = req.payload.get("prevTerm", 0)
            ok = (prev_index == 0) or (prev_index <= len(node.log) and log_term(node.log, prev_index) == prev_term)
            if ok:
                success = True
                idx = prev_index
                for e in req.payload.get("entries", []):
                    idx += 1
                    if log_term(node.log, idx) != e.get("term", 0):
                        while len(node.log) > idx - 1:
                            node.log.pop()
                        node.log.append({"term": e.get("term", 0), "value": e.get("value")})
                match_index = idx
                node.commit_index = max(node.commit_index, req.payload.get("commitIndex", 0))
        # reply
        rep = req.to_reply({"term": node.current_term, "success": success, "matchIndex": match_index})
        self.send_message(rep)

    def _handle_append_entries_reply(self, node: RaftNode, reply: Message):
        # reply is sent to leader; here 'node' is the leader who receives reply
        if node.current_term < reply.term:
            node.current_term = reply.term
            node.state = RaftNode.FOLLOWER
            node.voted_for = None
            return

        if node.state == RaftNode.LEADER and node.current_term == reply.term:
            if reply.payload.get("success", False):
                fm = reply.sender
                node.match_index[fm] = max(node.match_index[fm], reply.payload.get("matchIndex", 0))
                node.next_index[fm] = reply.payload.get("matchIndex", 0) + 1
            else:
                fm = reply.sender
                node.next_index[fm] = max(1, node.next_index[fm] - 1)
            node.rpc_due[reply.sender] = 0

    # -----------------------
    # Tick / update
    # -----------------------
    def tick(self, dt: float = 1.0):
        """
        Advance cluster time by dt (arbitrary units). Each tick:
        - advance time
        - run per-node rules (start election, try become leader, send append entries)
        - deliver messages whose recv_time <= now
        - advance commit index
        """
        self.time += dt
        
        # per-node rules
        for node in self.nodes:
            if not node.alive or node.state == RaftNode.STOPPED:
                continue
            # start election if due
            if node.state in (RaftNode.FOLLOWER, RaftNode.CANDIDATE) and node.election_timeout <= self.time:
                # start election
                node.current_term += 1
                node.state = RaftNode.CANDIDATE
                node.voted_for = node.id
                node.election_timeout = self.rand_election_timeout()
                node.vote_granted = {p: False for p in node.peers}
                node.match_index = {p: 0 for p in node.peers}
                node.next_index = {p: 1 for p in node.peers}
                node.rpc_due = {p: 0 for p in node.peers}
                node.heartbeat_due = {p: 0 for p in node.peers}
                self.events.append(f"[t={self.time}] Node {node.id} starts election term {node.current_term}")
                self.broadcast_request_vote(node)

        # attempt become leader and send append entries
        for node in self.nodes:
            if node.state == RaftNode.CANDIDATE:
                self.try_become_leader(node)
            if node.state == RaftNode.LEADER:
                # send append entries to peers if due
                for p in node.peers:
                    if node.heartbeat_due[p] <= self.time or (node.next_index[p] <= len(node.log) and node.rpc_due[p] <= self.time):
                        self.send_append_entries_to(node, p)
                self.advance_commit_index(node)

        # deliver messages
        self.deliver_messages()

    # -----------------------
    # Utilities: crash/restart/client
    # -----------------------
    def crash_node(self, node_id: int):
        node = self.nodes[node_id]
        node.stop()
        self.events.append(f"[t={self.time}] Node {node_id} crashed")

    def restart_node(self, node_id: int):
        node = self.nodes[node_id]
        node.resume(self.time)
        self.events.append(f"[t={self.time}] Node {node_id} restarted")

    def client_append(self, leader_id: int, value):
        leader = self.nodes[leader_id]
        if leader.state != RaftNode.LEADER:
            raise RuntimeError("Not leader")
        leader.append_entry(leader.current_term, value)
        # try replicate immediately
        for p in leader.peers:
            self.send_append_entries_to(leader, p)

    # -----------------------
    # Helpers
    # -----------------------
    def get_leader(self) -> Optional[int]:
        for n in self.nodes:
            if n.state == RaftNode.LEADER and n.alive:
                return n.id
        return None

    def get_events(self, n: int = 100):
        return self.events[-n:]
    
    def is_running(self):
        return self._running
