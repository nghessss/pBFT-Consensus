import time
import random
from threading import Thread

ELECTION_TIMEOUT = (3, 5)
HEARTBEAT_INTERVAL = 1


class RaftLogic:
    def __init__(self, node):
        self.node = node

    def start(self):
        Thread(target=self.election_loop, daemon=True).start()

    def election_loop(self):
        while self.node.state.alive:
            if self.node.state.role != "Leader":
                timeout = random.uniform(*ELECTION_TIMEOUT)
                if time.time() - self.node.state.last_heartbeat > timeout:
                    self.start_election()
            time.sleep(0.1)

    def start_election(self):
        state = self.node.state
        state.role = "Candidate"
        state.current_term += 1
        state.voted_for = state.node_id
        votes = 1

        for peer in state.peers:
            try:
                resp = self.node.rpc_clients[peer].request_vote(
                    term=state.current_term,
                    candidate_id=state.node_id
                )
                if resp.vote_granted:
                    votes += 1
            except:
                pass

        if votes > (len(state.peers) + 1) // 2:
            self.become_leader()

    def become_leader(self):
        self.node.state.role = "Leader"
        Thread(target=self.heartbeat_loop, daemon=True).start()

    def heartbeat_loop(self):
        while self.node.state.role == "Leader":
            for peer in self.node.state.peers:
                try:
                    self.node.rpc_clients[peer].append_entries(
                        term=self.node.state.current_term,
                        leader_id=self.node.state.node_id
                    )
                except:
                    pass
            time.sleep(HEARTBEAT_INTERVAL)
