import time
import random
from threading import Thread
import os

ELECTION_TIMEOUT = (5, 10)
HEARTBEAT_INTERVAL = 1

def set_title(title):
    os.system(f'title "{title}"')

    
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

        set_title(
            f"RAFT Node {state.node_id} | CANDIDATE | term={state.current_term}"
        )
        print(f"RAFT Node {state.node_id} | CANDIDATE | term={state.current_term}")
        
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
        set_title(
            f"RAFT Node {self.node.state.node_id} | LEADER"
        )
        print(f"RAFT Node {self.node.state.node_id} | LEADER")
    
        Thread(target=self.heartbeat_loop, daemon=True).start()

    def heartbeat_loop(self):
        while self.node.state.role == "Leader":
            for peer in self.node.state.peers:
                try:
                    print(f"Sent heartbeat to {peer}")
                    self.node.rpc_clients[peer].append_entries(
                        term=self.node.state.current_term,
                        leader_id=self.node.state.node_id
                    )
                except:
                    pass
            time.sleep(HEARTBEAT_INTERVAL)
