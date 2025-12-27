import time

class RaftState:
    def __init__(self, node_id, peers):
        # Persistent
        self.current_term = 0
        self.voted_for = None
        self.log = []

        # Volatile
        self.commit_index = -1
        self.last_applied = -1

        # Metadata
        self.node_id = node_id
        self.peers = peers
        self.role = "Follower"
        self.alive = True

        # Timing
        self.last_heartbeat = time.time()
