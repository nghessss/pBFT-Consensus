# core/pbft_node.py
import hashlib
import random
from typing import Dict, List, Optional, Any
from core.pbft_message import (
    Request, PrePrepare, Prepare, Commit, Reply, 
    Checkpoint, ViewChange, NewView, MessageLog
)

class PBFTNode:
    """
    A replica in the pBFT protocol.
    
    States:
    - NORMAL: Normal operation (3-phase protocol)
    - VIEW_CHANGING: Performing view change
    - STOPPED: Node is crashed/offline
    """
    
    NORMAL = "Normal"
    VIEW_CHANGING = "ViewChanging"
    STOPPED = "Stopped"
    
    # Checkpoint interval
    CHECKPOINT_INTERVAL = 10
    
    def __init__(self, node_id: int, total_nodes: int, initial_time: float = 0.0):
        self.id = node_id
        self.total_nodes = total_nodes
        self.f = (total_nodes - 1) // 3  # Maximum faulty nodes
        
        # Protocol state
        self.view = 0
        self.state = PBFTNode.NORMAL
        self.sequence_number = 0
        self.last_executed = 0
        
        # Water marks for sequence number window
        self.low_water_mark = 0
        self.high_water_mark = 256
        
        # Message log
        self.log = MessageLog()
        
        # Application state (simple key-value store for demo)
        self.app_state: Dict[str, Any] = {}
        
        # Client replies cache
        self.last_replies: Dict[int, Reply] = {}  # client_id -> Reply
        
        # View change
        self.view_change_timeout = 150000  # Base timeout for view change (match cluster setting)
        self.view_change_timer = initial_time + self.view_change_timeout * (1.0 + 0.5 * random.uniform(0, 1))  # Randomized
        self.view_changes: Dict[int, List[ViewChange]] = {}  # view -> [ViewChange]
        self.last_heard_from_primary = initial_time  # Track when last heard from primary
        
        # Tracking prepared requests (for view change)
        self.prepared_requests: Dict[int, Dict] = {}  # seq -> {view, digest, request}
        
        # Checkpoints
        self.stable_checkpoint = 0
        self.stable_checkpoint_proof: List[Checkpoint] = []
        
        # Node status
        self.alive = True
        self.is_byzantine = False  # For simulation: mark as Byzantine faulty
        
    def is_primary(self) -> bool:
        """Check if this node is the primary for current view"""
        return self.id == (self.view % self.total_nodes)
    
    def get_primary_id(self) -> int:
        """Get the primary node ID for current view"""
        return self.view % self.total_nodes
    
    def compute_state_digest(self) -> str:
        """Compute digest of current application state"""
        state_str = json.dumps(self.app_state, sort_keys=True)
        return hashlib.sha256(state_str.encode()).hexdigest()[:16]
    
    def in_water_marks(self, sequence: int) -> bool:
        """Check if sequence number is within water marks"""
        return self.low_water_mark < sequence <= self.high_water_mark
    
    # -------------------------
    # Predicate checks for pBFT protocol
    # -------------------------
    
    def is_prepared(self, sequence: int, view: int, digest: str) -> bool:
        """
        Check if prepared(m, v, n, i) is true:
        - Has pre-prepare for (v, n, d)
        - Has 2f matching prepares from different replicas
        """
        # Check pre-prepare
        if sequence not in self.log.pre_prepares:
            return False
        pp = self.log.pre_prepares[sequence]
        if pp.view != view or pp.digest != digest:
            return False
        
        # Check prepares (need 2f)
        prepares = self.log.get_prepares(sequence, view, digest)
        return len(prepares) >= 2 * self.f
    
    def is_committed_local(self, sequence: int, view: int, digest: str) -> bool:
        """
        Check if committed-local(m, v, n, i) is true:
        - prepared(m, v, n, i) is true
        - Has 2f+1 matching commits (including own)
        """
        if not self.is_prepared(sequence, view, digest):
            return False
        
        commits = self.log.get_commits(sequence, view, digest)
        return len(commits) >= 2 * self.f + 1
    
    def has_stable_checkpoint(self, sequence: int) -> bool:
        """Check if checkpoint at sequence is stable (2f+1 matching checkpoints)"""
        if sequence not in self.log.checkpoints:
            return False
        
        # Group by state digest
        digest_counts: Dict[str, int] = {}
        for ckpt in self.log.checkpoints[sequence]:
            digest_counts[ckpt.state_digest] = digest_counts.get(ckpt.state_digest, 0) + 1
        
        # Check if any digest has 2f+1 checkpoints
        for count in digest_counts.values():
            if count >= 2 * self.f + 1:
                return True
        return False
    
    # -------------------------
    # Protocol phases
    # -------------------------
    
    def accept_pre_prepare(self, pp: PrePrepare) -> bool:
        """
        Accept a PRE-PREPARE message if:
        - Signature is valid (simplified in simulation)
        - View matches current view
        - Sequence number is within water marks
        - Haven't accepted different pre-prepare for same (v, n)
        """
        if pp.view != self.view:
            return False
        
        if not self.in_water_marks(pp.sequence):
            return False
        
        # Check if already have different pre-prepare for same (v, n)
        if pp.sequence in self.log.pre_prepares:
            existing = self.log.pre_prepares[pp.sequence]
            if existing.digest != pp.digest:
                return False  # Conflict!
        
        return True
    
    def execute_request(self, request: Request) -> Any:
        """
        Execute a client request on the application state.
        For demo: simple key-value operations.
        """
        op = request.operation
        
        if isinstance(op, dict):
            # Skip HEARTBEAT messages - they're just for liveness
            if op.get("type") == "HEARTBEAT":
                return {"status": "HEARTBEAT", "ignored": True}
            
            if op.get("type") == "SET":
                self.app_state[op["key"]] = op["value"]
                return {"status": "OK", "key": op["key"], "value": op["value"]}
            elif op.get("type") == "GET":
                value = self.app_state.get(op["key"], None)
                return {"status": "OK", "key": op["key"], "value": value}
            elif op.get("type") == "DELETE":
                if op["key"] in self.app_state:
                    del self.app_state[op["key"]]
                return {"status": "OK", "key": op["key"]}
        
        # Default: just echo the operation
        return {"status": "EXECUTED", "operation": op}

    def execute_committed_in_order(self):
        """Execute as many committed-local requests as possible in sequence order.

        PBFT replicas may *commit* out of order, but must *execute* in order.
        This helper is important for recovery when a node previously skipped
        execution (e.g., it was marked Byzantine) and later becomes correct.

        Returns:
            (executed_any, last_executed_seq, last_result)
        """
        executed_any = False
        last_result: Any = None

        while True:
            next_seq = self.last_executed + 1
            if next_seq not in self.log.pre_prepares:
                break
            pp = self.log.pre_prepares[next_seq]

            if not self.is_committed_local(next_seq, pp.view, pp.digest):
                break

            last_result = self.execute_request(pp.request)
            self.last_executed = next_seq
            executed_any = True

            if self.should_checkpoint():
                state_digest = self.compute_state_digest()
                ckpt = Checkpoint(sequence=self.last_executed, state_digest=state_digest, replica_id=self.id)
                self.log.add_checkpoint(ckpt)

            self.advance_stable_checkpoint()

        return executed_any, self.last_executed, last_result
    
    def should_checkpoint(self) -> bool:
        """Check if should take checkpoint now"""
        return (self.last_executed > 0 and 
                self.last_executed % self.CHECKPOINT_INTERVAL == 0 and
                self.last_executed > self.stable_checkpoint)
    
    def advance_stable_checkpoint(self):
        """Advance stable checkpoint if we have 2f+1 matching checkpoints"""
        for seq in sorted(self.log.checkpoints.keys()):
            if seq <= self.stable_checkpoint:
                continue
            
            # Group by state digest
            digest_groups: Dict[str, List[Checkpoint]] = {}
            for ckpt in self.log.checkpoints[seq]:
                if ckpt.state_digest not in digest_groups:
                    digest_groups[ckpt.state_digest] = []
                digest_groups[ckpt.state_digest].append(ckpt)
            
            # Check if any digest has 2f+1
            for digest, ckpts in digest_groups.items():
                if len(ckpts) >= 2 * self.f + 1:
                    self.stable_checkpoint = seq
                    self.stable_checkpoint_proof = ckpts
                    self.low_water_mark = seq
                    # Garbage collection
                    self.log.cleanup_before(seq)
                    return
    
    # -------------------------
    # View change
    # -------------------------
    
    def should_trigger_view_change(self, current_time: float) -> bool:
        """Check if should trigger view change (timeout expired)"""
        return (self.state == PBFTNode.NORMAL and 
                current_time >= self.view_change_timer)
    
    def create_view_change(self, new_view: Optional[int] = None) -> ViewChange:
        """Create VIEW-CHANGE message.

        In PBFT, replicas move to view v+1. In this simulator we allow the cluster
        to pass an explicit target view to avoid divergent per-node view jumps.
        """
        target_view = (self.view + 1) if new_view is None else new_view
        return ViewChange(
            new_view=target_view,
            last_stable_checkpoint=self.stable_checkpoint,
            checkpoint_proof=self.stable_checkpoint_proof[:2 * self.f + 1],
            prepared_requests=list(self.prepared_requests.values()),
            replica_id=self.id
        )
    
    def accept_view_change(self, vc: ViewChange) -> bool:
        """Check if VIEW-CHANGE is valid"""
        if vc.new_view != self.view + 1:
            return False
        
        # Verify checkpoint proof (should have 2f+1 checkpoints)
        if len(vc.checkpoint_proof) < 2 * self.f + 1:
            return False
        
        return True
    
    def has_quorum_view_changes(self, new_view: int) -> bool:
        """Check if have 2f+1 VIEW-CHANGE messages for new_view"""
        if new_view not in self.view_changes:
            return False
        return len(self.view_changes[new_view]) >= 2 * self.f + 1
    
    # -------------------------
    # Node control
    # -------------------------
    
    def stop(self):
        """Stop the node (crash)"""
        self.state = PBFTNode.STOPPED
        self.alive = False
    
    def resume(self, current_time: float):
        """Resume the node after crash (view will be synced by cluster)"""
        self.state = PBFTNode.NORMAL
        self.alive = True
        self.view_change_timer = current_time + self.view_change_timeout
        # Note: view is NOT reset here - it's synced by cluster.restart_node()
    
    def set_byzantine(self, is_byzantine: bool):
        """Mark node as Byzantine faulty (for simulation)"""
        self.is_byzantine = is_byzantine
    
    # -------------------------
    # State export for UI
    # -------------------------
    
    def to_dict(self) -> Dict:
        """Export node state for UI"""
        return {
            "id": self.id,
            "view": self.view,
            "state": self.state,
            "is_primary": self.is_primary(),
            "sequence_number": self.sequence_number,
            "last_executed": self.last_executed,
            "stable_checkpoint": self.stable_checkpoint,
            "low_water_mark": self.low_water_mark,
            "high_water_mark": self.high_water_mark,
            "app_state": dict(self.app_state),
            "alive": self.alive,
            "is_byzantine": self.is_byzantine,
            "f": self.f,
        }
    
    def __repr__(self):
        role = "Primary" if self.is_primary() else "Backup"
        return f"<Node {self.id} {role} v={self.view} seq={self.sequence_number} state={self.state}>"


import json
