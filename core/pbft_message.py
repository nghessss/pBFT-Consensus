# core/pbft_message.py
from dataclasses import dataclass, field
from typing import Any, List, Dict, Optional
import hashlib
import json

@dataclass
class Request:
    """Client request message"""
    operation: Any          # The operation to execute
    timestamp: float        # Client timestamp
    client_id: int         # Client identifier
    
    def digest(self) -> str:
        """Compute digest of the request"""
        content = f"{self.operation}:{self.timestamp}:{self.client_id}"
        return hashlib.sha256(content.encode()).hexdigest()[:16]
    
    def __repr__(self):
        return f"<Request op={self.operation} t={self.timestamp} c={self.client_id}>"


@dataclass
class PrePrepare:
    """PRE-PREPARE message sent by primary"""
    view: int
    sequence: int
    digest: str
    request: Request
    primary_id: int
    send_time: float = 0.0
    recv_time: float = 0.0
    
    def __repr__(self):
        return f"<PrePrepare v={self.view} n={self.sequence} d={self.digest[:8]}>"


@dataclass
class Prepare:
    """PREPARE message sent by replicas"""
    view: int
    sequence: int
    digest: str
    replica_id: int
    send_time: float = 0.0
    recv_time: float = 0.0
    
    def __repr__(self):
        return f"<Prepare v={self.view} n={self.sequence} d={self.digest[:8]} from={self.replica_id}>"


@dataclass
class Commit:
    """COMMIT message sent by replicas"""
    view: int
    sequence: int
    digest: str
    replica_id: int
    send_time: float = 0.0
    recv_time: float = 0.0
    
    def __repr__(self):
        return f"<Commit v={self.view} n={self.sequence} d={self.digest[:8]} from={self.replica_id}>"


@dataclass
class Reply:
    """REPLY message sent to client"""
    view: int
    timestamp: float
    client_id: int
    replica_id: int
    result: Any
    
    def __repr__(self):
        return f"<Reply v={self.view} t={self.timestamp} from={self.replica_id}>"


@dataclass
class Checkpoint:
    """CHECKPOINT message for state snapshot"""
    sequence: int
    state_digest: str
    replica_id: int
    
    def __repr__(self):
        return f"<Checkpoint n={self.sequence} d={self.state_digest[:8]} from={self.replica_id}>"


@dataclass
class ViewChange:
    """VIEW-CHANGE message to change primary"""
    new_view: int
    last_stable_checkpoint: int
    checkpoint_proof: List[Checkpoint]
    prepared_requests: List[Dict]  # List of prepared certificates
    replica_id: int
    
    def __repr__(self):
        return f"<ViewChange nv={self.new_view} n={self.last_stable_checkpoint} from={self.replica_id}>"


@dataclass
class NewView:
    """NEW-VIEW message from new primary"""
    view: int
    view_changes: List[ViewChange]
    pre_prepares: List[PrePrepare]
    primary_id: int
    
    def __repr__(self):
        return f"<NewView v={self.view} from={self.primary_id}>"


class MessageLog:
    """Log to store protocol messages"""
    def __init__(self):
        self.pre_prepares: Dict[int, PrePrepare] = {}  # seq -> PrePrepare
        self.prepares: Dict[int, List[Prepare]] = {}   # seq -> [Prepare]
        self.commits: Dict[int, List[Commit]] = {}     # seq -> [Commit]
        self.checkpoints: Dict[int, List[Checkpoint]] = {}  # seq -> [Checkpoint]
        
    def add_pre_prepare(self, pp: PrePrepare):
        self.pre_prepares[pp.sequence] = pp
        
    def add_prepare(self, p: Prepare):
        if p.sequence not in self.prepares:
            self.prepares[p.sequence] = []
        # Avoid duplicates from same replica
        if not any(prep.replica_id == p.replica_id for prep in self.prepares[p.sequence]):
            self.prepares[p.sequence].append(p)
            
    def add_commit(self, c: Commit):
        if c.sequence not in self.commits:
            self.commits[c.sequence] = []
        # Avoid duplicates from same replica
        if not any(com.replica_id == c.replica_id for com in self.commits[c.sequence]):
            self.commits[c.sequence].append(c)
            
    def add_checkpoint(self, ckpt: Checkpoint):
        if ckpt.sequence not in self.checkpoints:
            self.checkpoints[ckpt.sequence] = []
        if not any(c.replica_id == ckpt.replica_id for c in self.checkpoints[ckpt.sequence]):
            self.checkpoints[ckpt.sequence].append(ckpt)
            
    def get_prepares(self, sequence: int, view: int, digest: str) -> List[Prepare]:
        """Get matching prepare messages"""
        if sequence not in self.prepares:
            return []
        return [p for p in self.prepares[sequence] 
                if p.view == view and p.digest == digest]
    
    def get_commits(self, sequence: int, view: int, digest: str) -> List[Commit]:
        """Get matching commit messages"""
        if sequence not in self.commits:
            return []
        return [c for c in self.commits[sequence] 
                if c.view == view and c.digest == digest]
    
    def get_checkpoints(self, sequence: int, state_digest: str) -> List[Checkpoint]:
        """Get matching checkpoint messages"""
        if sequence not in self.checkpoints:
            return []
        return [c for c in self.checkpoints[sequence] 
                if c.state_digest == state_digest]
    
    def cleanup_before(self, sequence: int):
        """Remove log entries before sequence number (garbage collection)"""
        self.pre_prepares = {s: pp for s, pp in self.pre_prepares.items() if s > sequence}
        self.prepares = {s: preps for s, preps in self.prepares.items() if s > sequence}
        self.commits = {s: coms for s, coms in self.commits.items() if s > sequence}
        self.checkpoints = {s: ckpts for s, ckpts in self.checkpoints.items() if s > sequence}
