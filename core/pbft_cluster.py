# core/pbft_cluster.py
import random
import threading
import time
from copy import deepcopy
from typing import Dict, List, Optional, Any
from core.pbft_node import PBFTNode
from core.pbft_message import (
    Request, PrePrepare, Prepare, Commit, Reply,
    Checkpoint, ViewChange, NewView
)

# Tunable constants
MIN_RPC_LATENCY = 5000
MAX_RPC_LATENCY = 10000
VIEW_CHANGE_TIMEOUT = 50000  # Reduced for faster demo (50 time units ~50 seconds)
HEARTBEAT_INTERVAL = 20000  # Primary sends heartbeat every 20 time units


class PBFTCluster:
    """
    pBFT cluster that manages multiple replicas and simulates the protocol.
    """
    
    def __init__(self, num_nodes: int = 7):
        """
        Initialize pBFT cluster with num_nodes replicas.
        Note: num_nodes should be 3f+1 where f is max faulty nodes.
        """
        if num_nodes < 4:
            num_nodes = 4  # Minimum 4 nodes (f=1)
        
        self.num_nodes = num_nodes
        self.f = (num_nodes - 1) // 3
        
        self.nodes: List[PBFTNode] = []
        self.time = 0.0
        
        # Message queues
        self.pre_prepares: List[PrePrepare] = []
        self.prepares: List[Prepare] = []
        self.commits: List[Commit] = []
        self.view_changes: List[ViewChange] = []
        self.new_views: List[NewView] = []
        
        # Client request queue
        self.pending_requests: List[Request] = []
        
        # Events log for UI
        self.events: List[str] = []
        self.max_events = 500  # Keep last 500 events
        
        # Threading
        self._running = False
        self._thread = None
        self._lock = threading.Lock()
        
        # Real-time sync
        self._real_start = None
        self._logical_start = None
        self.speed = 1.0
        
        self._create_nodes(num_nodes)
    
    def _create_nodes(self, n: int):
        """Create n replica nodes"""
        self.nodes = []
        for i in range(n):
            self.nodes.append(PBFTNode(node_id=i, total_nodes=n, initial_time=self.time))
        self.events.append(f"[t={self.time:.0f}] Created {n} nodes (f={self.f})")
        # Trim events if too long
        if len(self.events) > self.max_events:
            self.events = self.events[-self.max_events:]
    
    def resize(self, new_size: int):
        """Resize cluster (restart with new node count)"""
        if new_size < 4:
            new_size = 4
        self.stop_all()
        self.time = 0.0
        self.events.clear()
        self.clear_all_messages()
        self._create_nodes(new_size)
        self.num_nodes = new_size
        self.f = (new_size - 1) // 3
    
    def clear_all_messages(self):
        """Clear all pending messages"""
        self.pre_prepares.clear()
        self.prepares.clear()
        self.commits.clear()
        self.view_changes.clear()
        self.new_views.clear()
        self.pending_requests.clear()
    
    # -------------------------
    # Threading control
    # -------------------------
    
    def start_all(self):
        """Start the cluster simulation"""
        if self._running:
            return
        
        self._real_start = time.time()
        self._logical_start = self.time
        self._running = True
        self.events.append(f"[t={self.time:.0f}] Cluster started")
        
        def loop():
            while self._running:
                with self._lock:
                    self.tick()
                time.sleep(0.02)  # ~50 ticks/second
        
        self._thread = threading.Thread(target=loop, daemon=True)
        self._thread.start()
    
    def stop_all(self):
        """Stop the cluster simulation"""
        if not self._running:
            return
        self._running = False
        self.events.append(f"[t={self.time:.0f}] Cluster stopped")
    
    def is_running(self) -> bool:
        return self._running
    
    # -------------------------
    # Message utilities
    # -------------------------
    
    def randint_latency(self) -> float:
        """Generate random network latency"""
        return MIN_RPC_LATENCY + random.random() * (MAX_RPC_LATENCY - MIN_RPC_LATENCY)
    
    def send_pre_prepare(self, pp: PrePrepare):
        """Broadcast PRE-PREPARE message to all backups"""
        for i in range(self.num_nodes):
            if i == pp.primary_id:
                continue  # Skip primary (already has it)
            p = deepcopy(pp)
            p.send_time = self.time
            p.recv_time = self.time + self.randint_latency()
            self.pre_prepares.append(p)
    
    def broadcast_prepare(self, prep: Prepare):
        """Broadcast PREPARE to all nodes"""
        for i in range(self.num_nodes):
            if i == prep.replica_id:
                continue  # Don't send to self (already added to own log)
            p = deepcopy(prep)
            p.send_time = self.time
            p.recv_time = self.time + self.randint_latency()
            self.prepares.append(p)
    
    def broadcast_commit(self, commit: Commit):
        """Broadcast COMMIT to all nodes"""
        for i in range(self.num_nodes):
            if i == commit.replica_id:
                continue
            c = deepcopy(commit)
            c.send_time = self.time
            c.recv_time = self.time + self.randint_latency()
            self.commits.append(c)
    
    def broadcast_view_change(self, vc: ViewChange):
        """Broadcast VIEW-CHANGE to all nodes"""
        for i in range(self.num_nodes):
            v = deepcopy(vc)
            self.view_changes.append(v)
    
    def broadcast_new_view(self, nv: NewView):
        """Broadcast NEW-VIEW to all nodes"""
        for i in range(self.num_nodes):
            self.new_views.append(deepcopy(nv))
    
    # -------------------------
    # Client operations
    # -------------------------
    
    def submit_request(self, operation: Any, client_id: int = 0):
        """
        Submit a client request to the cluster.
        The primary will create PRE-PREPARE and initiate consensus.
        """
        request = Request(
            operation=operation,
            timestamp=self.time,
            client_id=client_id
        )
        self.pending_requests.append(request)
        self.events.append(f"[t={self.time:.0f}] Client {client_id} request: {operation}")
    
    # -------------------------
    # Protocol phases
    # -------------------------
    
    def send_primary_heartbeat(self):
        """Primary sends periodic heartbeat to show it's alive"""
        primary = self.get_primary()
        if not primary or not primary.alive or primary.state != PBFTNode.NORMAL:
            return
        
        # Check if primary needs to send heartbeat
        if not hasattr(primary, 'last_heartbeat'):
            primary.last_heartbeat = self.time
        
        if self.time - primary.last_heartbeat > HEARTBEAT_INTERVAL:
            primary.last_heartbeat = self.time
            # Reset view change timers for all backups by sending empty PRE-PREPARE
            for node in self.nodes:
                if node.id != primary.id and node.alive and node.state == PBFTNode.NORMAL:
                    # Reset view change timer when hearing from primary
                    node.view_change_timer = self.time + node.view_change_timeout * (1.0 + 0.5 * random.random())
                    node.last_heard_from_primary = self.time
    
    def process_pending_requests(self):
        """Primary processes pending client requests"""
        if not self.pending_requests:
            return
        
        primary = self.get_primary()
        if not primary or not primary.alive or primary.state != PBFTNode.NORMAL:
            return
        
        # Process one request at a time
        request = self.pending_requests.pop(0)
        
        # Assign sequence number
        primary.sequence_number += 1
        seq = primary.sequence_number
        
        # Check water marks
        if not primary.in_water_marks(seq):
            self.events.append(f"[t={self.time:.0f}] Seq {seq} outside water marks")
            return
        
        # Create PRE-PREPARE
        digest = request.digest()
        pp = PrePrepare(
            view=primary.view,
            sequence=seq,
            digest=digest,
            request=request,
            primary_id=primary.id
        )
        
        # Primary adds to own log
        primary.log.add_pre_prepare(pp)
        
        # Broadcast to backups
        self.send_pre_prepare(pp)
        
        self.events.append(
            f"[t={self.time:.0f}] Node {primary.id} (Primary) PRE-PREPARE v={pp.view} n={seq}"
        )
    
    def deliver_pre_prepares(self):
        """Deliver PRE-PREPARE messages to backups"""
        ready = [pp for pp in self.pre_prepares if pp.recv_time <= self.time]
        self.pre_prepares = [pp for pp in self.pre_prepares if pp.recv_time > self.time]
        
        for pp in ready:
            # Find the target node for this message copy
            target_nodes = [node for node in self.nodes if node.id != pp.primary_id]
            
            for node in target_nodes:
                if not node.alive or node.state != PBFTNode.NORMAL:
                    continue
                
                # Check if backup accepts PRE-PREPARE
                if not node.accept_pre_prepare(pp):
                    continue
                
                # Check if already processed this pre-prepare
                if pp.sequence in node.log.pre_prepares:
                    continue
                
                # Add to log
                node.log.add_pre_prepare(pp)
                
                # Reset view change timer - heard from primary
                node.view_change_timer = self.time + node.view_change_timeout * (1.0 + 0.5 * random.random())
                node.last_heard_from_primary = self.time
                
                # Send PREPARE
                prepare = Prepare(
                    view=pp.view,
                    sequence=pp.sequence,
                    digest=pp.digest,
                    replica_id=node.id
                )
                
                # Add to own log first
                node.log.add_prepare(prepare)
                
                # Broadcast to others
                self.broadcast_prepare(prepare)
                
                self.events.append(
                    f"[t={self.time:.0f}] Node {node.id} sends PREPARE n={pp.sequence}"
                )
                break  # Only process once per node
    
    def deliver_prepares(self):
        """Deliver PREPARE messages to replicas"""
        ready = [p for p in self.prepares if p.recv_time <= self.time]
        self.prepares = [p for p in self.prepares if p.recv_time > self.time]
        
        for prep in ready:
            # Deliver to all nodes except sender
            for node in self.nodes:
                if node.id == prep.replica_id:
                    continue  # Already in sender's log
                
                if not node.alive or node.state != PBFTNode.NORMAL:
                    continue
                
                if prep.view != node.view:
                    continue
                
                # Add to log (will check for duplicates internally)
                node.log.add_prepare(prep)
            
            # Now check all nodes if they reached prepared state
            for node in self.nodes:
                if not node.alive or node.state != PBFTNode.NORMAL:
                    continue
                    
                # Check if prepared
                if node.is_prepared(prep.sequence, prep.view, prep.digest):
                    # Mark as prepared (for view change)
                    if prep.sequence not in node.prepared_requests:
                        node.prepared_requests[prep.sequence] = {
                            "view": prep.view,
                            "digest": prep.digest,
                            "sequence": prep.sequence
                        }
                    
                    # Check if already sent commit for this
                    existing_commits = node.log.get_commits(prep.sequence, prep.view, prep.digest)
                    already_sent = any(c.replica_id == node.id for c in existing_commits)
                    
                    if not already_sent:
                        # Send COMMIT
                        commit = Commit(
                            view=prep.view,
                            sequence=prep.sequence,
                            digest=prep.digest,
                            replica_id=node.id
                        )
                        
                        # Add to own log
                        node.log.add_commit(commit)
                        
                        # Broadcast
                        self.broadcast_commit(commit)
                        
                        self.events.append(
                            f"[t={self.time:.0f}] Node {node.id} prepared & sends COMMIT n={prep.sequence}"
                        )
    
    def deliver_commits(self):
        """Deliver COMMIT messages and execute requests"""
        ready = [c for c in self.commits if c.recv_time <= self.time]
        self.commits = [c for c in self.commits if c.recv_time > self.time]
        
        for com in ready:
            # Deliver to all nodes
            for node in self.nodes:
                if node.id == com.replica_id:
                    continue  # Already in sender's log
                
                if not node.alive or node.state != PBFTNode.NORMAL:
                    continue
                
                if com.view != node.view:
                    continue
                
                # Add to log
                node.log.add_commit(com)
                
                # Check if committed-local
                if node.is_committed_local(com.sequence, com.view, com.digest):
                    # Execute if not already executed
                    if com.sequence == node.last_executed + 1:
                        # Get the request
                        if com.sequence in node.log.pre_prepares:
                            pp = node.log.pre_prepares[com.sequence]
                            result = node.execute_request(pp.request)
                            node.last_executed = com.sequence
                            
                            self.events.append(
                                f"[t={self.time:.0f}] Node {node.id} EXECUTED n={com.sequence} result={result}"
                            )
                            
                            # Check if should checkpoint
                            if node.should_checkpoint():
                                state_digest = node.compute_state_digest()
                                ckpt = Checkpoint(
                                    sequence=node.last_executed,
                                    state_digest=state_digest,
                                    replica_id=node.id
                                )
                                node.log.add_checkpoint(ckpt)
                                
                                # Broadcast checkpoint (simplified - just add to all nodes)
                                for other in self.nodes:
                                    if other.id != node.id:
                                        other.log.add_checkpoint(ckpt)
                                
                                self.events.append(
                                    f"[t={self.time:.0f}] Node {node.id} CHECKPOINT n={node.last_executed}"
                                )
                            
                            # Advance stable checkpoint if possible
                            node.advance_stable_checkpoint()
    
    # -------------------------
    # View change protocol
    # -------------------------
    
    def check_view_change_timeouts(self):
        """Check if any node should trigger view change"""
        primary = self.get_primary()
        
        for node in self.nodes:
            if not node.alive or node.state != PBFTNode.NORMAL:
                continue
            
            # Skip if this is the primary
            if node.id == (node.view % self.num_nodes):
                continue
            
            # Check if primary is dead (not alive)
            should_change = False
            if primary and not primary.alive:
                # Primary is crashed - trigger view change immediately!
                should_change = True
                reason = "Primary CRASHED"
            elif node.should_trigger_view_change(self.time):
                # Timeout expired
                should_change = True
                reason = "Timeout"
            
            if should_change:
                node.state = PBFTNode.VIEW_CHANGING
                vc = node.create_view_change()
                
                # Add to own collection
                new_view = vc.new_view
                if new_view not in node.view_changes:
                    node.view_changes[new_view] = []
                node.view_changes[new_view].append(vc)
                
                # Broadcast
                self.broadcast_view_change(vc)
                
                self.events.append(
                    f"[t={self.time:.0f}] Node {node.id} VIEW-CHANGE to v={new_view} ({reason})"
                )
    
    def deliver_view_changes(self):
        """Deliver VIEW-CHANGE messages"""
        while self.view_changes:
            vc = self.view_changes.pop(0)
            
            for node in self.nodes:
                if not node.alive:
                    continue
                
                # Accept view change even if in ViewChanging state
                if vc.new_view <= node.view:
                    continue  # Already in this view or later
                
                # Add to collection
                if vc.new_view not in node.view_changes:
                    node.view_changes[vc.new_view] = []
                
                # Avoid duplicates
                if not any(v.replica_id == vc.replica_id for v in node.view_changes[vc.new_view]):
                    node.view_changes[vc.new_view].append(vc)
                
                # Check if this node is new primary and has quorum
                new_primary_id = vc.new_view % self.num_nodes
                if node.id == new_primary_id and node.has_quorum_view_changes(vc.new_view):
                    # Check if NEW-VIEW already sent for this view
                    already_sent = any(nv.view == vc.new_view for nv in self.new_views)
                    if not already_sent:
                        # Create NEW-VIEW
                        nv = NewView(
                            view=vc.new_view,
                            view_changes=node.view_changes[vc.new_view][:2 * node.f + 1],
                            pre_prepares=[],  # Simplified: don't reprocess old requests
                            primary_id=node.id
                        )
                        
                        # Broadcast NEW-VIEW
                        self.broadcast_new_view(nv)
                        
                        self.events.append(
                            f"[t={self.time:.0f}] Node {node.id} (New Primary) NEW-VIEW v={vc.new_view}"
                        )
                        
                        # New primary enters new view immediately
                        node.view = vc.new_view
                        node.state = PBFTNode.NORMAL
                        node.view_change_timer = self.time + node.view_change_timeout * (1.5 + random.random())
    
    def deliver_new_views(self):
        """Deliver NEW-VIEW messages and enter new view"""
        delivered = []
        for nv in self.new_views:
            for node in self.nodes:
                if not node.alive:
                    continue
                
                if node.view >= nv.view:
                    continue  # Already in this view or later
                
                # Enter new view
                node.view = nv.view
                node.state = PBFTNode.NORMAL
                node.view_change_timer = self.time + node.view_change_timeout * (1.5 + random.random())
                
                self.events.append(
                    f"[t={self.time:.0f}] Node {node.id} enters view {nv.view}"
                )
            delivered.append(nv)
        
        # Remove delivered NEW-VIEW messages
        for nv in delivered:
            self.new_views.remove(nv)
    
    # -------------------------
    # Main tick
    # -------------------------
    
    def tick(self, dt: float = 1000.0):
        """
        Advance cluster by dt time units.
        Process all protocol phases.
        """
        self.time += dt
        
        # Send heartbeat from primary if no activity
        self.send_primary_heartbeat()
        
        # Process pending client requests (primary creates PRE-PREPARE)
        self.process_pending_requests()
        
        # Deliver messages in protocol order
        self.deliver_pre_prepares()  # Phase 1
        self.deliver_prepares()       # Phase 2
        self.deliver_commits()        # Phase 3
        
        # View change
        self.check_view_change_timeouts()
        self.deliver_view_changes()
        self.deliver_new_views()
    
    # -------------------------
    # Utilities
    # -------------------------
    
    def get_primary(self) -> Optional[PBFTNode]:
        """Get current primary node"""
        if not self.nodes:
            return None
        view = self.nodes[0].view
        primary_id = view % self.num_nodes
        return self.nodes[primary_id]
    
    def get_leader(self) -> Optional[int]:
        """Get current primary node ID (for UI compatibility)"""
        primary = self.get_primary()
        return primary.id if primary and primary.alive else None
    
    def crash_node(self, node_id: int):
        """Crash a node"""
        if 0 <= node_id < len(self.nodes):
            self.nodes[node_id].stop()
            self.events.append(f"[t={self.time:.0f}] Node {node_id} CRASHED")
    
    def restart_node(self, node_id: int):
        """Restart a crashed node"""
        if 0 <= node_id < len(self.nodes):
            node = self.nodes[node_id]
            
            # Find current view from alive nodes
            current_view = 0
            for other in self.nodes:
                if other.alive and other.id != node_id:
                    current_view = max(current_view, other.view)
                    break
            
            # Restart and sync to current view
            node.resume(self.time)
            node.view = current_view  # Sync to current cluster view!
            node.state = PBFTNode.NORMAL
            node.view_change_timer = self.time + node.view_change_timeout * (1.5 + random.random())
            
            self.events.append(f"[t={self.time:.0f}] Node {node_id} RESTARTED (synced to view {current_view})")
    
    def set_byzantine(self, node_id: int, is_byzantine: bool):
        """Mark a node as Byzantine faulty"""
        if 0 <= node_id < len(self.nodes):
            self.nodes[node_id].set_byzantine(is_byzantine)
            status = "Byzantine" if is_byzantine else "Correct"
            self.events.append(f"[t={self.time:.0f}] Node {node_id} marked as {status}")
    
    def get_events(self, n: int = 50) -> List[str]:
        """Get last n events"""
        return self.events[-n:]
    
    def get_status(self) -> Dict:
        """Get cluster status for UI"""
        return {
            "time": self.time,
            "num_nodes": self.num_nodes,
            "f": self.f,
            "primary_id": self.get_leader(),
            "running": self.is_running(),
            "nodes": [node.to_dict() for node in self.nodes],
            "events": self.get_events(20)
        }
