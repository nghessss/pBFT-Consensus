# core/pbft_cluster.py
import random
import threading
import time
from copy import deepcopy
from typing import Dict, List, Optional, Any, Tuple

import grpc

from core.pbft_node import PBFTNode
from core.pbft_message import (
    Request, PrePrepare, Prepare, Commit, Reply,
    Checkpoint, ViewChange, NewView
)

from core import pbft_rpc
from rpc import pbft_pb2, pbft_pb2_grpc

# Tunable constants
MIN_RPC_LATENCY = 5000
MAX_RPC_LATENCY = 10000
VIEW_CHANGE_TIMEOUT = 150000  # Time before backup triggers view change
HEARTBEAT_INTERVAL = 15000  # Logical heartbeat (timer reset), not a PBFT request


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

        # Cluster-wide (authoritative) view.
        # Using per-node view as the source of truth causes split-brain primary selection.
        self.current_view = 0

        # Used to avoid triggering view-change while the system is idle.
        self.last_client_activity_time = -1e18
        
        # Message queues
        # Queues store (recv_time, target_id, message)
        self.pre_prepares: List[Tuple[float, int, PrePrepare]] = []
        self.prepares: List[Tuple[float, int, Prepare]] = []
        self.commits: List[Tuple[float, int, Commit]] = []
        self.checkpoints: List[Tuple[float, int, Checkpoint]] = []
        self.view_changes: List[Tuple[float, int, ViewChange]] = []
        self.new_views: List[Tuple[float, int, NewView]] = []
        
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

        # gRPC per-node servers + stubs (multi-threaded node runtime)
        self._base_port = 50050
        self._rpc_servers: Dict[int, grpc.Server] = {}
        self._rpc_channels: Dict[int, grpc.Channel] = {}
        self._rpc_stubs: Dict[int, pbft_pb2_grpc.PBFTNodeServiceStub] = {}
        self._rpc_ports: Dict[int, int] = {}

        # RPC call timeout (seconds) to avoid UI hangs
        self._rpc_timeout_s = 0.5
        
        self._create_nodes(num_nodes)

    def close(self):
        """Stop background threads/servers (useful for scripts/tests)."""
        try:
            self.stop_all()
        finally:
            self._stop_rpc()

    def __del__(self):
        try:
            self._stop_rpc()
        except Exception:
            pass
    
    def _create_nodes(self, n: int):
        """Create n replica nodes"""
        # Stop any existing RPC servers before recreating.
        self._stop_rpc()

        self.nodes = []
        for i in range(n):
            self.nodes.append(PBFTNode(node_id=i, total_nodes=n, initial_time=self.time))

        # Start per-node gRPC servers (each node runs independently in its own server thread pool)
        for node in self.nodes:
            port = self._base_port + node.id
            self._rpc_ports[node.id] = port
            self._rpc_servers[node.id] = pbft_rpc.start_node_server(node, port)
            ch = grpc.insecure_channel(f"127.0.0.1:{port}")
            self._rpc_channels[node.id] = ch
            self._rpc_stubs[node.id] = pbft_pb2_grpc.PBFTNodeServiceStub(ch)

        # Ensure channels are ready before first tick.
        for node_id, ch in self._rpc_channels.items():
            try:
                grpc.channel_ready_future(ch).result(timeout=2.0)
            except Exception:
                self.events.append(f"[t={self.time:.0f}] Warning: gRPC channel for node {node_id} not ready")

        # Sync all nodes to cluster view
        for node in self.nodes:
            node.view = self.current_view
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
        self.current_view = 0
        self.last_client_activity_time = -1e18
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
        self.checkpoints.clear()
        self.view_changes.clear()
        self.new_views.clear()
        self.pending_requests.clear()

    def _stop_rpc(self):
        """Stop all gRPC servers/channels."""
        for server in list(self._rpc_servers.values()):
            try:
                stop_future = server.stop(0)
                try:
                    stop_future.wait(timeout=1)
                except Exception:
                    pass
                try:
                    server.wait_for_termination(timeout=1)
                except Exception:
                    pass
                executor = getattr(server, "_pbft_executor", None)
                if executor is not None:
                    try:
                        executor.shutdown(wait=False, cancel_futures=True)
                    except TypeError:
                        # Older Python: cancel_futures not available
                        executor.shutdown(wait=False)
            except Exception:
                pass
        self._rpc_servers.clear()

        for ch in list(self._rpc_channels.values()):
            try:
                ch.close()
            except Exception:
                pass
        self._rpc_channels.clear()
        self._rpc_stubs.clear()
        self._rpc_ports.clear()
    
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
        """Broadcast PRE-PREPARE message to all replicas (including primary)"""
        for i in range(self.num_nodes):
            p = deepcopy(pp)
            p.send_time = self.time
            latency = 0.0 if i == pp.primary_id else self.randint_latency()
            p.recv_time = self.time + latency
            self.pre_prepares.append((p.recv_time, i, p))
    
    def broadcast_prepare(self, prep: Prepare):
        """Broadcast PREPARE to all nodes"""
        for i in range(self.num_nodes):
            if i == prep.replica_id:
                continue  # Don't send to self (already added to own log)
            p = deepcopy(prep)
            p.send_time = self.time
            p.recv_time = self.time + self.randint_latency()
            self.prepares.append((p.recv_time, i, p))
    
    def broadcast_commit(self, commit: Commit):
        """Broadcast COMMIT to all nodes"""
        for i in range(self.num_nodes):
            if i == commit.replica_id:
                continue
            c = deepcopy(commit)
            c.send_time = self.time
            c.recv_time = self.time + self.randint_latency()
            self.commits.append((c.recv_time, i, c))

    def broadcast_checkpoint(self, ckpt: Checkpoint):
        """Broadcast CHECKPOINT to all other nodes"""
        for i in range(self.num_nodes):
            if i == ckpt.replica_id:
                continue
            cp = deepcopy(ckpt)
            recv_time = self.time + self.randint_latency()
            self.checkpoints.append((recv_time, i, cp))
    
    def broadcast_view_change(self, vc: ViewChange):
        """Broadcast VIEW-CHANGE to all nodes"""
        for i in range(self.num_nodes):
            v = deepcopy(vc)
            self.view_changes.append((self.time, i, v))
    
    def broadcast_new_view(self, nv: NewView):
        """Broadcast NEW-VIEW to all nodes"""
        for i in range(self.num_nodes):
            self.new_views.append((self.time, i, deepcopy(nv)))
    
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
        self.last_client_activity_time = self.time
        self.events.append(f"[t={self.time:.0f}] Client {client_id} request: {operation}")
    
    # -------------------------
    # Protocol phases
    # -------------------------
    
    def send_primary_heartbeat(self):
        """Logical heartbeat.

        PBFT liveness is normally driven by progress (receiving PRE-PREPARE/COMMIT).
        In this simulator, we use a lightweight heartbeat that ONLY resets backup
        view-change timers (does not create PBFT consensus messages / sequence numbers).
        """
        primary = self.get_primary()
        if not primary or not primary.alive or primary.state != PBFTNode.NORMAL:
            return

        if not hasattr(primary, "last_heartbeat"):
            primary.last_heartbeat = self.time

        if self.time - primary.last_heartbeat > HEARTBEAT_INTERVAL:
            primary.last_heartbeat = self.time
            for node in self.nodes:
                if node.id == primary.id or not node.alive or node.state != PBFTNode.NORMAL:
                    continue
                node.last_heard_from_primary = self.time
                node.view_change_timer = self.time + node.view_change_timeout * (1.0 + 0.5 * random.random())
    
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

        # Broadcast PRE-PREPARE via gRPC (delivery scheduled with simulated latency)
        primary.last_heartbeat = self.time
        self.send_pre_prepare(pp)
        
        self.events.append(
            f"[t={self.time:.0f}] Node {primary.id} (Primary) PRE-PREPARE v={pp.view} n={seq}"
        )
    
    def deliver_pre_prepares(self):
        """Deliver PRE-PREPARE messages to backups"""
        ready = [(rt, tid, pp) for (rt, tid, pp) in self.pre_prepares if rt <= self.time]
        self.pre_prepares = [(rt, tid, pp) for (rt, tid, pp) in self.pre_prepares if rt > self.time]

        for _rt, target_id, pp in ready:
            node = self.nodes[target_id]
            if not node.alive or node.state != PBFTNode.NORMAL:
                continue

            try:
                resp = self._rpc_stubs[target_id].ReceivePrePrepare(
                    pbft_rpc._to_pre_prepare_msg(pp), timeout=self._rpc_timeout_s
                )
            except Exception as e:
                self.events.append(f"[t={self.time:.0f}] RPC ReceivePrePrepare to Node {target_id} failed: {e}")
                continue

            if resp.accepted and resp.has_prepare:
                prepare = pbft_rpc._from_prepare_msg(resp.prepare)
                self.broadcast_prepare(prepare)
                self.events.append(f"[t={self.time:.0f}] Node {target_id} sends PREPARE n={prepare.sequence}")
    
    def deliver_prepares(self):
        """Deliver PREPARE messages to replicas"""
        ready = [(rt, tid, p) for (rt, tid, p) in self.prepares if rt <= self.time]
        self.prepares = [(rt, tid, p) for (rt, tid, p) in self.prepares if rt > self.time]

        for _rt, target_id, prep in ready:
            node = self.nodes[target_id]
            if not node.alive or node.state != PBFTNode.NORMAL:
                continue

            try:
                resp = self._rpc_stubs[target_id].ReceivePrepare(
                    pbft_rpc._to_prepare_msg(prep), timeout=self._rpc_timeout_s
                )
            except Exception as e:
                self.events.append(f"[t={self.time:.0f}] RPC ReceivePrepare to Node {target_id} failed: {e}")
                continue

            if resp.accepted and resp.has_commit:
                commit = pbft_rpc._from_commit_msg(resp.commit)
                self.broadcast_commit(commit)
                self.events.append(f"[t={self.time:.0f}] Node {target_id} prepared & sends COMMIT n={commit.sequence}")
    
    def deliver_commits(self):
        """Deliver COMMIT messages and execute requests"""
        ready = [(rt, tid, c) for (rt, tid, c) in self.commits if rt <= self.time]
        self.commits = [(rt, tid, c) for (rt, tid, c) in self.commits if rt > self.time]

        for _rt, target_id, com in ready:
            node = self.nodes[target_id]
            if not node.alive or node.state != PBFTNode.NORMAL:
                continue

            try:
                resp = self._rpc_stubs[target_id].ReceiveCommit(
                    pbft_rpc._to_commit_msg(com), timeout=self._rpc_timeout_s
                )
            except Exception as e:
                self.events.append(f"[t={self.time:.0f}] RPC ReceiveCommit to Node {target_id} failed: {e}")
                continue

            if resp.accepted and resp.executed:
                self.events.append(
                    f"[t={self.time:.0f}] Node {target_id} EXECUTED n={resp.executed_sequence} result={resp.result_json}"
                )
            if resp.accepted and resp.has_checkpoint:
                ckpt = pbft_rpc._from_checkpoint_msg(resp.checkpoint)
                self.broadcast_checkpoint(ckpt)
                self.events.append(f"[t={self.time:.0f}] Node {target_id} CHECKPOINT n={ckpt.sequence}")

    def deliver_checkpoints(self):
        ready = [(rt, tid, ck) for (rt, tid, ck) in self.checkpoints if rt <= self.time]
        self.checkpoints = [(rt, tid, ck) for (rt, tid, ck) in self.checkpoints if rt > self.time]

        for _rt, target_id, ckpt in ready:
            node = self.nodes[target_id]
            if not node.alive:
                continue
            try:
                self._rpc_stubs[target_id].ReceiveCheckpoint(
                    pbft_rpc._to_checkpoint_msg(ckpt), timeout=self._rpc_timeout_s
                )
            except Exception as e:
                self.events.append(f"[t={self.time:.0f}] RPC ReceiveCheckpoint to Node {target_id} failed: {e}")
    
    # -------------------------
    # View change protocol
    # -------------------------
    
    def check_view_change_timeouts(self):
        """Check if any node should trigger view change"""
        # Don't trigger view change if the system is idle (no recent client activity)
        if not self.pending_requests and (self.time - self.last_client_activity_time) > VIEW_CHANGE_TIMEOUT:
            return

        primary = self.get_primary()
        if primary is None:
            return

        target_view = self.current_view + 1
        
        for node in self.nodes:
            if not node.alive or node.state != PBFTNode.NORMAL:
                continue
            
            # Keep nodes aligned to the cluster view
            if node.view != self.current_view:
                node.view = self.current_view

            # Skip if this is the primary for the cluster's current view
            if node.id == (self.current_view % self.num_nodes):
                continue
            
            # Check if primary is dead (not alive)
            should_change = False
            if not primary.alive:
                # Primary is crashed - trigger view change immediately!
                should_change = True
                reason = "Primary CRASHED"
            elif node.should_trigger_view_change(self.time):
                # Timeout expired
                should_change = True
                reason = "Timeout"
            
            if should_change:
                node.state = PBFTNode.VIEW_CHANGING
                vc = node.create_view_change(new_view=target_view)
                
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
            _rt, target_id, vc = self.view_changes.pop(0)

            # Ignore stale/old view changes
            if vc.new_view <= self.current_view:
                continue

            # Deliver via RPC to the intended target
            node = self.nodes[target_id]
            if not node.alive:
                continue
            try:
                self._rpc_stubs[target_id].ReceiveViewChange(
                    pbft_rpc._to_view_change_msg(vc), timeout=self._rpc_timeout_s
                )
            except Exception as e:
                self.events.append(f"[t={self.time:.0f}] RPC ReceiveViewChange to Node {target_id} failed: {e}")
                continue

            # Check if new primary has quorum (quorum is tracked inside node.view_changes)
            new_primary_id = vc.new_view % self.num_nodes
            new_primary = self.nodes[new_primary_id]
            if new_primary.alive and new_primary.has_quorum_view_changes(vc.new_view):
                already_sent = any(nv.view == vc.new_view for (_rt2, _tid2, nv) in self.new_views)
                if not already_sent:
                    nv = NewView(
                        view=vc.new_view,
                        view_changes=new_primary.view_changes[vc.new_view][:2 * new_primary.f + 1],
                        pre_prepares=[],
                        primary_id=new_primary_id,
                    )
                    self.broadcast_new_view(nv)
                    self.events.append(f"[t={self.time:.0f}] Node {new_primary_id} (New Primary) NEW-VIEW v={vc.new_view}")

                    self.current_view = vc.new_view
                    # Sync all alive replicas to the cluster view
                    for other in self.nodes:
                        if not other.alive:
                            continue
                        other.view = self.current_view
                        if other.state != PBFTNode.STOPPED:
                            other.state = PBFTNode.NORMAL
                        other.last_heard_from_primary = self.time
                        other.view_change_timer = self.time + other.view_change_timeout * (1.5 + random.random())
    
    def deliver_new_views(self):
        """Deliver NEW-VIEW messages and enter new view"""
        delivered = []
        for rt, target_id, nv in self.new_views:
            if rt > self.time:
                continue
            # Update cluster view as well
            if nv.view > self.current_view:
                self.current_view = nv.view
            node = self.nodes[target_id]
            if not node.alive:
                delivered.append((rt, target_id, nv))
                continue

            try:
                self._rpc_stubs[target_id].ReceiveNewView(
                    pbft_rpc._to_new_view_msg(nv), timeout=self._rpc_timeout_s
                )
            except Exception as e:
                self.events.append(f"[t={self.time:.0f}] RPC ReceiveNewView to Node {target_id} failed: {e}")
                delivered.append((rt, target_id, nv))
                continue

            if node.view < nv.view:
                node.view = nv.view
            node.state = PBFTNode.NORMAL
            node.view_change_timer = self.time + node.view_change_timeout * (1.5 + random.random())
            node.last_heard_from_primary = self.time
            self.events.append(f"[t={self.time:.0f}] Node {target_id} enters view {nv.view}")
            delivered.append((rt, target_id, nv))
        
        # Remove delivered NEW-VIEW messages
        for item in delivered:
            if item in self.new_views:
                self.new_views.remove(item)
    
    # -------------------------
    # Main tick
    # -------------------------
    
    def tick(self, dt: float = 1000.0):
        """
        Advance cluster by dt time units.
        Process all protocol phases.
        """
        self.time += dt

        # View change first (so a crashed primary is replaced before we try to process requests)
        self.check_view_change_timeouts()
        self.deliver_view_changes()
        self.deliver_new_views()

        # Primary liveness (timer reset only)
        self.send_primary_heartbeat()

        # Process pending client requests (primary creates PRE-PREPARE)
        self.process_pending_requests()

        # Deliver messages in protocol order
        self.deliver_pre_prepares()  # Phase 1
        self.deliver_prepares()       # Phase 2
        self.deliver_commits()        # Phase 3
        self.deliver_checkpoints()
    
    # -------------------------
    # Utilities
    # -------------------------
    
    def get_primary(self) -> Optional[PBFTNode]:
        """Get current primary node"""
        if not self.nodes:
            return None
        primary_id = self.current_view % self.num_nodes
        return self.nodes[primary_id]
    
    def get_leader(self) -> Optional[int]:
        """Get current primary node ID (for UI compatibility)"""
        primary = self.get_primary()
        return primary.id if primary and primary.alive else None
    
    def crash_node(self, node_id: int):
        """Crash a node"""
        if 0 <= node_id < len(self.nodes):
            try:
                self._rpc_stubs[node_id].Crash(pbft_pb2.Empty(), timeout=self._rpc_timeout_s)
            except Exception:
                self.nodes[node_id].stop()
            self.events.append(f"[t={self.time:.0f}] Node {node_id} CRASHED")
    
    def restart_node(self, node_id: int):
        """Restart a crashed node"""
        if 0 <= node_id < len(self.nodes):
            node = self.nodes[node_id]
            try:
                self._rpc_stubs[node_id].Restart(
                    pbft_pb2.TimeMsg(current_time=self.time), timeout=self._rpc_timeout_s
                )
            except Exception:
                node.resume(self.time)

            node.view = self.current_view
            node.state = PBFTNode.NORMAL
            node.view_change_timer = self.time + node.view_change_timeout * (1.5 + random.random())

            self.events.append(f"[t={self.time:.0f}] Node {node_id} RESTARTED (synced to view {self.current_view})")
    
    def set_byzantine(self, node_id: int, is_byzantine: bool):
        """Mark a node as Byzantine faulty"""
        if 0 <= node_id < len(self.nodes):
            with self._lock:
                before_exec = self.nodes[node_id].last_executed
                try:
                    self._rpc_stubs[node_id].SetByzantine(
                        pbft_pb2.BoolMsg(value=is_byzantine), timeout=self._rpc_timeout_s
                    )
                except Exception:
                    self.nodes[node_id].set_byzantine(is_byzantine)

                status = "Byzantine" if is_byzantine else "Correct"
                self.events.append(f"[t={self.time:.0f}] Node {node_id} marked as {status}")

                # If the node was fixed, it may have executed a backlog immediately.
                if not is_byzantine:
                    after_exec = self.nodes[node_id].last_executed
                    if after_exec > before_exec:
                        self.events.append(
                            f"[t={self.time:.0f}] Node {node_id} caught up: executed {before_exec + 1}..{after_exec}"
                        )
    
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
