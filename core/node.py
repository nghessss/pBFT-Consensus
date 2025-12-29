from __future__ import annotations

import hashlib
import time
from threading import Lock
from typing import Optional, Tuple

from core.state import PBFTEntry, PBFTState
from rpc import pbft_pb2


class PBFTNode:
    def __init__(self, node_id: int, peers: list[int], rpc_clients):
        self.state = PBFTState(node_id, peers)
        self.rpc_clients = rpc_clients
        self._lock = Lock()

    def start(self):
        state = self.state
        print(f"[PBFT Node {state.node_id}] starting")
        print(
            f"[PBFT Node {state.node_id}] role={state.role} view={state.view} primary={state.primary_id} f={state.f} n={state.n}"
        )

    # ============================
    # PBFT helpers
    # ============================
    def _digest(self, request: pbft_pb2.ClientRequest) -> str:
        h = hashlib.sha256()
        h.update(request.client_id.encode("utf-8"))
        h.update(b"|")
        h.update(request.request_id.encode("utf-8"))
        h.update(b"|")
        h.update(request.payload.encode("utf-8"))
        return h.hexdigest()

    def _entry_key(self, view: int, seq: int) -> Tuple[int, int]:
        return (view, seq)

    def _pending_key(self, view: int, seq: int, digest: str) -> Tuple[int, int, str]:
        return (int(view), int(seq), str(digest))

    # ============================
    # RPC handlers (called by rpc/server.py)
    # ============================
    def on_client_request(self, req: pbft_pb2.ClientRequest, timeout_s: float = 30.0) -> pbft_pb2.ClientReply:
        state = self.state

        if not state.alive:
            return pbft_pb2.ClientReply(
                client_id=req.client_id,
                request_id=req.request_id,
                replica_id=state.node_id,
                view=state.view,
                seq=0,
                committed=False,
                result="",
                error="node is not alive",
            )

        if state.node_id != state.primary_id:
            print(
                f"[PBFT {state.node_id}] RECV REQUEST client={req.client_id} rid={req.request_id} (not primary, primary_id={state.primary_id})"
            )
            # Minimal forwarding for convenience (avoid forwarding loops)
            if req.forwarded:
                return pbft_pb2.ClientReply(
                    client_id=req.client_id,
                    request_id=req.request_id,
                    replica_id=state.node_id,
                    view=state.view,
                    seq=0,
                    committed=False,
                    result="",
                    error=f"not primary (primary_id={state.primary_id})",
                )

            if state.primary_id in self.rpc_clients:
                fwd = pbft_pb2.ClientRequest(
                    client_id=req.client_id,
                    request_id=req.request_id,
                    timestamp_ms=req.timestamp_ms,
                    payload=req.payload,
                    forwarded=True,
                )
                try:
                    print(f"[PBFT {state.node_id}] SEND REQUEST -> primary {state.primary_id}")
                    return self.rpc_clients[state.primary_id].client_request(fwd, timeout=timeout_s)
                except Exception as e:
                    return pbft_pb2.ClientReply(
                        client_id=req.client_id,
                        request_id=req.request_id,
                        replica_id=state.node_id,
                        view=state.view,
                        seq=0,
                        committed=False,
                        result="",
                        error=f"forward to primary failed: {e}",
                    )

            return pbft_pb2.ClientReply(
                client_id=req.client_id,
                request_id=req.request_id,
                replica_id=state.node_id,
                view=state.view,
                seq=0,
                committed=False,
                result="",
                error=f"not primary (primary_id={state.primary_id})",
            )

        # Primary path
        with self._lock:
            view = state.view
            seq = state.next_seq
            state.next_seq += 1
            digest = self._digest(req)

            key = self._entry_key(view, seq)
            entry = PBFTEntry(
                view=view,
                seq=seq,
                digest=digest,
                client_id=req.client_id,
                request_id=req.request_id,
                payload=req.payload,
            )
            state.log[key] = entry

        print(f"[PBFT {state.node_id}] REQUEST  client={req.client_id} rid={req.request_id} -> view={view} seq={seq}")

        # PRE-PREPARE
        pre = pbft_pb2.PrePrepareRequest(
            view=view,
            seq=seq,
            digest=digest,
            primary_id=state.node_id,
            request=req,
        )
        # PBFT: primary multicasts PRE-PREPARE first, then processes locally (as a replica)
        for peer in state.peers:
            try:
                print(f"[PBFT {state.node_id}] SEND PRE-PREPARE -> {peer} view={view} seq={seq}")
                self.rpc_clients[peer].pre_prepare(pre)
            except Exception:
                pass

        self.on_pre_prepare(pre)  # local

        # Wait until committed (primary returns a single reply)
        entry = state.log[self._entry_key(view, seq)]
        deadline = time.time() + timeout_s
        with entry.done:
            while not entry.executed and time.time() < deadline:
                remaining = max(0.05, deadline - time.time())
                entry.done.wait(timeout=remaining)

        with self._lock:
            entry = state.log.get(self._entry_key(view, seq))

        if entry is None:
            return pbft_pb2.ClientReply(
                client_id=req.client_id,
                request_id=req.request_id,
                replica_id=state.node_id,
                view=view,
                seq=seq,
                committed=False,
                result="",
                error="request entry missing",
            )

        return pbft_pb2.ClientReply(
            client_id=entry.client_id,
            request_id=entry.request_id,
            replica_id=state.node_id,
            view=entry.view,
            seq=entry.seq,
            committed=bool(entry.committed),
            result=entry.result or "",
            error=entry.error or "",
        )

    def on_pre_prepare(self, req: pbft_pb2.PrePrepareRequest) -> pbft_pb2.Ack:
        state = self.state
        if not state.alive:
            return pbft_pb2.Ack(ok=False, error="node is not alive")
        if req.view != state.view:
            return pbft_pb2.Ack(ok=False, error="wrong view")
        if req.primary_id != state.primary_id:
            return pbft_pb2.Ack(ok=False, error="wrong primary")

        digest = self._digest(req.request)
        if digest != req.digest:
            return pbft_pb2.Ack(ok=False, error="digest mismatch")

        key = self._entry_key(req.view, int(req.seq))
        pkey = self._pending_key(req.view, int(req.seq), req.digest)

        with self._lock:
            if key not in state.log:
                state.log[key] = PBFTEntry(
                    view=req.view,
                    seq=int(req.seq),
                    digest=req.digest,
                    client_id=req.request.client_id,
                    request_id=req.request.request_id,
                    payload=req.request.payload,
                )
            entry = state.log[key]

            # Apply any out-of-order PREPARE/COMMIT that arrived before this PRE-PREPARE
            pending_prepares = state.pending_prepares.pop(pkey, set())
            if pending_prepares:
                entry.prepares.update(pending_prepares)

            pending_commits = state.pending_commits.pop(pkey, set())
            if pending_commits:
                entry.commits.update(pending_commits)

        if state.node_id == int(req.primary_id):
            print(
                f"[PBFT {state.node_id}] LOCAL PRE-PREPARE view={req.view} seq={req.seq} digest={req.digest[:8]}..."
            )
        else:
            print(
                f"[PBFT {state.node_id}] RECV PRE-PREPARE from {req.primary_id} view={req.view} seq={req.seq} digest={req.digest[:8]}..."
            )

        # PREPARE (broadcast)
        prepare = pbft_pb2.PrepareRequest(
            view=req.view,
            seq=req.seq,
            digest=req.digest,
            replica_id=state.node_id,
        )
        if state.node_id != state.primary_id:
            for peer in state.peers:
                try:
                    print(f"[PBFT {state.node_id}] SEND PREPARE -> {peer} view={req.view} seq={req.seq}")
                    self.rpc_clients[peer].prepare(prepare)
                except Exception:
                    pass

            self.on_prepare(prepare)

        return pbft_pb2.Ack(ok=True, error="")

    def on_prepare(self, req: pbft_pb2.PrepareRequest) -> pbft_pb2.Ack:
        state = self.state
        if not state.alive:
            return pbft_pb2.Ack(ok=False, error="node is not alive")
        if req.view != state.view:
            return pbft_pb2.Ack(ok=False, error="wrong view")

        key = self._entry_key(req.view, int(req.seq))

        if int(req.replica_id) == state.node_id:
            print(f"[PBFT {state.node_id}] LOCAL PREPARE view={req.view} seq={req.seq}")
        else:
            print(f"[PBFT {state.node_id}] RECV PREPARE from {req.replica_id} view={req.view} seq={req.seq}")

        with self._lock:
            entry = state.log.get(key)
            if entry is None:
                pkey = self._pending_key(req.view, int(req.seq), req.digest)
                state.pending_prepares.setdefault(pkey, set()).add(int(req.replica_id))
                print(f"[PBFT {state.node_id}] BUFFER PREPARE from {req.replica_id} view={req.view} seq={req.seq} (no pre-prepare yet)")
                return pbft_pb2.Ack(ok=True, error="buffered")
            if entry.executed:
                # Late/duplicate message after execution; ignore.
                return pbft_pb2.Ack(ok=True, error="ignored (already executed)")
            if entry.digest != req.digest:
                return pbft_pb2.Ack(ok=False, error="digest mismatch")

            entry.prepares.add(int(req.replica_id))
            prepares_count = len(entry.prepares)
            became_prepared = False
            if not entry.prepared and prepares_count >= state.quorum_prepare:
                entry.prepared = True
                became_prepared = True

        if became_prepared:
            print(
                f"[PBFT {state.node_id}] PREPARED view={req.view} seq={req.seq} prepares={prepares_count}/{state.quorum_prepare}"
            )

            commit = pbft_pb2.CommitRequest(
                view=req.view,
                seq=req.seq,
                digest=req.digest,
                replica_id=state.node_id,
            )
            # PBFT: once PREPARED, multicast COMMIT (do not wait to be committed).
            for peer in state.peers:
                try:
                    print(f"[PBFT {state.node_id}] SEND COMMIT -> {peer} view={req.view} seq={req.seq}")
                    self.rpc_clients[peer].commit(commit)
                except Exception:
                    pass

            # Count our own COMMIT vote locally
            self.on_commit(commit)

        return pbft_pb2.Ack(ok=True, error="")

    def on_commit(self, req: pbft_pb2.CommitRequest) -> pbft_pb2.Ack:
        state = self.state
        if not state.alive:
            return pbft_pb2.Ack(ok=False, error="node is not alive")
        if req.view != state.view:
            return pbft_pb2.Ack(ok=False, error="wrong view")

        key = self._entry_key(req.view, int(req.seq))

        if int(req.replica_id) == state.node_id:
            print(f"[PBFT {state.node_id}] LOCAL COMMIT view={req.view} seq={req.seq}")
        else:
            print(f"[PBFT {state.node_id}] RECV COMMIT from {req.replica_id} view={req.view} seq={req.seq}")

        with self._lock:
            entry = state.log.get(key)
            if entry is None:
                pkey = self._pending_key(req.view, int(req.seq), req.digest)
                state.pending_commits.setdefault(pkey, set()).add(int(req.replica_id))
                print(f"[PBFT {state.node_id}] BUFFER COMMIT from {req.replica_id} view={req.view} seq={req.seq} (no pre-prepare yet)")
                return pbft_pb2.Ack(ok=True, error="buffered")
            if entry.executed:
                return pbft_pb2.Ack(ok=True, error="ignored (already executed)")
            if entry.digest != req.digest:
                return pbft_pb2.Ack(ok=False, error="digest mismatch")

            entry.commits.add(int(req.replica_id))
            commits_count = len(entry.commits)
            became_committed = False
            # Only move to COMMITTED after PREPARED (PBFT ordering)
            if entry.prepared and (not entry.committed) and commits_count >= state.quorum_commit:
                entry.committed = True
                became_committed = True

        if became_committed:
            print(
                f"[PBFT {state.node_id}] COMMITTED view={req.view} seq={req.seq} commits={commits_count}/{state.quorum_commit}"
            )
            self._execute(entry)

        return pbft_pb2.Ack(ok=True, error="")

    def _execute(self, entry: PBFTEntry) -> None:
        state = self.state
        with self._lock:
            if entry.executed:
                return
            # Minimal deterministic "execution": echo payload
            entry.result = entry.payload
            entry.executed = True
            entry.error = ""

        print(
            f"[PBFT {state.node_id}] REPLY    client={entry.client_id} rid={entry.request_id} result={entry.result!r}"
        )

        with entry.done:
            entry.done.notify_all()
