import json
import threading
import time
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any, Optional

import grpc

from core.pbft_node import PBFTNode
from core.pbft_message import Request, PrePrepare, Prepare, Commit, Checkpoint, ViewChange, NewView

from rpc import pbft_pb2, pbft_pb2_grpc


def _to_request_msg(req: Request) -> pbft_pb2.RequestMsg:
    return pbft_pb2.RequestMsg(
        operation_json=json.dumps(req.operation),
        timestamp=req.timestamp,
        client_id=req.client_id,
        digest=req.digest(),
    )


def _from_request_msg(msg: pbft_pb2.RequestMsg) -> Request:
    try:
        op: Any = json.loads(msg.operation_json) if msg.operation_json else None
    except Exception:
        op = msg.operation_json
    return Request(operation=op, timestamp=msg.timestamp, client_id=msg.client_id)


def _to_pre_prepare_msg(pp: PrePrepare) -> pbft_pb2.PrePrepareMsg:
    return pbft_pb2.PrePrepareMsg(
        view=pp.view,
        sequence=pp.sequence,
        digest=pp.digest,
        request=_to_request_msg(pp.request),
        primary_id=pp.primary_id,
        send_time=pp.send_time,
        recv_time=pp.recv_time,
    )


def _from_pre_prepare_msg(msg: pbft_pb2.PrePrepareMsg) -> PrePrepare:
    return PrePrepare(
        view=msg.view,
        sequence=int(msg.sequence),
        digest=msg.digest,
        request=_from_request_msg(msg.request),
        primary_id=msg.primary_id,
        send_time=msg.send_time,
        recv_time=msg.recv_time,
    )


def _to_prepare_msg(p: Prepare) -> pbft_pb2.PrepareMsg:
    return pbft_pb2.PrepareMsg(
        view=p.view,
        sequence=p.sequence,
        digest=p.digest,
        replica_id=p.replica_id,
        send_time=p.send_time,
        recv_time=p.recv_time,
    )


def _from_prepare_msg(msg: pbft_pb2.PrepareMsg) -> Prepare:
    return Prepare(
        view=msg.view,
        sequence=int(msg.sequence),
        digest=msg.digest,
        replica_id=msg.replica_id,
        send_time=msg.send_time,
        recv_time=msg.recv_time,
    )


def _to_commit_msg(c: Commit) -> pbft_pb2.CommitMsg:
    return pbft_pb2.CommitMsg(
        view=c.view,
        sequence=c.sequence,
        digest=c.digest,
        replica_id=c.replica_id,
        send_time=c.send_time,
        recv_time=c.recv_time,
    )


def _from_commit_msg(msg: pbft_pb2.CommitMsg) -> Commit:
    return Commit(
        view=msg.view,
        sequence=int(msg.sequence),
        digest=msg.digest,
        replica_id=msg.replica_id,
        send_time=msg.send_time,
        recv_time=msg.recv_time,
    )


def _to_checkpoint_msg(ckpt: Checkpoint) -> pbft_pb2.CheckpointMsg:
    return pbft_pb2.CheckpointMsg(
        sequence=ckpt.sequence,
        state_digest=ckpt.state_digest,
        replica_id=ckpt.replica_id,
    )


def _from_checkpoint_msg(msg: pbft_pb2.CheckpointMsg) -> Checkpoint:
    return Checkpoint(sequence=int(msg.sequence), state_digest=msg.state_digest, replica_id=msg.replica_id)


def _to_view_change_msg(vc: ViewChange) -> pbft_pb2.ViewChangeMsg:
    return pbft_pb2.ViewChangeMsg(
        new_view=vc.new_view,
        last_stable_checkpoint=vc.last_stable_checkpoint,
        checkpoint_proof=[_to_checkpoint_msg(c) for c in vc.checkpoint_proof],
        prepared_requests_json=[json.dumps(x) for x in vc.prepared_requests],
        replica_id=vc.replica_id,
    )


def _from_view_change_msg(msg: pbft_pb2.ViewChangeMsg) -> ViewChange:
    prepared = []
    for s in msg.prepared_requests_json:
        try:
            prepared.append(json.loads(s))
        except Exception:
            prepared.append({"raw": s})
    return ViewChange(
        new_view=msg.new_view,
        last_stable_checkpoint=int(msg.last_stable_checkpoint),
        checkpoint_proof=[_from_checkpoint_msg(c) for c in msg.checkpoint_proof],
        prepared_requests=prepared,
        replica_id=msg.replica_id,
    )


def _to_new_view_msg(nv: NewView) -> pbft_pb2.NewViewMsg:
    return pbft_pb2.NewViewMsg(
        view=nv.view,
        view_changes=[_to_view_change_msg(vc) for vc in nv.view_changes],
        primary_id=nv.primary_id,
    )


def _from_new_view_msg(msg: pbft_pb2.NewViewMsg) -> NewView:
    return NewView(
        view=msg.view,
        view_changes=[_from_view_change_msg(vc) for vc in msg.view_changes],
        pre_prepares=[],
        primary_id=msg.primary_id,
    )


class PBFTNodeServicer(pbft_pb2_grpc.PBFTNodeServiceServicer):
    """gRPC wrapper around a PBFTNode.

    The servicer owns a lock because gRPC can invoke handlers concurrently.
    """

    def __init__(self, node: PBFTNode):
        self.node = node
        self._lock = threading.Lock()

    def Ping(self, request: pbft_pb2.PingRequest, context) -> pbft_pb2.PingReply:
        # Debug/testing endpoint: sleeps inside the node lock so you can observe
        # per-node serialization and cross-node parallelism.
        sleep_ms = int(request.sleep_ms)
        if sleep_ms < 0:
            sleep_ms = 0
        if sleep_ms > 60_000:
            sleep_ms = 60_000

        with self._lock:
            if sleep_ms:
                time.sleep(sleep_ms / 1000.0)
            return pbft_pb2.PingReply(
                ok=True,
                node_id=int(self.node.id),
                slept_ms=sleep_ms,
                server_time_iso=datetime.now(timezone.utc).isoformat(),
            )

    def ReceivePrePrepare(self, request: pbft_pb2.PrePrepareMsg, context) -> pbft_pb2.PrePrepareResponse:
        pp = _from_pre_prepare_msg(request)
        with self._lock:
            if not self.node.alive or self.node.state != PBFTNode.NORMAL:
                return pbft_pb2.PrePrepareResponse(accepted=False, has_prepare=False, error="node not normal/alive")

            if not self.node.accept_pre_prepare(pp):
                return pbft_pb2.PrePrepareResponse(accepted=False, has_prepare=False, error="rejected pre-prepare")

            if pp.sequence in self.node.log.pre_prepares:
                return pbft_pb2.PrePrepareResponse(accepted=False, has_prepare=False, error="duplicate pre-prepare")

            self.node.log.add_pre_prepare(pp)

            # Heard from primary -> reset timer
            self.node.view_change_timer = pp.recv_time + self.node.view_change_timeout
            self.node.last_heard_from_primary = pp.recv_time

            # In this simulator, every correct node (including primary) sends PREPARE.
            prepare = Prepare(view=pp.view, sequence=pp.sequence, digest=pp.digest, replica_id=self.node.id)
            self.node.log.add_prepare(prepare)

            return pbft_pb2.PrePrepareResponse(
                accepted=True,
                prepare=_to_prepare_msg(prepare),
                has_prepare=True,
                error="",
            )

    def ReceivePrepare(self, request: pbft_pb2.PrepareMsg, context) -> pbft_pb2.PrepareResponse:
        prep = _from_prepare_msg(request)
        with self._lock:
            if not self.node.alive or self.node.state != PBFTNode.NORMAL:
                return pbft_pb2.PrepareResponse(accepted=False, has_commit=False, error="node not normal/alive")

            if prep.view != self.node.view:
                return pbft_pb2.PrepareResponse(accepted=False, has_commit=False, error="wrong view")

            self.node.log.add_prepare(prep)

            # prepared? -> maybe send COMMIT
            if self.node.is_prepared(prep.sequence, prep.view, prep.digest):
                if prep.sequence not in self.node.prepared_requests:
                    self.node.prepared_requests[prep.sequence] = {
                        "view": prep.view,
                        "digest": prep.digest,
                        "sequence": prep.sequence,
                    }

                existing_commits = self.node.log.get_commits(prep.sequence, prep.view, prep.digest)
                already_sent = any(c.replica_id == self.node.id for c in existing_commits)
                if not already_sent:
                    commit = Commit(view=prep.view, sequence=prep.sequence, digest=prep.digest, replica_id=self.node.id)
                    self.node.log.add_commit(commit)
                    return pbft_pb2.PrepareResponse(
                        accepted=True,
                        commit=_to_commit_msg(commit),
                        has_commit=True,
                        error="",
                    )

            return pbft_pb2.PrepareResponse(accepted=True, has_commit=False, error="")

    def ReceiveCommit(self, request: pbft_pb2.CommitMsg, context) -> pbft_pb2.CommitResponse:
        com = _from_commit_msg(request)
        with self._lock:
            if not self.node.alive or self.node.state != PBFTNode.NORMAL:
                return pbft_pb2.CommitResponse(accepted=False, executed=False, has_checkpoint=False, error="node not normal/alive")

            if com.view != self.node.view:
                return pbft_pb2.CommitResponse(accepted=False, executed=False, has_checkpoint=False, error="wrong view")

            self.node.log.add_commit(com)

            executed = False
            executed_seq = 0
            result_json = ""
            ckpt_msg: Optional[pbft_pb2.CheckpointMsg] = None

            # Byzantine simulation (minimal): a Byzantine node may skip execution, but it still
            # logs protocol messages so it can catch up later when fixed.
            if not self.node.is_byzantine:
                prev_exec = self.node.last_executed
                executed_any, last_exec, last_result = self.node.execute_committed_in_order()
                if executed_any and last_exec != prev_exec:
                    executed = True
                    executed_seq = last_exec
                    try:
                        result_json = json.dumps(last_result)
                    except Exception:
                        result_json = str(last_result)

                    # If we executed up to a checkpoint boundary, the node log may now contain it.
                    if self.node.last_executed in self.node.log.checkpoints:
                        # Best-effort: pick this node's own checkpoint for that seq if present.
                        for c in self.node.log.checkpoints[self.node.last_executed]:
                            if c.replica_id == self.node.id:
                                ckpt_msg = _to_checkpoint_msg(c)
                                break

            return pbft_pb2.CommitResponse(
                accepted=True,
                executed=executed,
                executed_sequence=executed_seq,
                result_json=result_json,
                checkpoint=ckpt_msg if ckpt_msg is not None else pbft_pb2.CheckpointMsg(),
                has_checkpoint=ckpt_msg is not None,
                error=("" if not self.node.is_byzantine else "byzantine skipped execution"),
            )

    def ReceiveCheckpoint(self, request: pbft_pb2.CheckpointMsg, context) -> pbft_pb2.Ack:
        ckpt = _from_checkpoint_msg(request)
        with self._lock:
            if not self.node.alive:
                return pbft_pb2.Ack(ok=False, error="node not alive")
            self.node.log.add_checkpoint(ckpt)
            self.node.advance_stable_checkpoint()
        return pbft_pb2.Ack(ok=True, error="")

    def ReceiveViewChange(self, request: pbft_pb2.ViewChangeMsg, context) -> pbft_pb2.Ack:
        vc = _from_view_change_msg(request)
        with self._lock:
            if not self.node.alive:
                return pbft_pb2.Ack(ok=False, error="node not alive")
            if vc.new_view not in self.node.view_changes:
                self.node.view_changes[vc.new_view] = []
            if not any(v.replica_id == vc.replica_id for v in self.node.view_changes[vc.new_view]):
                self.node.view_changes[vc.new_view].append(vc)
        return pbft_pb2.Ack(ok=True, error="")

    def ReceiveNewView(self, request: pbft_pb2.NewViewMsg, context) -> pbft_pb2.Ack:
        nv = _from_new_view_msg(request)
        with self._lock:
            if not self.node.alive:
                return pbft_pb2.Ack(ok=False, error="node not alive")
            if self.node.view < nv.view:
                self.node.view = nv.view
                self.node.state = PBFTNode.NORMAL
        return pbft_pb2.Ack(ok=True, error="")

    def GetStatus(self, request: pbft_pb2.Empty, context) -> pbft_pb2.NodeStatus:
        with self._lock:
            try:
                app_state_json = json.dumps(self.node.app_state, sort_keys=True)
            except Exception:
                app_state_json = str(self.node.app_state)
            d = self.node.to_dict()
            return pbft_pb2.NodeStatus(
                id=d["id"],
                view=d["view"],
                state=d["state"],
                is_primary=d["is_primary"],
                sequence_number=d["sequence_number"],
                last_executed=d["last_executed"],
                stable_checkpoint=d["stable_checkpoint"],
                low_water_mark=d["low_water_mark"],
                high_water_mark=d["high_water_mark"],
                app_state_json=app_state_json,
                alive=d["alive"],
                is_byzantine=d["is_byzantine"],
                f=d["f"],
            )

    def Crash(self, request: pbft_pb2.Empty, context) -> pbft_pb2.Ack:
        with self._lock:
            self.node.stop()
        return pbft_pb2.Ack(ok=True, error="")

    def Restart(self, request: pbft_pb2.TimeMsg, context) -> pbft_pb2.Ack:
        with self._lock:
            self.node.resume(request.current_time)
        return pbft_pb2.Ack(ok=True, error="")

    def SetByzantine(self, request: pbft_pb2.BoolMsg, context) -> pbft_pb2.Ack:
        with self._lock:
            self.node.set_byzantine(request.value)
            # When a node is fixed (no longer Byzantine), immediately try to catch up by
            # executing any already-committed requests in order.
            if not request.value and self.node.alive and self.node.state == PBFTNode.NORMAL:
                self.node.execute_committed_in_order()
        return pbft_pb2.Ack(ok=True, error="")


def start_node_server(node: PBFTNode, port: int) -> grpc.Server:
    executor = ThreadPoolExecutor(max_workers=16)
    server = grpc.server(thread_pool=executor)
    pbft_pb2_grpc.add_PBFTNodeServiceServicer_to_server(PBFTNodeServicer(node), server)
    server.add_insecure_port(f"127.0.0.1:{port}")
    server.start()

    # Attach executor so callers can shut it down to avoid hanging process exit.
    setattr(server, "_pbft_executor", executor)
    return server
