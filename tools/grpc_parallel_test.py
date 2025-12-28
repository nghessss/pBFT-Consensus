import os
import sys

# Allow running as `python tools/grpc_parallel_test.py` from repo root on Windows.
_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

import argparse
import time
from concurrent.futures import ThreadPoolExecutor

import grpc

from rpc import pbft_pb2, pbft_pb2_grpc


def ping(stub: pbft_pb2_grpc.PBFTNodeServiceStub, sleep_ms: int) -> pbft_pb2.PingReply:
    return stub.Ping(pbft_pb2.PingRequest(sleep_ms=sleep_ms), timeout=5.0)


def connect_stubs(base_port: int, n: int):
    channels = []
    stubs = []
    for node_id in range(n):
        port = base_port + node_id
        ch = grpc.insecure_channel(f"127.0.0.1:{port}")
        try:
            grpc.channel_ready_future(ch).result(timeout=1.0)
        except Exception:
            ch.close()
            continue
        channels.append(ch)
        stubs.append(pbft_pb2_grpc.PBFTNodeServiceStub(ch))
    return channels, stubs


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=7, help="number of node ports to probe")
    ap.add_argument("--base-port", type=int, default=50050)
    ap.add_argument("--sleep-ms", type=int, default=300)
    ap.add_argument(
        "--spawn",
        action="store_true",
        help="spawn an in-process PBFTCluster (only use when ports are free)",
    )
    args = ap.parse_args()

    channels = []
    stubs = []
    cluster = None

    if args.spawn:
        from core.pbft_cluster import PBFTCluster

        cluster = PBFTCluster(num_nodes=args.n)
        stubs = [cluster._rpc_stubs[i] for i in range(args.n)]
    else:
        channels, stubs = connect_stubs(args.base_port, args.n)

    if len(stubs) < 2:
        raise SystemExit(
            f"Need at least 2 reachable nodes for a parallel test; found {len(stubs)}. "
            f"Is Streamlit running and nodes bound on ports {args.base_port}+?"
        )

    n = len(stubs)
    sleep_ms = args.sleep_ms

    try:
        # 1) Sequential pings across nodes
        t0 = time.perf_counter()
        for s in stubs:
            ping(s, sleep_ms)
        t1 = time.perf_counter()
        seq_s = t1 - t0

        # 2) Parallel pings across nodes
        t2 = time.perf_counter()
        with ThreadPoolExecutor(max_workers=n) as ex:
            replies = [f.result() for f in [ex.submit(ping, stubs[i], sleep_ms) for i in range(n)]]
        t3 = time.perf_counter()
        par_s = t3 - t2

        # 3) Parallel pings to SAME node (shows per-node lock serialization)
        one = stubs[0]
        t4 = time.perf_counter()
        with ThreadPoolExecutor(max_workers=n) as ex:
            _ = [f.result() for f in [ex.submit(ping, one, sleep_ms) for _ in range(n)]]
        t5 = time.perf_counter()
        same_node_s = t5 - t4

        print(f"Reachable nodes: {n} (probed {args.n}), sleep_ms={sleep_ms}")
        print(f"Sequential across nodes: {seq_s:.3f}s (expected ~{n*sleep_ms/1000:.1f}s)")
        print(f"Parallel across nodes:   {par_s:.3f}s (expected ~{sleep_ms/1000:.1f}s)")
        print(f"Parallel same node x{n}: {same_node_s:.3f}s (expected ~{n*sleep_ms/1000:.1f}s due to per-node lock)")
        print("Sample replies:", [(r.node_id, r.slept_ms) for r in replies[: min(5, len(replies))]])
    finally:
        for ch in channels:
            try:
                ch.close()
            except Exception:
                pass
        if cluster is not None:
            cluster.close()


if __name__ == "__main__":
    main()
