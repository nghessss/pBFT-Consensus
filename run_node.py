import argparse
import time
import traceback

from core.node import PBFTNode
from rpc.server import serve
from rpc.client import PBFTClient


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--id", type=int, required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument(
        "--peers",
        type=str,
        default="",
        help="Comma-separated peer list: id@host:port"
    )
    args = parser.parse_args()

    # ============================
    # PARSE PEERS
    # ============================
    peers = {}

    if args.peers:
        for item in args.peers.split(","):
            pid, addr = item.split("@")
            peers[int(pid)] = addr

    peers.pop(args.id, None)

    # ============================
    # INIT RPC CLIENTS
    # ============================
    clients = {pid: PBFTClient(addr) for pid, addr in peers.items()}
    
    # ============================
    # INIT RAFT NODE
    # ============================
    node = PBFTNode(
        node_id=args.id,
        peers=list(peers.keys()),
        rpc_clients=clients
    )

    print("=" * 50)
    node.start()
    print(f"[Node {args.id}] Port : {args.port}")
    print(f"[Node {args.id}] Peers: {list(peers.keys())}")
    print("=" * 50)
    
    # ============================
    # START RPC SERVER
    # ============================
    try:
        print(f"[Node {args.id}] Starting RPC server...")
        serve(node, args.port)   # nếu block thì OK luôn
        print(f"[Node {args.id}] serve() returned (UNEXPECTED)")
    except Exception:
        print(f"[Node {args.id}] RPC server crashed!")
        traceback.print_exc()


if __name__ == "__main__":
    main()
