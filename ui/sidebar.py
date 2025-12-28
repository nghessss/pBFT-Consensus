import streamlit as st
import time

from rpc.client import PBFTClient
from rpc import raft_pb2

def render_sidebar(cluster):
    st.sidebar.header("⚙️ Cluster Controls (PBFT)")

    msg_box = st.sidebar.empty()
    
    node_count = st.sidebar.number_input(
        "Number of nodes",
        min_value=4,
        max_value=10,
        value=len(cluster.nodes) if cluster.nodes else 4,
        step=1
    )

    ok, f = cluster.validate_node_count(int(node_count))
    if ok:
        st.sidebar.caption(f"PBFT constraint OK: n={int(node_count)} = 3*{f}+1")
    else:
        st.sidebar.caption("PBFT requires n = 3f + 1 (valid: 4, 7, 10)")

    if not cluster.is_running():
        if st.sidebar.button("Apply Node Count", use_container_width=True):
            cluster.resize(int(node_count))
            if cluster.last_error:
                msg_box.error(cluster.last_error)
            else:
                msg_box.success(f"Prepared {int(node_count)} nodes")
    else:
        msg_box.info("Stop cluster before resizing")

    col1, col2 = st.sidebar.columns(2)

    with col1:
        if st.button("▶️ Start Cluster", use_container_width=True):
            cluster.start_all()
            if cluster.last_error:
                msg_box.error(cluster.last_error)
            else:
                msg_box.success("Cluster started")

    with col2:
        if st.button("⛔ Stop Cluster", use_container_width=True):
            cluster.stop_all()
            msg_box.warning("Cluster stopped")

    st.sidebar.markdown("### 🔍 Node List")

    for node in cluster.nodes:
        st.sidebar.write(
            f"Node {node['id']} | Port {node['port']} | "
            f"{'RUNNING' if node['process'] else 'STOPPED'}"
        )

    st.sidebar.markdown("### 📩 Client Request")
    if not cluster.nodes:
        st.sidebar.info("Apply node count first (valid: 4, 7, 10).")
        return

    node_ids = [n["id"] for n in cluster.nodes]
    target_id = st.sidebar.selectbox(
        "Send to node",
        options=node_ids,
        format_func=lambda node_id: next(
            (f"Node {n['id']} (localhost:{n['port']})" for n in cluster.nodes if n["id"] == node_id),
            f"Node {node_id}",
        ),
        disabled=not cluster.is_running(),
    )
    payload = st.sidebar.text_input(
        "Payload",
        value="hello",
        disabled=not cluster.is_running(),
    )

    if st.sidebar.button("Send Request", use_container_width=True, disabled=not cluster.is_running()):
        try:
            target_node = next((n for n in cluster.nodes if n["id"] == target_id), None)
            if target_node is None:
                st.sidebar.error("Unknown target node")
                return

            client = PBFTClient(f"localhost:{target_node['port']}")
            now = int(time.time() * 1000)
            req = raft_pb2.ClientRequest(
                client_id="streamlit",
                request_id=str(now),
                timestamp_ms=now,
                payload=payload,
                forwarded=False,
            )
            reply = client.client_request(req, timeout=30.0)
            if reply.error:
                st.sidebar.error(reply.error)
            else:
                st.sidebar.success(
                    f"Committed={reply.committed} view={reply.view} seq={reply.seq} primary={reply.replica_id}"
                )
        except Exception as e:
            st.sidebar.error(f"Send failed: {e}")
