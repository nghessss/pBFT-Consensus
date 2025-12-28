import streamlit as st
from rpc.client import RaftRPCClient

MAX_COLS = 4

def render_cluster_html(nodes):
    MAX_COLS = 4

    for i in range(0, len(nodes), MAX_COLS):
        cols = st.columns(MAX_COLS)

        for col, node in zip(cols, nodes[i:i+MAX_COLS]):
            with col:
                if node["process"]:
                    client = RaftRPCClient(
                        f"localhost:{node['port']}"
                    )
                    try:
                        status = client.get_status()
                    except:
                        st.error("Node unreachable")

                    color = {
                        "Leader": "#ffcc00",
                        "Candidate": "#ff9999",
                        "Follower": "#99ccff"
                    }.get(status.role, "#cccccc")

                    st.markdown(
                        f"""
                        <div style="
                            border:2px solid black;
                            padding:10px;
                            background:{color};
                            border-radius:8px;
                        ">
                        <b>Node {status.node_id}</b><br>
                        Role: {status.role}<br>
                        Term: {status.term}
                        </div>
                        """,
                        unsafe_allow_html=True
                    )