import streamlit as st
from rpc.client import PBFTClient

MAX_COLS = 4

def render_cluster_html(nodes):
    MAX_COLS = 4

    for i in range(0, len(nodes), MAX_COLS):
        cols = st.columns(MAX_COLS)

        for col, node in zip(cols, nodes[i:i+MAX_COLS]):
            with col:
                if node["process"]:
                    client = PBFTClient(
                        f"localhost:{node['port']}"
                    )
                    try:
                        status = client.get_status()
                    except:
                        st.error("Node unreachable")

                    color = {
                        "Primary": "#ffcc00",
                        "Replica": "#99ccff",
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
                        View: {status.view}<br>
                        Primary: {status.primary_id}<br>
                        f: {status.f}
                        </div>
                        """,
                        unsafe_allow_html=True
                    )