import streamlit as st
from rpc.client import RaftRPCClient

MAX_COLS = 4

def render_cluster_html(nodes):
    for i in range(0, len(nodes), MAX_COLS):
        row = nodes[i:i + MAX_COLS]
        cols = st.columns(len(row))

        for col, node in zip(cols, row):
            with col:
                alive = node["process"] is not None
                state = "RUNNING" if alive else "STOPPED"

                st.markdown(f"### 🧠 Node {node['id']}")
                st.write(f"Port: {node['port']}")
                st.write(f"State: {state}")

                if alive:
                    if st.button(
                        "🔔 Ping",
                        key=f"ping_{node['id']}"
                    ):
                        client = RaftRPCClient(
                            f"localhost:{node['port']}"
                        )
                        result = client.ping()
                        st.success(result)
                else:
                    st.button(
                        "Ping",
                        disabled=True,
                        key=f"ping_{node['id']}"
                    )