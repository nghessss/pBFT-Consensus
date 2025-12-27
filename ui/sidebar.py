import streamlit as st

def render_sidebar(cluster):
    st.sidebar.header("⚙️ Cluster Controls")

    node_count = st.sidebar.number_input(
        "Number of nodes",
        min_value=3,
        max_value=10,
        value=len(cluster.nodes) if cluster.nodes else 3,
        step=1
    )

    if not cluster.is_running():
        if st.sidebar.button("Apply Node Count", use_container_width=True):
            cluster.resize(node_count)
            st.sidebar.success(f"Prepared {node_count} nodes")
    else:
        st.sidebar.info("Stop cluster before resizing")

    col1, col2 = st.sidebar.columns(2)

    with col1:
        if st.button("▶️ Start Cluster", use_container_width=True):
            cluster.start_all()
            st.sidebar.success("Cluster started")

    with col2:
        if st.button("⛔ Stop Cluster", use_container_width=True):
            cluster.stop_all()
            st.sidebar.warning("Cluster stopped")

    st.sidebar.markdown("### 🔍 Node List")

    for node in cluster.nodes:
        st.sidebar.write(
            f"Node {node['id']} | Port {node['port']} | "
            f"{'RUNNING' if node['process'] else 'STOPPED'}"
        )
